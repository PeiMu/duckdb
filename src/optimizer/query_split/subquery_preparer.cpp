#include "duckdb/optimizer/query_split/subquery_preparer.hpp"

#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/stream_query_result.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> SubqueryPreparer::GenerateProjHead(const unique_ptr<LogicalOperator> &original_plan,
                                                               unique_ptr<LogicalOperator> subquery,
                                                               const table_expr_info &table_expr_queue,
                                                               const std::vector<TableExpr> &original_proj_expr,
                                                               bool merge_sibling_expr) {
#if ENABLE_DEBUG_PRINT
	// debug: print subquery
	Printer::Print("Current subquery");
	subquery->Print();
#endif

	vector<unique_ptr<Expression>> new_exprs;
	// get the lowest-level `TableExpr` info
	auto expr_idx_pair_vec = table_expr_queue.front();
	auto temp_proj_exprs = expr_idx_pair_vec[0];

	// merge sibling node's expressions
	if (merge_sibling_expr) {
#ifdef DEBUG
		D_ASSERT(!last_sibling_exprs.empty());
#endif
		temp_proj_exprs.insert(last_sibling_exprs.begin(), last_sibling_exprs.end());
	}

	if (context.config.enable_dbshaker_split_jop) {
		if (expr_idx_pair_vec.size() > 1) {
			if (ENABLE_PARALLEL_EXECUTION) {
				// todo: execute in parallel
			} else {
				last_sibling_exprs.insert(expr_idx_pair_vec[1].begin(), expr_idx_pair_vec[1].end());
			}
		} else {
			last_sibling_exprs.clear();
		}
	} else {
		// follow the pipeline breaker role
		if (expr_idx_pair_vec.size() > 1) {
			temp_proj_exprs.insert(expr_idx_pair_vec[1].begin(), expr_idx_pair_vec[1].end());
		}
	}

	// collect all columns with the same table from the upper levels
	std::set<TableExpr> used_expr_in_upper_levels;

	// `temp_stack` contains the `TableExpr` info of all the upper-level subqueries,
	// we try to merge the matching tables in a bottom-up order by levels
	auto temp_stack = table_expr_queue;
	temp_stack.pop_front();
	while (!temp_stack.empty()) {
		auto temp_vec = temp_stack.front();
		temp_stack.pop_front();
		for (const auto &temp_set : temp_vec) {
			for (const auto &table_expr : temp_set) {
				// check if expressions in the upper operators still use tables in the current level,
				// which will be merged into DATA_CHUNK
				auto same_table_it = std::find_if(
				    temp_proj_exprs.begin(), temp_proj_exprs.end(),
				    [table_expr](const TableExpr &temp_expr) { return table_expr.table_idx == temp_expr.table_idx; });
				if (same_table_it != temp_proj_exprs.end()) {
					used_expr_in_upper_levels.emplace(table_expr);
				}
			}
		}
	}

	// merge projection's expressions if they have the same table index
	std::set<TableExpr> used_expr_in_proj;
	for (const auto &ori_proj_expr : original_proj_expr) {
		auto find_it =
		    std::find_if(temp_proj_exprs.begin(), temp_proj_exprs.end(), [ori_proj_expr](TableExpr proj_expr) {
			    return proj_expr.table_idx == ori_proj_expr.table_idx;
		    });
		if (temp_proj_exprs.end() != find_it) {
			used_expr_in_proj.emplace(ori_proj_expr);
		}
	}

	proj_exprs.clear();
	// get the union set of upper_levels and proj
	proj_exprs.insert(used_expr_in_upper_levels.begin(), used_expr_in_upper_levels.end());
	proj_exprs.insert(used_expr_in_proj.begin(), used_expr_in_proj.end());

	for (const auto &expr_pair : proj_exprs) {
		ColumnBinding binding = ColumnBinding(expr_pair.table_idx, expr_pair.column_idx);
		auto col_ref_select_expr =
		    make_uniq<BoundColumnRefExpression>(expr_pair.column_name, expr_pair.return_type, binding, 0);
		new_exprs.emplace_back(std::move(col_ref_select_expr));
#if ENABLE_DEBUG_PRINT
		// debug
		std::string str = "table index: " + std::to_string(binding.table_index) +
		                  ", column index: " + std::to_string(binding.column_index) +
		                  ", name: " + expr_pair.column_name + ", type: " + expr_pair.return_type.ToString();
		Printer::Print(str);
#endif
	}

	auto new_proj_node = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(new_exprs));
	new_proj_node->AddChild(std::move(subquery));

#if ENABLE_DEBUG_PRINT
	// debug: print subquery
	Printer::Print("Current subquery with projection");
	new_proj_node->Print();
#endif
	return unique_ptr_cast<LogicalProjection, LogicalOperator>(std::move(new_proj_node));
}

shared_ptr<PreparedStatementData> SubqueryPreparer::AdaptSelect(shared_ptr<PreparedStatementData> original_stmt_data,
                                                                const unique_ptr<LogicalOperator> &subquery) {
	auto subquery_stmt = make_shared<PreparedStatementData>(original_stmt_data->statement_type);
	// copy from `original_stmt_data`
	subquery_stmt->properties = original_stmt_data->properties;
	subquery_stmt->names = original_stmt_data->names;
	subquery_stmt->types = original_stmt_data->types;
	for (const auto &v : original_stmt_data->value_map) {
		// todo: may have bugs here, can we just copy or need `std::move`?
		subquery_stmt->value_map.at(v.first) = v.second;
	}
	subquery_stmt->catalog_version = original_stmt_data->catalog_version;
	subquery_stmt->unbound_statement = original_stmt_data->unbound_statement->Copy();

	// Modify the SelectNode based of the subquery
	auto &select_statemet = subquery_stmt->unbound_statement->Cast<SelectStatement>();
#ifdef DEBUG
	D_ASSERT(QueryNodeType::SELECT_NODE == select_statemet.node->type);
#endif
	auto &select_node = select_statemet.node->Cast<SelectNode>();
	if (!select_node.select_list.empty()) {
		select_node.select_list.clear();
		subquery_stmt->names.clear();
		subquery_stmt->types.clear();
		for (const auto &proj_expr : subquery->expressions) {
			if (ExpressionType::BOUND_COLUMN_REF == proj_expr->type) {
				// we should have gotten every information about the table_expr, including alias
#ifdef DEBUG
				D_ASSERT(!proj_expr->alias.empty());
#endif
				unique_ptr<ColumnRefExpression> new_select_expr = make_uniq<ColumnRefExpression>(proj_expr->alias);
				select_node.select_list.emplace_back(std::move(new_select_expr));
				auto new_name = proj_expr->alias;
				subquery_stmt->names.emplace_back(new_name);
				auto new_type = proj_expr->return_type;
				subquery_stmt->types.emplace_back(new_type);
			}
		}
	}
	return subquery_stmt;
}

int64_t SubqueryPreparer::MergeDataChunk(std::vector<unique_ptr<LogicalOperator>> &current_level_subqueries,
                                         unique_ptr<ColumnDataCollection> previous_result, idx_t estimated_card) {

	//	unique_ptr<MaterializedQueryResult> result_materialized;
	//	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	// #if TIME_BREAK_DOWN
	//	auto timer = chrono_tic();
	// #endif
	//	int64_t chunk_size = 0;
	//	if (previous_result->type == QueryResultType::STREAM_RESULT) {
	//		auto &stream_query = previous_result->Cast<duckdb::StreamQueryResult>();
	//		result_materialized = stream_query.Materialize();
	//		collection = make_uniq<ColumnDataCollection>(result_materialized->Collection());
	//	} else if (previous_result->type == QueryResultType::MATERIALIZED_RESULT) {
	//		ColumnDataAppendState append_state;
	//		collection->InitializeAppend(append_state);
	//		unique_ptr<DataChunk> chunk;
	//		ErrorData error;
	//		while (true) {
	//			previous_result->TryFetch(chunk, error);
	//			if (!chunk || chunk->size() == 0) {
	//				break;
	//			}
	//			chunk_size += chunk->size();
	//			// set chunk cardinality
	//			chunk->SetCardinality(chunk->size());
	//			collection->Append(append_state, *chunk);
	//		}
	//	}
	// #if TIME_BREAK_DOWN
	//	std::string str = "Fetch data with size=" + std::to_string(chunk_size) + ", ";
	//	chrono_toc(&timer, str.data());
	// #endif

	int64_t chunk_size = previous_result->Count();

	// generate an unused table index by the binder
	new_table_idx = binder.GenerateTableIndex();

	if (nullptr == chunk_scan) {
		chunk_scan =
		    make_uniq<LogicalColumnDataGet>(new_table_idx, previous_result->Types(), std::move(previous_result));
	} else {
		chunk_scan->table_index = new_table_idx;
		chunk_scan->chunk_types = previous_result->Types();
		chunk_scan->collection = std::move(previous_result);
	}

#if ENABLE_SPECIFY_EST_STAT
#ifdef DEBUG
	D_ASSERT(0 != estimated_card);
#endif
	chunk_scan->estimated_cardinality = estimated_card;
#else
	chunk_scan->estimated_cardinality = chunk_size;
#endif
	chunk_scan->has_estimated_cardinality = true;
	bool merged = false;
	MergeToSubquery(*current_level_subqueries[0], merged);
	if (!merged) {
#ifdef DEBUG
		D_ASSERT(current_level_subqueries.size() == 2);
#endif
		MergeToSubquery(*current_level_subqueries[1], merged);
	}
#ifdef DEBUG
	D_ASSERT(merged);
#endif

	return chunk_size;
}

bool SubqueryPreparer::MergeSibling(std::vector<unique_ptr<LogicalOperator>> &current_level_subqueries,
                                    unique_ptr<LogicalOperator> last_sibling_node) {
	auto merge_sibling = [&last_sibling_node](LogicalOperator *subquery_pointer) {
		while (LogicalOperatorType::LOGICAL_GET != subquery_pointer->type &&
		       LogicalOperatorType::LOGICAL_CHUNK_GET != subquery_pointer->type &&
		       !subquery_pointer->children.empty()) {
			if (nullptr == subquery_pointer->children[0]) {
				subquery_pointer->children[0] = std::move(last_sibling_node);
				return true;
			} else if (subquery_pointer->children.size() > 1 && nullptr == subquery_pointer->children[1]) {
#ifdef DEBUG
				D_ASSERT(nullptr != last_sibling_node);
#endif
				subquery_pointer->children[1] = std::move(last_sibling_node);
				return true;
			} else {
				subquery_pointer = subquery_pointer->children[0].get();
			}
		}
		return false;
	};

	bool merge_to_left = true;
	// merge the sibling back to the upper subquery
	auto subquery_pointer = current_level_subqueries[0].get();
	bool merged = merge_sibling(subquery_pointer);
	if (!merged) {
#ifdef DEBUG
		D_ASSERT(current_level_subqueries.size() == 2);
#endif
		subquery_pointer = current_level_subqueries[1].get();
		merged = merge_sibling(subquery_pointer);
		merge_to_left = false;
	}
	// check this is the last operator
#ifdef DEBUG
	D_ASSERT(merged && !subquery_pointer->children.empty());
#endif
	return merge_to_left;
}

void SubqueryPreparer::AddOldTableIndex(const unique_ptr<LogicalOperator> &op) {
	if (LogicalOperatorType::LOGICAL_GET == op->type) {
		old_table_idx.emplace(op->Cast<LogicalGet>().table_index);
	} else if (LogicalOperatorType::LOGICAL_CHUNK_GET == op->type) {
		old_table_idx.emplace(op->Cast<LogicalColumnDataGet>().table_index);
	} else if (LogicalOperatorType::LOGICAL_FILTER == op->type) {
		AddOldTableIndex(std::move(op->children[0]));
	} else if (LogicalOperatorType::LOGICAL_COMPARISON_JOIN == op->type ||
	           LogicalOperatorType::LOGICAL_CROSS_PRODUCT == op->type ||
	           LogicalOperatorType::LOGICAL_ANY_JOIN == op->type) {
		AddOldTableIndex(std::move(op->children[0]));
		AddOldTableIndex(std::move(op->children[1]));
	} else {
		Printer::Print(StringUtil::Format("Do not support yet, op->type:  %s", LogicalOperatorToString(op->type)));
		D_ASSERT(false);
	}
}

void SubqueryPreparer::MergeToSubquery(LogicalOperator &op, bool &merged) {
	for (int idx = op.children.size() - 1; idx > -1; idx--) {
		auto &child = op.children[idx];
		if (merged)
			return;
		// find the insert point and insert the `ColumnDataGet` node to the logical plan
		if (nullptr == child || child->split_index == merge_index) {
#ifdef DEBUG
			D_ASSERT(nullptr != chunk_scan);
#endif
			child = std::move(chunk_scan);
			merged = true;
			merge_index--;
			return;
		}
		MergeToSubquery(*child, merged);
	}
}

table_expr_info SubqueryPreparer::UpdateTableExpr(table_expr_info table_expr_queue,
                                                  std::vector<TableExpr> &original_proj_expr) {
	table_expr_info ret;
	// find if `table_expr_queue` has the `old_table_idx` that need to be updated
	while (!table_expr_queue.empty()) {
		auto table_expr_vec = table_expr_queue.front();
		std::vector<std::set<TableExpr>> new_vec;
		for (const auto &table_expr_set : table_expr_vec) {
			std::set<TableExpr> new_set;
			for (const auto &table_expr : table_expr_set) {
				if (old_table_idx.count(table_expr.table_idx)) {
					TableExpr new_table_expr = table_expr;
					// update column index based on the current level's "proj_exprs" order
					auto find_column_it =
					    std::find_if(proj_exprs.begin(), proj_exprs.end(), [table_expr](const TableExpr &proj_expr) {
						    return proj_expr.table_idx == table_expr.table_idx &&
						           proj_expr.column_idx == table_expr.column_idx;
					    });
#ifdef DEBUG
					D_ASSERT(proj_exprs.end() != find_column_it);
#endif
					new_table_expr.column_idx = std::distance(proj_exprs.begin(), find_column_it);
					// replace to the new table index (gotten in `MergeDataChunk`)
					new_table_expr.table_idx = new_table_idx;
					new_set.emplace(new_table_expr);
				} else {
					new_set.emplace(table_expr);
				}
			}
			new_vec.emplace_back(new_set);
		}
		ret.emplace_back(new_vec);
		table_expr_queue.pop_front();
	}

	// find if `original_proj_expr` has the `old_table_idx` that need to be updated
	for (auto &expr : original_proj_expr) {
		if (old_table_idx.count(expr.table_idx)) {
			// update column index based on the current level's "proj_exprs" order
			auto find_column_it =
			    std::find_if(proj_exprs.begin(), proj_exprs.end(), [expr](const TableExpr &proj_expr) {
				    return proj_expr.table_idx == expr.table_idx && proj_expr.column_idx == expr.column_idx;
			    });
#ifdef DEBUG
			D_ASSERT(proj_exprs.end() != find_column_it);
#endif
			expr.column_idx = std::distance(proj_exprs.begin(), find_column_it);
			// replace to the new table index (gotten in `MergeDataChunk`)
			expr.table_idx = new_table_idx;
		}
	}

	return ret;
}

unique_ptr<LogicalOperator> SubqueryPreparer::UpdateProjHead(unique_ptr<LogicalOperator> plan,
                                                             const std::vector<TableExpr> &original_proj_expr) {
	auto plan_pointer = plan.get();
	if (LogicalOperatorType::LOGICAL_LIMIT == plan_pointer->type) {
		plan_pointer = plan_pointer->children[0].get();
	}
	if (LogicalOperatorType::LOGICAL_ORDER_BY == plan_pointer->type) {
		plan_pointer = plan_pointer->children[0].get();
	}
#ifdef DEBUG
	D_ASSERT(LogicalOperatorType::LOGICAL_PROJECTION == plan_pointer->type);
#endif
	auto &proj_op = plan_pointer->Cast<LogicalProjection>();
	if (nullptr != proj_op.children[0] &&
	    LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY == proj_op.children[0]->type) {
		// update aggregate expressions
		auto &aggregate_op = proj_op.children[0]->Cast<LogicalAggregate>();
		auto proj_expr_index = 0;

		// update expr of group by
		for (auto &agg_group_expr : aggregate_op.groups) {
			// we assume the group by node can be covered by `GetRefColumnBinding`
			// or we will update it by `UpdateExprs` if it's necessary
			auto &column_binding = GetRefColumnBinding(agg_group_expr);
			column_binding.table_index = original_proj_expr[proj_expr_index].table_idx;
			column_binding.column_index = original_proj_expr[proj_expr_index].column_idx;
#ifdef DEBUG
			// D_ASSERT(agg_group_expr.alias == original_proj_expr[proj_expr_index].column_name);
			D_ASSERT(agg_group_expr->return_type == original_proj_expr[proj_expr_index].return_type);
#endif
			proj_expr_index++;
		}

		// update expr of aggregate op expression
		for (auto &agg_expr : aggregate_op.expressions) {
#ifdef DEBUG
			D_ASSERT(ExpressionType::BOUND_AGGREGATE == agg_expr->type);
#endif
			auto &aggregate_expr = agg_expr->Cast<BoundAggregateExpression>();
			for (auto &expr : aggregate_expr.children) {
				UpdateExprs(expr, [original_proj_expr, &proj_expr_index](unique_ptr<Expression> &expr) {
					auto &column_binding = GetRefColumnBinding(expr);
					column_binding.table_index = original_proj_expr[proj_expr_index].table_idx;
					column_binding.column_index = original_proj_expr[proj_expr_index].column_idx;
#ifdef DEBUG
					// D_ASSERT(expr.alias == original_proj_expr[proj_expr_index].column_name);
					D_ASSERT(expr->return_type == original_proj_expr[proj_expr_index].return_type);
#endif
					proj_expr_index++;
				});
			}
		}
	} else {
		auto proj_expr_index = 0;
		for (auto &expr : proj_op.expressions) {
			UpdateExprs(expr, [original_proj_expr, &proj_expr_index](unique_ptr<Expression> &expr) {
				auto &column_binding = GetRefColumnBinding(expr);
				column_binding.table_index = original_proj_expr[proj_expr_index].table_idx;
				column_binding.column_index = original_proj_expr[proj_expr_index].column_idx;
#ifdef DEBUG
				D_ASSERT(expr->alias == original_proj_expr[proj_expr_index].column_name);
				D_ASSERT(expr->return_type == original_proj_expr[proj_expr_index].return_type);
#endif
				proj_expr_index++;
			});
		}
	}
	return std::move(plan);
}

unique_ptr<Expression> SubqueryPreparer::VisitReplace(BoundColumnRefExpression &expr,
                                                      unique_ptr<Expression> *expr_ptr) {
	const auto find_expr_it = std::find_if(proj_exprs.begin(), proj_exprs.end(), [&expr](const TableExpr &table_expr) {
		return table_expr.table_idx == expr.binding.table_index && table_expr.column_idx == expr.binding.column_index;
	});

	if (find_expr_it != proj_exprs.end() && old_table_idx.count(find_expr_it->table_idx)) {
		expr.binding.table_index = new_table_idx;
		expr.binding.column_index = std::distance(proj_exprs.begin(), find_expr_it);
	}

	return nullptr;
}

void SubqueryPreparer::UpdateSubqueriesIndex(subquery_queue &subqueries) {
	for (auto &subquery_vec : subqueries) {
		for (auto &subquery : subquery_vec) {
			VisitOperator(*subquery);
		}
	}
}

void SubqueryPreparer::CanonicalizeCrossProduct(unique_ptr<LogicalOperator> &plan) {
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_LIMIT:
		break;
	default:
		return;
	}

	// 1. collect the (JOIN, op_level) pair
	// it is a left-deep plan at this point (before the JoinOrderOpt)
	auto op_child = plan.get();
	std::stack<std::pair<LogicalOperator *, int>> join_pointers_pair;
	int op_level = 0;
	while (!op_child->children.empty()) {
		if (LogicalOperatorType::LOGICAL_COMPARISON_JOIN == op_child->type) {
			join_pointers_pair.push({op_child, op_level});
		}
		op_child = op_child->children[0].get();
		op_level++;
	}

	while (!join_pointers_pair.empty()) {
		// check if the current level JOIN needs to canonicalize
		auto current_pair = join_pointers_pair.top();
		LogicalOperator *current_join_pointer = current_pair.first;
		int current_join_level = current_pair.second;
		join_pointers_pair.pop();
		auto &current_join = current_join_pointer->Cast<LogicalComparisonJoin>();

		std::unordered_set<idx_t> left_cond_table_index;
		std::unordered_map<idx_t, unique_ptr<LogicalOperator>> table_blocks;
		std::deque<idx_t> table_blocks_key_order;
		std::queue<unique_ptr<LogicalOperator>> unused_blocks;

		unique_ptr<LogicalOperator> last_block = CheckTableUsage(current_join_pointer, left_cond_table_index,
		                                                         table_blocks, table_blocks_key_order, unused_blocks);
		if (nullptr == last_block)
			continue;

		// 2. if all tables below the last join are used,
		// revert to the original sub plan and check the next JOIN point
		if (unused_blocks.empty()) {
			RevertUsedBlocks(current_join_pointer, std::move(last_block), table_blocks_key_order, table_blocks);
			continue;
		}

		// 3. construct the new plan tree
		// add the cross_product with the used table
		unique_ptr<LogicalOperator> belowed_cross_product = std::move(last_block);
		for (auto &block : table_blocks) {
			if (left_cond_table_index.count(block.first)) {
				belowed_cross_product =
				    LogicalCrossProduct::Create(std::move(belowed_cross_product), std::move(block.second));
			}
		}

		// add the belowed_cross_product to the current_join
		current_join.children[0] = std::move(belowed_cross_product);

		// get the previous op of current_join by index since we cannot convert JOIN to LogicalOperator
		auto reordered_plan = plan.get();
		for (int level_id = 0; level_id < current_join_level - 1; level_id++) {
			reordered_plan = reordered_plan->children[0].get();
		}

		// add the unused blocks as aboved_cross_product, on top of the current_join
		unique_ptr<LogicalOperator> aboved_cross_product = std::move(reordered_plan->children[0]);
		while (!unused_blocks.empty()) {
			aboved_cross_product =
			    LogicalCrossProduct::Create(std::move(aboved_cross_product), std::move(unused_blocks.front()));
			unused_blocks.pop();
		}

		// get the position to reorder
		reordered_plan = plan.get();
		for (int level_id = 0; level_id < current_join_level - 1; level_id++) {
			reordered_plan = reordered_plan->children[0].get();
		}
		// add the aboved_cross_product
		reordered_plan->children[0] = std::move(aboved_cross_product);
	}
}

bool SubqueryPreparer::NeedRewrite(const std::vector<unique_ptr<LogicalOperator>> &subqueries_vec) {
	if (subqueries_vec.empty()) {
		return true;
	}

	if (LogicalOperatorType::LOGICAL_COMPARISON_JOIN != subqueries_vec[0]->type) {
#ifdef DEBUG
		D_ASSERT(LogicalOperatorType::LOGICAL_FILTER == subqueries_vec[0]->type);
#endif
		return false;
	}

	LogicalOperator *current_join_pointer = subqueries_vec[0].get();
	std::unordered_set<idx_t> left_cond_table_index;
	std::unordered_map<idx_t, unique_ptr<LogicalOperator>> table_blocks;
	std::deque<idx_t> table_blocks_key_order;
	std::queue<unique_ptr<LogicalOperator>> unused_blocks;

	std::vector<std::pair<idx_t, idx_t>> table_index_pairs;
	std::function<void(const unique_ptr<LogicalOperator> &op)> collect_cond_tables;
	collect_cond_tables = [&collect_cond_tables, &table_index_pairs](const unique_ptr<LogicalOperator> &op) {
		if (LogicalOperatorType::LOGICAL_COMPARISON_JOIN == op->type) {
			auto &join_op = op->Cast<LogicalComparisonJoin>();

			// collect table pairs form JOIN
			for (const auto &cond : join_op.conditions) {
				table_index_pairs.emplace_back(
				    std::make_pair(GetConstTableExpr(cond.left).table_idx, GetConstTableExpr(cond.right).table_idx));
			}
		}

		for (auto &child : op->children) {
			collect_cond_tables(child);
		}
	};

	collect_cond_tables(subqueries_vec[0]);

	UnionFind uf;
	// Union the pairs
	for (auto &index_pair : table_index_pairs) {
		uf.unite(index_pair.first, index_pair.second);
	}
	// Find distinct groups
	unordered_set<int> groups;
	for (auto &index_pair : table_index_pairs) {
		groups.insert(uf.find_parent(index_pair.first));
		groups.insert(uf.find_parent(index_pair.second));
	}

	if (groups.size() > 1)
		return true;

	// fixme: it may have more JOINs
	unique_ptr<LogicalOperator> last_block = CheckTableUsage(current_join_pointer, left_cond_table_index, table_blocks,
	                                                         table_blocks_key_order, unused_blocks);

	if (nullptr == last_block) {
		return false;
	}

	RevertUsedBlocks(current_join_pointer, std::move(last_block), table_blocks_key_order, table_blocks);

	if (!unused_blocks.empty()) {
		RevertUnusedBlocks(current_join_pointer, unused_blocks);
		return true;
	} else {
		return false;
	}
}

bool SubqueryPreparer::NeedReorder(const std::vector<unique_ptr<LogicalOperator>> &subqueries_vec,
                                   std::deque<std::pair<idx_t, idx_t>> table_card_order, idx_t previous_result_card) {
	if (subqueries_vec.empty()) {
		return true;
	}

	if (LogicalOperatorType::LOGICAL_COMPARISON_JOIN != subqueries_vec[0]->type) {
		return false;
	}

	LogicalOperator *current_join_pointer = subqueries_vec[0].get();
	auto &current_join = current_join_pointer->Cast<LogicalComparisonJoin>();

	// collect all table index
	std::unordered_set<idx_t> table_indexes;
	for (const auto &cond : current_join.conditions) {
		table_indexes.insert(GetConstTableExpr(cond.left).table_idx);
		table_indexes.insert(GetConstTableExpr(cond.right).table_idx);
	}

	// check if it is needed to reorder,
	// when the previous_result_card is larger than the smallest one of the table in the upper level subquery
	for (auto cid = table_card_order.cbegin(); cid != table_card_order.cend();) {
		if (table_indexes.count(cid->first)) {
			cid = table_card_order.erase(cid);
		} else {
			cid++;
		}
	}

	return previous_result_card > table_card_order.back().second;
}

void SubqueryPreparer::MergeSubquery(unique_ptr<LogicalOperator> &plan, subquery_queue old_subqueries) {
	// get the position to reorder
	auto new_plan = plan.get();
	while (true) {
		if (nullptr == new_plan->children[0]) {
			auto old_subquery_pair = std::move(old_subqueries.back());
			if (2 == old_subquery_pair.size()) {
#ifdef DEBUG
				D_ASSERT(nullptr == new_plan->children[1]);
#endif
				// keep the same order
				new_plan->children[0] = std::move(old_subquery_pair[1]);
				new_plan->children[1] = std::move(old_subquery_pair[0]);
			} else {
				new_plan->children[0] = std::move(old_subquery_pair[0]);
			}
			old_subqueries.pop_back();
		}
		new_plan = new_plan->children[0].get();
		if (old_subqueries.empty())
			break;
	}
}

void SubqueryPreparer::InsertTableBlocks(unique_ptr<LogicalOperator> &op,
                                         unordered_map<idx_t, unique_ptr<LogicalOperator>> &table_blocks,
                                         std::deque<idx_t> &table_blocks_key_order) {
	if (LogicalOperatorType::LOGICAL_GET == op->type) {
		auto &get_op = op->Cast<LogicalGet>();
		table_blocks.emplace(get_op.table_index, std::move(op));
		table_blocks_key_order.emplace_back(get_op.table_index);
	} else if (LogicalOperatorType::LOGICAL_CHUNK_GET == op->type) {
		auto &chunk_op = op->Cast<LogicalColumnDataGet>();
		table_blocks.emplace(chunk_op.table_index, std::move(op));
		table_blocks_key_order.emplace_back(chunk_op.table_index);
	} else if (LogicalOperatorType::LOGICAL_FILTER == op->type) {
		idx_t table_index;
		std::function<void(unique_ptr<LogicalOperator> & current_op)> find_get;
		find_get = [&find_get, &table_index](unique_ptr<LogicalOperator> &current_op) {
			for (auto &child_op : current_op->children) {
				if (LogicalOperatorType::LOGICAL_GET != child_op->type)
					find_get(child_op);
				else {
					auto &get_op = child_op->Cast<LogicalGet>();
					table_index = get_op.table_index;
				}
			}
		};
		find_get(op);
		table_blocks.emplace(table_index, std::move(op));
		table_blocks_key_order.emplace_back(table_index);
	} else if (LogicalOperatorType::LOGICAL_COMPARISON_JOIN == op->type) {
		auto &join_op = op->Cast<LogicalComparisonJoin>();
		if (JoinType::SEMI == join_op.join_type) {
			// insert the SEMI JOIN to `table_blocks`, e.g.
			// SEMI JION (table.index = CHUNK_GET.0)
			auto &left_child = join_op.children[0];
#ifdef DEBUG
			auto &right_child = join_op.children[1];
			D_ASSERT(LogicalOperatorType::LOGICAL_GET == left_child->type);
			D_ASSERT(LogicalOperatorType::LOGICAL_CHUNK_GET == right_child->type);
#endif
			idx_t table_index = left_child->Cast<LogicalGet>().table_index;
			table_blocks.emplace(table_index, std::move(op));
			table_blocks_key_order.emplace_back(table_index);
		} else if (JoinType::INNER == join_op.join_type) {
			// fixme: should have a smarter decision, but now we only use the right table's index
			auto &right_table = join_op.children[1]->Cast<LogicalGet>();
			table_blocks.emplace(right_table.table_index, std::move(op));
			table_blocks_key_order.emplace_back(right_table.table_index);
		}
	} else {
		Printer::Print(
		    StringUtil::Format("Do not support yet, block_op->type:  %s", LogicalOperatorToString(op->type)));
		D_ASSERT(false);
	}
}

bool SubqueryPreparer::BlockUsed(const unordered_set<idx_t> &left_cond_table_index,
                                 const unique_ptr<LogicalOperator> &op) {
	idx_t table_index;
	if (LogicalOperatorType::LOGICAL_GET == op->type) {
		auto &get_op = op->Cast<LogicalGet>();
		table_index = get_op.table_index;
	} else if (LogicalOperatorType::LOGICAL_CHUNK_GET == op->type) {
		auto &chunk_op = op->Cast<LogicalColumnDataGet>();
		table_index = chunk_op.table_index;
	} else if (LogicalOperatorType::LOGICAL_FILTER == op->type) {
		std::function<void(const unique_ptr<LogicalOperator> &current_op)> find_get;
		find_get = [&find_get, &table_index](const unique_ptr<LogicalOperator> &current_op) {
			for (auto &child_op : current_op->children) {
				if (LogicalOperatorType::LOGICAL_GET != child_op->type)
					find_get(child_op);
				else {
					auto &get_op = child_op->Cast<LogicalGet>();
					table_index = get_op.table_index;
				}
			}
		};
		find_get(op);
	} else if (LogicalOperatorType::LOGICAL_COMPARISON_JOIN == op->type) {
		auto &join_op = op->Cast<LogicalComparisonJoin>();
		// fixme: should have a smarter decision
		if (LogicalOperatorType::LOGICAL_GET == join_op.children[0]->type &&
		    LogicalOperatorType::LOGICAL_GET == join_op.children[1]->type) {
			// hint: if all tables have the same name under this JOIN, it shouldn't be a subquery, which means the risk
			// of use this JOIN block, ref: top_down.cpp, DSB query102_0.sql.
			auto &left_get = join_op.children[0]->Cast<LogicalGet>();
			auto &right_get = join_op.children[1]->Cast<LogicalGet>();
			auto left_table_name = left_get.function.to_string(left_get.bind_data.get());
			auto right_table_name = right_get.function.to_string(right_get.bind_data.get());
			if (left_table_name == right_table_name) {
				return left_cond_table_index.count(right_get.table_index) ||
				       left_cond_table_index.count(left_get.table_index);
			} else
				return true;
		} else {
			// all blocks should be used
			// fixme: might have bugs
			return true;
		}
	} else if (LogicalOperatorType::LOGICAL_CROSS_PRODUCT == op->type) {
		// no need to check the right child
		return BlockUsed(left_cond_table_index, op->children[0]);
	} else {
		Printer::Print(
		    StringUtil::Format("Do not support yet, block_op->type:  %s", LogicalOperatorToString(op->type)));
		D_ASSERT(false);
	}

	return left_cond_table_index.count(table_index);
}

void SubqueryPreparer::ExplainAnalyzeSubQuery(ClientContextLock &lock,
                                              shared_ptr<PreparedStatementData> original_stmt_data,
                                              unique_ptr<LogicalOperator> explain_sub_plan, idx_t catalog_version,
                                              string statement_query, idx_t n_param,
                                              case_insensitive_map_t<idx_t> named_param_map) {
	switch (explain_sub_plan->children[0]->type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_LIMIT:
		break;
	default:
		return;
	}

	auto explain_subquery_stmt = AdaptSelect(original_stmt_data, explain_sub_plan);
	auto explain_stmt_data = make_shared<PreparedStatementData>(StatementType::EXPLAIN_STATEMENT);
	auto explain_stmt =
	    make_uniq<ExplainStatement>(std::move(explain_subquery_stmt->unbound_statement), ExplainType::EXPLAIN_ANALYZE);
	explain_stmt_data->names = {"explain_key", "explain_value"};
	explain_stmt_data->types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
	explain_stmt_data->properties.return_type = StatementReturnType::QUERY_RESULT;
	//			explain_stmt_data->value_map
	explain_stmt_data->catalog_version = catalog_version;
	explain_stmt_data->unbound_statement = std::move(explain_stmt);

	PhysicalPlanGenerator explain_physical_planner(context);
	auto explain_physical_plan = explain_physical_planner.CreatePlan(std::move(explain_sub_plan));
	explain_stmt_data->plan = std::move(explain_physical_plan);
	auto explain_prepared_stmt = make_uniq<PreparedStatement>(context.shared_from_this(), std::move(explain_stmt_data),
	                                                          statement_query, n_param, named_param_map);
	duckdb::vector<Value> explain_bound_values;
	auto explain_result = explain_prepared_stmt->ExecuteRow(lock, explain_bound_values, false);
	Printer::Print("EXPLAIN ANALYZE:");
	explain_result->Print();
}

void SubqueryPreparer::RevertSubqueriesIndex(unique_ptr<Expression> &expr) {
	UpdateExprs(expr, [this](unique_ptr<Expression> &expr) {
		auto &column_binding = GetRefColumnBinding(expr);
		auto find_expr = stored_sub_plan_exprs.find(column_binding.table_index);
		if (find_expr != stored_sub_plan_exprs.end()) {
			auto &origin_expr = find_expr->second[column_binding.column_index];
			auto origin_expr_index = GetConstTableExpr(origin_expr);
			column_binding.table_index = origin_expr_index.table_idx;
			column_binding.column_index = origin_expr_index.column_idx;
		}
	});
}

unique_ptr<LogicalOperator> SubqueryPreparer::MergeBack(unique_ptr<LogicalOperator> last_sub_plan,
                                                        const unique_ptr<LogicalOperator> &sub_plan) {
	switch (sub_plan->type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_LIMIT:
		break;
	default:
		return nullptr;
	}

	auto current_sub_plan = sub_plan->Copy(context);
	// 1. remove the projection_map of JOINs in the current_sub_plan
	std::function<void(unique_ptr<LogicalOperator> & op)> remove_projection_map;
	remove_projection_map = [&remove_projection_map](unique_ptr<LogicalOperator> &op) {
		switch (op->type) {
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
			auto &join = op->Cast<LogicalComparisonJoin>();
			join.left_projection_map.clear();
			join.right_projection_map.clear();
			break;
		}
		default:
			break;
		}

		for (auto &child : op->children) {
			remove_projection_map(child);
		}
	};
	remove_projection_map(current_sub_plan);

	if (nullptr == last_sub_plan) {
		return current_sub_plan;
	}

#if ENABLE_DEBUG_PRINT
	Printer::Print("last_sub_plan");
	last_sub_plan->Print();
	Printer::Print("current_sub_plan");
	current_sub_plan->Print();
#endif

	// 2. revert the indexes of the current_sub_plan
	if (LogicalOperatorType::LOGICAL_LIMIT == last_sub_plan->type) {
		last_sub_plan = std::move(last_sub_plan->children[0]);
	}
	if (LogicalOperatorType::LOGICAL_ORDER_BY == last_sub_plan->type) {
		last_sub_plan = std::move(last_sub_plan->children[0]);
	}
#ifdef DEBUG
	D_ASSERT(LogicalOperatorType::LOGICAL_PROJECTION == last_sub_plan->type);
#endif
	auto &proj_node = last_sub_plan->Cast<LogicalProjection>();
	//	// 1.1. proj_node's expressions might have new_table_idx
	//	for (auto &proj_expr : proj_node.expressions) {
	//		RevertSubqueriesIndex(proj_expr);
	//	}
	stored_sub_plan_exprs[new_table_idx] = std::move(proj_node.expressions);

	// 3. remove projection head of the last_sub_plan
	last_sub_plan = std::move(last_sub_plan->children[0]);

	// 4. store the last_sub_plan into a map<merge_index, operator>
	stored_sub_plans[new_table_idx] = std::move(last_sub_plan);

	// 5. merge back the sub plans and revert the indexes
	std::function<void(unique_ptr<LogicalOperator> & op)> merge_back;
	merge_back = [&merge_back, this](unique_ptr<LogicalOperator> &op) {
		switch (op->type) {
		// todo: refactor to a standalone class
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			auto &proj = op->Cast<LogicalProjection>();
			auto &exprs = proj.expressions;
			for (auto &expr : exprs) {
				RevertSubqueriesIndex(expr);
			}
			break;
		}
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			auto &aggregate_op = op->Cast<LogicalAggregate>();
			// revert expr of group by
			for (auto &agg_group_expr : aggregate_op.groups) {
				RevertSubqueriesIndex(agg_group_expr);
			}
			// revert expr of aggregate op expression
			for (auto &agg_expr : aggregate_op.expressions) {
#ifdef DEBUG
				D_ASSERT(ExpressionType::BOUND_AGGREGATE == agg_expr->type);
#endif
				auto &aggregate_expr = agg_expr->Cast<BoundAggregateExpression>();
				for (auto &bound_agg_expr : aggregate_expr.children) {
					RevertSubqueriesIndex(bound_agg_expr);
				}
			}
			break;
		}
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
			auto &join = op->Cast<LogicalComparisonJoin>();
			auto &conditions = join.conditions;
			for (auto &cond : conditions) {
				RevertSubqueriesIndex(cond.left);
				RevertSubqueriesIndex(cond.right);
			}
			break;
		}
		case LogicalOperatorType::LOGICAL_FILTER: {
			auto &filter = op->Cast<LogicalFilter>();
			auto &exprs = filter.expressions;
			std::function<void(unique_ptr<Expression> & expr)> revert_index;
			revert_index = [this, &revert_index](unique_ptr<Expression> &expr) {
				switch (expr->type) {
				case ExpressionType::VALUE_CONSTANT:
					break;
				case ExpressionType::BOUND_FUNCTION: {
					auto &bound_func_expr = expr->Cast<BoundFunctionExpression>();
					for (auto &child_expr : bound_func_expr.children) {
						revert_index(child_expr);
					}
					break;
				}
				case ExpressionType::COMPARE_NOTEQUAL:
				case ExpressionType::COMPARE_EQUAL:
				case ExpressionType::COMPARE_GREATERTHAN:
				case ExpressionType::COMPARE_LESSTHAN:
				case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
				case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
					auto &compare_expr = expr->Cast<BoundComparisonExpression>();
					revert_index(compare_expr.left);
					revert_index(compare_expr.right);
					break;
				}
				case ExpressionType::CONJUNCTION_OR:
				case ExpressionType::CONJUNCTION_AND: {
					auto &conjunction_expr = expr->Cast<BoundConjunctionExpression>();
					for (auto &child_expr : conjunction_expr.children) {
						revert_index(child_expr);
					}
					break;
				}
				case ExpressionType::OPERATOR_IS_NULL:
				case ExpressionType::OPERATOR_IS_NOT_NULL:
				case ExpressionType::OPERATOR_NOT: {
					auto &operator_expr = expr->Cast<BoundOperatorExpression>();
					for (auto &child_expr : operator_expr.children) {
						revert_index(child_expr);
					}
					break;
				}
				default:
					RevertSubqueriesIndex(expr);
				}
			};

			for (auto &expr : exprs) {
				revert_index(expr);
			}
			break;
		}
		default:
			break;
		}

		for (auto child_it = op->children.begin(); child_it != op->children.end(); child_it++) {
			if (LogicalOperatorType::LOGICAL_CHUNK_GET == (*child_it)->type) {
				// check if it's a new generated one
				auto &chunk_get = (*child_it)->Cast<LogicalColumnDataGet>();
				auto find_sub_plan = stored_sub_plans.find(chunk_get.table_index);
				if (find_sub_plan != stored_sub_plans.end()) {
#ifdef DEBUG
					D_ASSERT(nullptr != find_sub_plan->second);
#endif
					op->children.erase(child_it);
					op->children.insert(child_it, std::move(find_sub_plan->second));
					stored_sub_plans.erase(find_sub_plan);
				}
			}
			merge_back(*child_it);
		}
	};
	merge_back(current_sub_plan);
#if ENABLE_DEBUG_PRINT
	Printer::Print("After MergeBack");
	current_sub_plan->Print();
#endif
	return current_sub_plan;
}

unique_ptr<LogicalOperator>
SubqueryPreparer::CheckTableUsage(LogicalOperator *current_join_pointer, unordered_set<idx_t> &left_cond_table_index,
                                  std::unordered_map<idx_t, unique_ptr<LogicalOperator>> &table_blocks,
                                  std::deque<idx_t> &table_blocks_key_order,
                                  std::queue<unique_ptr<LogicalOperator>> &unused_blocks) {
	auto &current_join = current_join_pointer->Cast<LogicalComparisonJoin>();

	// 1. collect the left-cond of JOIN, since the right child must be shown in the right-cond
	for (const auto &cond : current_join.conditions) {
		left_cond_table_index.emplace(GetConstTableExpr(cond.left).table_idx);
	}

	// 2. collect all tables below the current JOIN in the top-down order
	if (LogicalOperatorType::LOGICAL_CROSS_PRODUCT != current_join_pointer->children[0]->type) {
		// we only want to confirm if the CROSS_PRODUCT can be simplified
		// which means if the child node is not a CROSS_PRODUCT, it's not necessary to check
		return nullptr;
	}
	// fixme: it's an ugly way... since we cannot convert row pointer to unique_ptr
	auto last_cross_product = current_join_pointer;
	auto check_pointer = current_join_pointer->children[0].get();
	while (LogicalOperatorType::LOGICAL_CROSS_PRODUCT == check_pointer->type) {
		auto &cross_product_op = check_pointer->Cast<LogicalCrossProduct>();
		if (LogicalOperatorType::LOGICAL_COMPARISON_JOIN == cross_product_op.children[1]->type) {
			// skip the right hand JOINs, except SEMI JOIN
			auto &join_op = cross_product_op.children[1]->Cast<LogicalComparisonJoin>();
			if (JoinType::SEMI != join_op.join_type) {
				break;
			}
		}
		InsertTableBlocks(cross_product_op.children[1], table_blocks, table_blocks_key_order);
		check_pointer = check_pointer->children[0].get();
		last_cross_product = last_cross_product->children[0].get();
	}
	auto &last_block = last_cross_product->children[0];

	// 3. if the last block is unused in the last join, find the used table from `table_blocks_key_order` as the
	// new `last_block`
	bool used = BlockUsed(left_cond_table_index, last_block);
	if (!used) {
		idx_t last_sibling_index = -1;
		for (auto it = table_blocks_key_order.begin(); it != table_blocks_key_order.end(); it++) {
			if (left_cond_table_index.count(*it)) {
				last_sibling_index = *it;
				table_blocks_key_order.erase(it);
				break;
			}
		}
		if (left_cond_table_index.count(last_sibling_index)) {
			auto last_sibling = std::move(table_blocks[last_sibling_index]);
			table_blocks.erase(last_sibling_index);
			InsertTableBlocks(last_block, table_blocks, table_blocks_key_order);
			last_block = std::move(last_sibling);
		} else {
			// the last_block should be a JOIN with same tables.
#ifdef DEBUG
			D_ASSERT(LogicalOperatorType::LOGICAL_COMPARISON_JOIN == last_block->type);
#endif
		}
	}

	// 4. get the unused blocks
	for (auto &block : table_blocks) {
		if (!left_cond_table_index.count(block.first)) {
			unused_blocks.push(std::move(block.second));
		}
	}

	return std::move(last_block);
}

void SubqueryPreparer::RevertUsedBlocks(LogicalOperator *current_join_pointer, unique_ptr<LogicalOperator> last_block,
                                        deque<idx_t> &table_blocks_key_order,
                                        std::unordered_map<idx_t, unique_ptr<LogicalOperator>> &table_blocks) {
	LogicalOperator *revert_pointer = current_join_pointer;

	while (!table_blocks_key_order.empty() &&
	       LogicalOperatorType::LOGICAL_CROSS_PRODUCT == revert_pointer->children[0]->type) {
		revert_pointer = revert_pointer->children[0].get();
		auto &revert_op = revert_pointer->Cast<LogicalCrossProduct>();
		revert_op.children[1] = std::move(table_blocks[table_blocks_key_order.front()]);
		table_blocks_key_order.pop_front();
	}

#ifdef DEBUG
	D_ASSERT(nullptr == revert_pointer->children[0]);
#endif
	revert_pointer->children[0] = std::move(last_block);

#ifdef DEBUG
	D_ASSERT(table_blocks_key_order.empty());
#endif
}

void SubqueryPreparer::RevertUnusedBlocks(LogicalOperator *current_join_pointer,
                                          std::queue<unique_ptr<LogicalOperator>> &unused_blocks) {
	LogicalOperator *revert_pointer = current_join_pointer;

	while (!unused_blocks.empty() && LogicalOperatorType::LOGICAL_CROSS_PRODUCT == revert_pointer->children[0]->type) {
		revert_pointer = revert_pointer->children[0].get();
		auto &revert_op = revert_pointer->Cast<LogicalCrossProduct>();
		if (nullptr == revert_op.children[1]) {
			revert_op.children[1] = std::move(unused_blocks.front());
			unused_blocks.pop();
		}
	}

#ifdef DEBUG
	D_ASSERT(nullptr != revert_pointer->children[0]);
	D_ASSERT(unused_blocks.empty());
#endif
}

idx_t SubqueryPreparer::GetEstCard(const unique_ptr<LogicalOperator> &sub_plan) {
	std::function<void(const unique_ptr<LogicalOperator> &current_op)> get_est_card;
	idx_t est_card = 0;
	get_est_card = [&get_est_card, &est_card](const unique_ptr<LogicalOperator> &current_op) {
		for (auto &child_op : current_op->children) {
			get_est_card(child_op);
			est_card = est_card > child_op->estimated_cardinality ? est_card : child_op->estimated_cardinality;
		}
	};

	get_est_card(sub_plan);
	return est_card;
}
} // namespace duckdb
