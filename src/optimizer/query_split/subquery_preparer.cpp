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
#if FOLLOW_PIPELINE_BREAKER
	if (expr_idx_pair_vec.size() > 1) {
		temp_proj_exprs.insert(expr_idx_pair_vec[1].begin(), expr_idx_pair_vec[1].end());
	}
#else
	if (expr_idx_pair_vec.size() > 1) {
		if (ENABLE_PARALLEL_EXECUTION) {
			// todo: execute in parallel
		} else {
			last_sibling_exprs.insert(expr_idx_pair_vec[1].begin(), expr_idx_pair_vec[1].end());
		}
	} else {
		last_sibling_exprs.clear();
	}
#endif

	// collect all columns with the same table from the upper levels
	std::set<TableExpr> used_expr_in_upper_levels;

	// `temp_stack` contains the `TableExpr` info of all the upper-level subqueries,
	// we try to merge the matching tables in a bottom-up order by levels
	auto temp_stack = table_expr_queue;
	temp_stack.pop();
	while (!temp_stack.empty()) {
		auto temp_vec = temp_stack.front();
		temp_stack.pop();
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

	auto new_plan = original_plan->Copy(context);
	new_plan->children.clear();
	new_plan->AddChild(std::move(subquery));
	new_plan->expressions.clear();
	new_plan->expressions = std::move(new_exprs);

#if ENABLE_DEBUG_PRINT
	// debug: print subquery
	Printer::Print("Current subquery with projection");
	new_plan->Print();
#endif
	return new_plan;
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

void SubqueryPreparer::MergeDataChunk(std::vector<unique_ptr<LogicalOperator>> &current_level_subqueries,
                                      unique_ptr<ColumnDataCollection> previous_result) {

//	unique_ptr<MaterializedQueryResult> result_materialized;
//	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
//#if TIME_BREAK_DOWN
//	auto timer = chrono_tic();
//#endif
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
//#if TIME_BREAK_DOWN
//	std::string str = "Fetch data with size=" + std::to_string(chunk_size) + ", ";
//	chrono_toc(&timer, str.data());
//#endif

	int64_t chunk_size = previous_result->Count();

	// generate an unused table index by the binder
	new_table_idx = binder.GenerateTableIndex();

	chunk_scan = make_uniq<LogicalColumnDataGet>(new_table_idx, previous_result->Types(), std::move(previous_result));
	chunk_scan->estimated_cardinality = chunk_size;
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
}

bool SubqueryPreparer::MergeSibling(std::vector<unique_ptr<LogicalOperator>> &current_level_subqueries,
                                    unique_ptr<LogicalOperator> last_sibling_node) {
	auto merge_sibling = [&last_sibling_node](LogicalOperator *subquery_pointer) {
		while (!subquery_pointer->children.empty()) {
			if (subquery_pointer->children.size() > 1 && nullptr == subquery_pointer->children[1]) {
#ifdef DEBUG
				D_ASSERT(nullptr != last_sibling_node);
#endif
				subquery_pointer->children[1] = std::move(last_sibling_node);
				return true;
			}
			subquery_pointer = subquery_pointer->children[0].get();
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
	           LogicalOperatorType::LOGICAL_CROSS_PRODUCT == op->type) {
		AddOldTableIndex(std::move(op->children[0]));
		AddOldTableIndex(std::move(op->children[1]));
	} else {
		Printer::Print(StringUtil::Format("Do not support yet, op->type:  %s", LogicalOperatorToString(op->type)));
		D_ASSERT(false);
	}
}

void SubqueryPreparer::MergeToSubquery(LogicalOperator &op, bool &merged) {
	for (auto child_it = op.children.begin(); child_it != op.children.end(); child_it++) {
		if (merged)
			return;
		// find the insert point and insert the `ColumnDataGet` node to the logical plan
		if (nullptr == (*child_it) || (*child_it)->split_index == merge_index) {
#ifdef DEBUG
			D_ASSERT(nullptr != chunk_scan);
#endif
			op.children.erase(child_it);
			op.children.insert(child_it, std::move(chunk_scan));
			merged = true;
			merge_index--;
			return;
		}
		MergeToSubquery(*(*child_it), merged);
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
		ret.push(new_vec);
		table_expr_queue.pop();
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
#ifdef DEBUG
	D_ASSERT(LogicalOperatorType::LOGICAL_PROJECTION == plan->type);
#endif
	auto &proj_op = plan->Cast<LogicalProjection>();
	if (LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY == proj_op.children[0]->type) {
		// update aggregate expressions
		auto &aggregate_op = proj_op.children[0]->Cast<LogicalAggregate>();
#ifdef DEBUG
		D_ASSERT(aggregate_op.expressions.size() == original_proj_expr.size());
#endif
		auto proj_expr_index = 0;
		for (auto &agg_expr : aggregate_op.expressions) {
#ifdef DEBUG
			D_ASSERT(ExpressionType::BOUND_AGGREGATE == agg_expr->type);
#endif
			auto &aggregate_expr = agg_expr->Cast<BoundAggregateExpression>();
			for (auto &expr : aggregate_expr.children) {
#ifdef DEBUG
				D_ASSERT(ExpressionType::BOUND_COLUMN_REF == expr->type);
#endif
				auto &column_ref_expr = expr->Cast<BoundColumnRefExpression>();
				column_ref_expr.binding.table_index = original_proj_expr[proj_expr_index].table_idx;
				column_ref_expr.binding.column_index = original_proj_expr[proj_expr_index].column_idx;
#ifdef DEBUG
				D_ASSERT(column_ref_expr.alias == original_proj_expr[proj_expr_index].column_name);
				D_ASSERT(column_ref_expr.return_type == original_proj_expr[proj_expr_index].return_type);
#endif
			}
			proj_expr_index++;
		}

	} else {
#ifdef DEBUG
		D_ASSERT(proj_op.expressions.size() == original_proj_expr.size());
#endif
		auto proj_expr_index = 0;
		for (auto &expr : proj_op.expressions) {
#ifdef DEBUG
			D_ASSERT(ExpressionType::BOUND_COLUMN_REF == expr->type);
#endif
			auto &column_ref_expr = expr->Cast<BoundColumnRefExpression>();
			column_ref_expr.binding.table_index = original_proj_expr[proj_expr_index].table_idx;
			column_ref_expr.binding.column_index = original_proj_expr[proj_expr_index].column_idx;
#ifdef DEBUG
			D_ASSERT(column_ref_expr.alias == original_proj_expr[proj_expr_index].column_name);
			D_ASSERT(column_ref_expr.return_type == original_proj_expr[proj_expr_index].return_type);
#endif
			proj_expr_index++;
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

bool SubqueryPreparer::Rewrite(unique_ptr<LogicalOperator> &plan) {
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
		break;
	default:
		return false;
	}

	// 1. collect the condition of the last join
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
		auto current_pair = join_pointers_pair.top();
		LogicalOperator *current_join_pointer = current_pair.first;
		int current_join_level = current_pair.second;
		join_pointers_pair.pop();
		auto &current_join = current_join_pointer->Cast<LogicalComparisonJoin>();

		// select the needed tables
		std::unordered_set<idx_t> left_cond_table_index;
		for (const auto &cond : current_join.conditions) {
#ifdef DEBUG
			D_ASSERT(ExpressionType::BOUND_COLUMN_REF == cond.left->type);
#endif
			auto &left_expr = cond.left->Cast<BoundColumnRefExpression>();
			left_cond_table_index.emplace(left_expr.binding.table_index);
		}

		// 2. collect all tables below the current JOIN in the top-down order
		std::unordered_map<idx_t, unique_ptr<LogicalOperator>> table_blocks;
		std::deque<idx_t> table_blocks_key_order;

		if (LogicalOperatorType::LOGICAL_CROSS_PRODUCT != current_join_pointer->children[0]->type) {
			continue;
		}
		// it's an ugly way... since we cannot convert row pointer to unique_ptr
		auto second_last_pointer = current_join_pointer;
		auto last_cross_product = current_join_pointer->children[0].get();
		while (LogicalOperatorType::LOGICAL_CROSS_PRODUCT == last_cross_product->type) {
			auto &cross_product_op = last_cross_product->Cast<LogicalCrossProduct>();
			if (LogicalOperatorType::LOGICAL_COMPARISON_JOIN == cross_product_op.children[1]->type) {
				break;
			}
			InsertTableBlocks(cross_product_op.children[1], table_blocks, table_blocks_key_order);
			last_cross_product = last_cross_product->children[0].get();
			second_last_pointer = second_last_pointer->children[0].get();
		}
		auto &last_block = second_last_pointer->children[0];

		// if the last block is unused in the last join, find the used table from `table_blocks_key_order` as the
		// `last_block`
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
#ifdef DEBUG
			D_ASSERT(left_cond_table_index.count(last_sibling_index));
#endif
			auto last_sibling = std::move(table_blocks[last_sibling_index]);
			table_blocks.erase(last_sibling_index);
			InsertTableBlocks(last_block, table_blocks, table_blocks_key_order);
			last_block = std::move(last_sibling);
		}

		std::queue<unique_ptr<LogicalOperator>> unused_blocks;
		for (auto &block : table_blocks) {
			if (!left_cond_table_index.count(block.first)) {
				unused_blocks.push(std::move(block.second));
			}
		}

		// if all tables below the last join are used
		if (unused_blocks.empty()) {
			// revert table blocks to plan
			auto revert_pointer = current_join_pointer;
			while (!table_blocks_key_order.empty() &&
			       LogicalOperatorType::LOGICAL_CROSS_PRODUCT == revert_pointer->children[0]->type) {
				revert_pointer = revert_pointer->children[0].get();
				auto &revert_op = revert_pointer->Cast<LogicalCrossProduct>();
				revert_op.children[1] = std::move(table_blocks[table_blocks_key_order.front()]);
				table_blocks_key_order.pop_front();
			}
#ifdef DEBUG
			D_ASSERT(table_blocks_key_order.empty());
#endif
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
 	return true;
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
		// if it is a JOIN, all blocks should be used
		return true;
	} else if (LogicalOperatorType::LOGICAL_CROSS_PRODUCT == op->type) {
#if DEBUG
		// no need to check the right JOIN
		D_ASSERT(LogicalOperatorType::LOGICAL_COMPARISON_JOIN == op->children[1]->type);
#endif
		return BlockUsed(left_cond_table_index, op->children[0]);
	} else {
		Printer::Print(
		    StringUtil::Format("Do not support yet, block_op->type:  %s", LogicalOperatorToString(op->type)));
		D_ASSERT(false);
	}

	return left_cond_table_index.count(table_index);
}
} // namespace duckdb
