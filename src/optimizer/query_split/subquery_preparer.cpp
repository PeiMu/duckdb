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

	if (expr_idx_pair_vec.size() > 1) {
		if (ENABLE_PARALLEL_EXECUTION) {
			// todo: execute in parallel
		} else {
			last_sibling_exprs.insert(expr_idx_pair_vec[1].begin(), expr_idx_pair_vec[1].end());
		}
	} else {
		last_sibling_exprs.clear();
	}

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
                                      unique_ptr<QueryResult> previous_result) {
	vector<LogicalType> types = previous_result->types;

	unique_ptr<MaterializedQueryResult> result_materialized;
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
#if TIME_BREAK_DOWN
	auto timer = chrono_tic();
#endif
	int chunk_size = 0;
	if (previous_result->type == QueryResultType::STREAM_RESULT) {
		auto &stream_query = previous_result->Cast<duckdb::StreamQueryResult>();
		result_materialized = stream_query.Materialize();
		collection = make_uniq<ColumnDataCollection>(result_materialized->Collection());
	} else if (previous_result->type == QueryResultType::MATERIALIZED_RESULT) {
		ColumnDataAppendState append_state;
		collection->InitializeAppend(append_state);
		unique_ptr<DataChunk> chunk;
		ErrorData error;
		while (true) {
			previous_result->TryFetch(chunk, error);
			if (!chunk || chunk->size() == 0) {
				break;
			}
			chunk_size += chunk->size();
			// set chunk cardinality
			chunk->SetCardinality(chunk->size());
			collection->Append(append_state, *chunk);
		}
	}
#if TIME_BREAK_DOWN
	chrono_toc(&timer, "generate collection time\n");
	Printer::Print("chunk size = " + std::to_string(chunk_size));
#endif

	// generate an unused table index by the binder
	new_table_idx = binder.GenerateTableIndex();

	chunk_scan = make_uniq<LogicalColumnDataGet>(new_table_idx, types, std::move(collection));
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
	subquery_pointer = subquery_pointer->children[0].get();
	D_ASSERT(subquery_pointer->children.empty());
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

	// find if `proj_expr` has the `old_table_idx` that need to be updated
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
} // namespace duckdb
