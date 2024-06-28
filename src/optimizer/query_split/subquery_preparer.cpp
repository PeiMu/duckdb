#include "duckdb/optimizer/query_split/subquery_preparer.hpp"

#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/stream_query_result.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> SubqueryPreparer::GenerateProjHead(const unique_ptr<LogicalOperator> &original_plan,
                                                               unique_ptr<LogicalOperator> subquery,
                                                               const table_expr_info &table_expr_queue,
                                                               const std::set<TableExpr> &original_proj_expr,
                                                               const std::set<idx_t> &curren_level_used_table) {
#if ENABLE_DEBUG_PRINT
	// debug: print subquery
	Printer::Print("Current subquery");
	subquery->Print();
#endif

	vector<unique_ptr<Expression>> new_exprs;
	// get the lowest-level `TableExpr` info
	auto expr_idx_pair_vec = table_expr_queue.front();
	proj_exprs.clear();
	proj_exprs = expr_idx_pair_vec[0];
	// merge sibling node's expressions
	if (!last_sibling_exprs.empty()) {
		proj_exprs.insert(last_sibling_exprs.begin(), last_sibling_exprs.end());
	}

	if (expr_idx_pair_vec.size() > 1) {
		if (ENABLE_PARALLEL_EXECUTION) {
			// todo: execute in parallel
		} else {
			last_sibling_exprs = expr_idx_pair_vec[1];
		}
	} else {
		last_sibling_exprs.clear();
	}
	// collect all columns with the same table
	// `temp_stack` contains the `TableExpr` info of all the upper-level subqueries,
	// we try to merge the matching tables in a bottom-up order by levels
	auto temp_stack = table_expr_queue;
	temp_stack.pop();
	while (!temp_stack.empty()) {
		auto temp_vec = temp_stack.front();
		temp_stack.pop();
		for (const auto &temp : temp_vec) {
			std::set<TableExpr> temp_set;
			for (const auto &table_expr : temp) {
				// check if expressions in the upper operators still use tables in the current level,
				// which will be merged into DATA_CHUNK
				auto same_table_it =
				    std::find_if(curren_level_used_table.begin(), curren_level_used_table.end(),
				                 [table_expr](idx_t used_table_id) { return table_expr.table_idx == used_table_id; });
				if (same_table_it != curren_level_used_table.end()) {
					temp_set.emplace(table_expr);
				}
			}
			// merge the temp_set to proj_exprs
			if (!temp_set.empty())
				proj_exprs.insert(temp_set.begin(), temp_set.end());
		}
	}

	// merge projection's expressions if they have the same table index
	for (const auto &ori_proj_expr : original_proj_expr) {
		auto find_it = std::find_if(proj_exprs.begin(), proj_exprs.end(), [ori_proj_expr](TableExpr proj_expr) {
			return proj_expr.table_idx == ori_proj_expr.table_idx;
		});
		if (proj_exprs.end() != find_it) {
			proj_exprs.emplace(ori_proj_expr);
		}
	}

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

		// update `old_table_idx`
		old_table_idx.emplace(expr_pair.table_idx);
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
	D_ASSERT(QueryNodeType::SELECT_NODE == select_statemet.node->type);
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

unique_ptr<LogicalOperator> SubqueryPreparer::MergeDataChunk(const unique_ptr<LogicalOperator> &original_plan,
                                                             unique_ptr<LogicalOperator> subquery,
                                                             unique_ptr<QueryResult> previous_result,
                                                             bool last_subquery) {
	vector<LogicalType> types = previous_result->types;

	unique_ptr<MaterializedQueryResult> result_materialized;
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
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
			// set chunk cardinality
			chunk->SetCardinality(chunk->size());
			collection->Append(append_state, *chunk);
		}
	}

	// generate an unused table index by the binder
	new_table_idx = binder.GenerateTableIndex();

	chunk_scan = make_uniq<LogicalColumnDataGet>(new_table_idx, types, std::move(collection));
	bool merged = false;
	MergeToSubquery(*subquery, merged);
	D_ASSERT(merged);

	if (last_subquery) {
		// add the original projection head
		unique_ptr<LogicalOperator> ret = original_plan->Copy(context);
		auto child = ret.get();
		while (child->children[0]) {
			child = child->children[0].get();
		}
		child->children[0] = std::move(subquery);
#if ENABLE_DEBUG_PRINT
		// debug: print subquery
		Printer::Print("The last subquery");
		ret->Print();
#endif
		return ret;
	} else {
#if ENABLE_DEBUG_PRINT
		std::string new_idx = "New table index: " + std::to_string(new_table_idx);
		Printer::Print(new_idx);
		// debug: print subquery
//		Printer::Print("After merge data chunk");
//		subquery->Print();
#endif
		// todo: update statistics?

		return std::move(subquery);
	}
}

void SubqueryPreparer::MergeToSubquery(LogicalOperator &op, bool &merged) {
	for (auto child_it = op.children.begin(); child_it != op.children.end(); child_it++) {
		if (merged)
			return;
		// find the insert point and insert the `ColumnDataGet` node to the logical plan
		if (nullptr == (*child_it) || (*child_it)->split_point) {
			D_ASSERT(nullptr != chunk_scan);
			op.children.erase(child_it);
			op.children.insert(child_it, std::move(chunk_scan));
			merged = true;
			return;
		}
		MergeToSubquery(*(*child_it), merged);
	}
}

table_expr_info SubqueryPreparer::UpdateTableExpr(table_expr_info table_expr_queue,
                                                  std::set<TableExpr> &original_proj_expr,
                                                  std::set<idx_t> &curren_level_used_table) {
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
					D_ASSERT(proj_exprs.end() != find_column_it);
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
	for (auto it = original_proj_expr.begin(); it != original_proj_expr.end();) {
		if (old_table_idx.count(it->table_idx)) {
			TableExpr new_table_expr = (*it);
			// update column index based on the current level's "proj_exprs" order
			auto find_column_it = std::find_if(proj_exprs.begin(), proj_exprs.end(), [it](const TableExpr &proj_expr) {
				return proj_expr.table_idx == it->table_idx && proj_expr.column_idx == it->column_idx;
			});
			D_ASSERT(proj_exprs.end() != find_column_it);
			new_table_expr.column_idx = std::distance(proj_exprs.begin(), find_column_it);
			// replace to the new table index (gotten in `MergeDataChunk`)
			new_table_expr.table_idx = new_table_idx;
			it = original_proj_expr.erase(it);
			original_proj_expr.emplace(new_table_expr);
		} else {
			it++;
		}
	}

	// add new_table_idx to "curren_level_used_table" since the merged CHUNK's table index is "new_table_idx"
	curren_level_used_table.emplace(new_table_idx);

//	old_table_idx.clear();

	return ret;
}

unique_ptr<LogicalOperator> SubqueryPreparer::UpdateProjHead(unique_ptr<LogicalOperator> plan,
                                                             const std::set<TableExpr> &original_proj_expr) {
	D_ASSERT(LogicalOperatorType::LOGICAL_PROJECTION == plan->type);
	auto &proj_op = plan->Cast<LogicalProjection>();
	if (LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY == proj_op.children[0]->type) {
		// update aggregate expressions
		auto &aggregate_op = proj_op.children[0]->Cast<LogicalAggregate>();
		D_ASSERT(aggregate_op.expressions.size() == original_proj_expr.size());
		auto it = original_proj_expr.cbegin();
		for (auto &agg_expr : aggregate_op.expressions) {
			D_ASSERT(ExpressionType::BOUND_AGGREGATE == agg_expr->type);
			auto &aggregate_expr = agg_expr->Cast<BoundAggregateExpression>();
			// todo: check: maybe we don't need to search?
			for (auto &expr : aggregate_expr.children) {
				D_ASSERT(ExpressionType::BOUND_COLUMN_REF == expr->type);
				auto &column_ref_expr = expr->Cast<BoundColumnRefExpression>();
				column_ref_expr.binding.table_index = it->table_idx;
				column_ref_expr.binding.column_index = it->column_idx;
			}
			it++;
//			for (auto &expr : aggregate_expr.children) {
//				D_ASSERT(ExpressionType::BOUND_COLUMN_REF == expr->type);
//				auto &column_ref_expr = expr->Cast<BoundColumnRefExpression>();
//				auto find_it = std::find_if(original_proj_expr.begin(), original_proj_expr.end(),
//				                            [&column_ref_expr](const TableExpr &original_expr) {
//					                            // todo: cannot check by name
//					                            return original_expr.column_name == column_ref_expr.alias;
//				                            });
//				if (find_it != original_proj_expr.end()) {
//					column_ref_expr.binding.table_index = find_it->table_idx;
//					column_ref_expr.binding.column_index = find_it->column_idx;
//				}
//			}
		}
	} else {
		for (auto &expr : proj_op.expressions) {
			D_ASSERT(ExpressionType::BOUND_COLUMN_REF == expr->type);
			auto &column_ref_expr = expr->Cast<BoundColumnRefExpression>();
			auto find_it = std::find_if(original_proj_expr.begin(), original_proj_expr.end(),
			                            [&column_ref_expr](const TableExpr &original_expr) {
				                            return original_expr.column_name == column_ref_expr.alias;
			                            });
			if (find_it != original_proj_expr.end()) {
				column_ref_expr.binding.table_index = find_it->table_idx;
				column_ref_expr.binding.column_index = find_it->column_idx;
			}
		}
	}
	return std::move(plan);
}

unique_ptr<Expression> SubqueryPreparer::VisitReplace(BoundColumnRefExpression &expr,
                                                      unique_ptr<Expression> *expr_ptr) {
	const auto find_expr_it = std::find_if(proj_exprs.begin(), proj_exprs.end(), [&expr](const TableExpr &table_expr) {
		return table_expr.table_idx == expr.binding.table_index && table_expr.column_idx == expr.binding.column_index;
	});

	if (find_expr_it != proj_exprs.end()) {
		expr.binding.table_index = new_table_idx;
		expr.binding.column_index = std::distance(proj_exprs.begin(), find_expr_it);
		old_table_idx.emplace(find_expr_it->table_idx);
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

void SubqueryPreparer::MergeSubquery(unique_ptr<LogicalOperator> &plan, unique_ptr<LogicalOperator> subquery) {

}

void SubqueryPreparer::UpdatePlanIndex(unique_ptr<LogicalOperator> &plan) {
	VisitOperator(*plan);
}
} // namespace duckdb
