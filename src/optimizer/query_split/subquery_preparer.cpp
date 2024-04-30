#include "duckdb/optimizer/query_split/subquery_preparer.hpp"

#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/stream_query_result.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> SubqueryPreparer::GenerateProjHead(const unique_ptr<LogicalOperator> &original_plan,
                                                               unique_ptr<LogicalOperator> subquery,
                                                               const table_expr_info &table_expr_queue) {
#ifdef DEBUG
	// debug: print subquery
	Printer::Print("Current subquery");
	subquery->Print();
#endif

	vector<unique_ptr<Expression>> new_exprs;
	// get the lowest-level `TableExpr` info
	auto expr_idx_pair_vec = table_expr_queue.front();
	proj_exprs.clear();
	proj_exprs = expr_idx_pair_vec[0];
	if (!last_sibling_exprs.empty()) {
		proj_exprs.insert(last_sibling_exprs.begin(), last_sibling_exprs.end());
	}

	if (expr_idx_pair_vec.size() > 1) {
		// todo: execute in parallel

		last_sibling_exprs = expr_idx_pair_vec[1];
	}
	// collect all columns with the same table
	// `temp_stack` contains the `TableExpr` info of all the upper-level subqueries,
	// we try to merge the matching tables in a bottom-up order by levels
	auto temp_stack = table_expr_queue;
	while (!temp_stack.empty()) {
		auto temp_vec = temp_stack.front();
		temp_stack.pop();
		for (const auto &temp : temp_vec) {
			std::set<TableExpr> temp_set;
			for (const auto &current_pair : proj_exprs) {
				auto same_table_it = std::find_if(temp.begin(), temp.end(), [current_pair](TableExpr table_expr) {
					return table_expr.table_idx == current_pair.table_idx;
				});
				if (same_table_it != temp.end()) {
					temp_set.emplace(*same_table_it);
				}
			}
			// merge the temp_set to proj_exprs
			if (!temp_set.empty())
				proj_exprs.insert(temp_set.begin(), temp_set.end());
		}
	}
	for (const auto &expr_pair : proj_exprs) {
		ColumnBinding binding = ColumnBinding(expr_pair.table_idx, expr_pair.column_idx);
		auto col_ref_select_expr =
		    make_uniq<BoundColumnRefExpression>(expr_pair.column_name, expr_pair.return_type, binding, 0);
		new_exprs.emplace_back(std::move(col_ref_select_expr));
#ifdef DEBUG
		// debug
		std::string str = "table index: " + std::to_string(binding.table_index) +
		                  ", column index: " + std::to_string(binding.column_index) +
		                  ", name: " + expr_pair.column_name;
		Printer::Print(str);
#endif
	}

	auto new_plan = original_plan->Copy(context);
	new_plan->children.clear();
	new_plan->AddChild(std::move(subquery));
	new_plan->expressions.clear();
	new_plan->expressions = std::move(new_exprs);

#ifdef DEBUG
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

unique_ptr<LogicalOperator> SubqueryPreparer::MergeDataChunk(unique_ptr<LogicalOperator> subquery,
                                                             unique_ptr<QueryResult> previous_result) {
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
		while (true) {
			unique_ptr<DataChunk> chunk;
			ErrorData error;
			previous_result->TryFetch(chunk, error);
			if (!chunk || chunk->size() == 0) {
				break;
			}
			collection->Append(append_state, *chunk);
		}
	}

	// generate an unused table index by the binder
	new_table_idx = binder.GenerateTableIndex();

	chunk_scan = make_uniq<LogicalColumnDataGet>(new_table_idx, types, std::move(collection));
	VisitOperator(*subquery);

#ifdef DEBUG
	std::string new_idx = "New table index: " + std::to_string(new_table_idx);
	Printer::Print(new_idx);
	// debug: print subquery
	Printer::Print("After merge data chunk");
	subquery->Print();
#endif
	// todo: update statistics?

	return std::move(subquery);
}

void SubqueryPreparer::VisitOperator(LogicalOperator &op) {
	// update the table_idx and column_idx
	VisitOperatorExpressions(op);

	for (auto child_it = op.children.begin(); child_it != op.children.end(); child_it++) {
		// find the insert point and insert the `ColumnDataGet` node to the logical plan
		if ((*child_it)->split_point) {
			op.children.erase(child_it);
			D_ASSERT(nullptr != chunk_scan);
			op.children.insert(child_it, std::move(chunk_scan));
			return;
		}
		VisitOperator(*(*child_it));
	}
}

unique_ptr<Expression> SubqueryPreparer::VisitReplace(BoundColumnRefExpression &expr,
                                                      unique_ptr<Expression> *expr_ptr) {
	auto find_table_it = std::find_if(proj_exprs.begin(), proj_exprs.end(), [&expr](const TableExpr &table_expr) {
		return table_expr.table_idx == expr.binding.table_index;
	});

	if (find_table_it != proj_exprs.end()) {
		expr.binding.table_index = new_table_idx;
		old_table_idx.emplace_back(find_table_it->table_idx);
		auto find_column_it = std::find_if(proj_exprs.begin(), proj_exprs.end(), [&expr](const TableExpr &table_expr) {
			return table_expr.column_idx == expr.binding.column_index;
		});
		D_ASSERT(find_column_it != proj_exprs.end());
		expr.binding.column_index = std::distance(proj_exprs.begin(), find_column_it);
	}

	return nullptr;
}

table_expr_info SubqueryPreparer::UpdateTableIndex(table_expr_info table_expr_queue) {
	table_expr_info ret;
	while (!table_expr_queue.empty()) {
		auto table_expr_vec = table_expr_queue.front();
		std::vector<std::set<TableExpr>> new_vec;
		for (const auto &table_expr_set : table_expr_vec) {
			auto find_it =
			    std::find_if(table_expr_set.begin(), table_expr_set.end(), [this](const TableExpr &table_expr) {
				    for (const auto &old_idx : old_table_idx) {
					    if (old_idx == table_expr.table_idx)
						    return true;
				    }
				    return false;
			    });
			std::set<TableExpr> new_set = table_expr_set;
			if (table_expr_set.end() != find_it) {
				TableExpr new_table_expr = (*find_it);
				// replace to the new table index (gotten in `MergeDataChunk`)
				new_table_expr.table_idx = new_table_idx;
				new_set.emplace(new_table_expr);
				new_set.erase(*find_it);
			}
			new_vec.emplace_back(new_set);
		}
		table_expr_queue.pop();
		ret.push(new_vec);
	}
	old_table_idx.clear();

	return ret;
}
} // namespace duckdb
