#include "duckdb/optimizer/query_split/subquery_preparer.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> SubqueryPreparer::MergeDataChunk(unique_ptr<LogicalOperator> subquery,
                                                             unique_ptr<DataChunk> previous_result,
                                                             const set<TableExpr> &table_expr_set) {
	vector<LogicalType> types = previous_result->GetTypes();
	auto collection = make_uniq<ColumnDataCollection>(context, types);
	collection->Append(*previous_result);

	// generate an unused table index by the binder
	idx_t table_idx = binder.GenerateTableIndex();

	auto chunk_scan = make_uniq<LogicalColumnDataGet>(table_idx, types, std::move(collection));
	// todo: find the insert point and insert the `ColumnDataGet` node to the logical plan
	std::function<void(LogicalOperator & op)> get_insert_point_test;
	get_insert_point_test = [&chunk_scan, &get_insert_point_test](LogicalOperator &op) {
		for (auto child_it = op.children.begin(); child_it != op.children.end(); child_it++) {
			if (LogicalOperatorType::LOGICAL_FILTER == (*child_it)->type) {
				op.children.erase(child_it);
				op.children.emplace_back(std::move(chunk_scan));
				return;
			} else if (LogicalOperatorType::LOGICAL_COMPARISON_JOIN == (*child_it)->type) {
				auto &join_op = (*child_it)->Cast<LogicalComparisonJoin>();

			} else {
				get_insert_point_test(*(*child_it));
			}
		}
	};
	get_insert_point_test(*subquery);
#ifdef DEBUG
	// debug: print subquery
	Printer::Print("After merge data chunk");
	subquery->Print();
#endif
	// todo: update statistics?

	return std::move(subquery);
}

unique_ptr<LogicalOperator>
SubqueryPreparer::GenerateProjHead(const unique_ptr<LogicalOperator> &original_plan,
                                   unique_ptr<LogicalOperator> subquery,
                                   const std::stack<std::set<TableExpr>> &table_expr_stack) {
#ifdef DEBUG
	// debug: print subquery
	Printer::Print("Current subquery");
	subquery->Print();
#endif

	vector<unique_ptr<Expression>> new_exprs;
	// get the lowest-level `TableExpr` info
	auto expr_idx_pair = table_expr_stack.top();
	// collect all columns with the same table
	// `temp_stack` contains the `TableExpr` info of all the upper-level subqueries,
	// we try to merge the matching tables in a bottom-up order by levels
	auto temp_stack = table_expr_stack;
	while (!temp_stack.empty()) {
		auto temp = temp_stack.top();
		temp_stack.pop();
		std::set<TableExpr> temp_set;
		for (const auto &current_pair : expr_idx_pair) {
			auto same_table_it = std::find_if(temp.begin(), temp.end(), [current_pair](TableExpr table_expr) {
				return table_expr.table_idx == current_pair.table_idx;
			});
			if (same_table_it != temp.end()) {
				temp_set.emplace(*same_table_it);
			}
		}
		// merge the temp_set to expr_idx_pair
		if (!temp_set.empty())
			expr_idx_pair.insert(temp_set.begin(), temp_set.end());
	}
	for (const auto &expr_pair : expr_idx_pair) {
		ColumnBinding binding = ColumnBinding(expr_pair.table_idx, expr_pair.column_idx);
		auto col_ref_select_expr =
		    make_uniq<BoundColumnRefExpression>(expr_pair.column_name, expr_pair.return_type, binding, 0);
		new_exprs.emplace_back(std::move(col_ref_select_expr));
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
} // namespace duckdb
