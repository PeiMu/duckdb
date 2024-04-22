#include "duckdb/optimizer/query_split/top_down.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> TopDownSplit::Split(unique_ptr<LogicalOperator> plan) {
	// for the first n-1 subqueries, only select the most related nodes/expressions
	// for the last subquery, merge the previous subqueries
	unique_ptr<LogicalOperator> subquery;
#ifdef DEBUG
	// debug
	plan->Print();
#endif
	GetTargetTables(*plan);
	VisitOperator(*plan);
	return std::move(plan);
}

void TopDownSplit::VisitOperator(LogicalOperator &op) {
	std::vector<unique_ptr<LogicalOperator>> same_level_subqueries;
	std::vector<std::set<TableExpr>> same_level_table_exprs;
	// todo: collect table_expr_queue from projection node
	// if (op.type == LogicalOperatorType::LOGICAL_PROJECTION)
	// GetProjTableExpr(op.Cast<LogicalProjection>());

	// For now, we only check logical_filter and logical_comparison_join.
	// Basically, the split point is based on logical_comparison_join,
	// but if it has a logical_filter parent, then we split at the
	// logical_filter node.
	for (auto &child : op.children) {
		std::set<TableExpr> table_exprs;
		switch (child->type) {
		case LogicalOperatorType::LOGICAL_FILTER:
			// add filter's column usage
			table_exprs = GetFilterTableExpr(child->Cast<LogicalFilter>());
			// check if it's a filter node, otherwise set false
			filter_parent = true;
			child->split_point = true;
			break;
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
			table_exprs = GetJoinTableExpr(child->Cast<LogicalComparisonJoin>());
			if (filter_parent) {
				filter_parent = false;
			} else {
				child->split_point = true;
			}
			break;
		default:
			filter_parent = false;
			break;
		}
		if (!table_exprs.empty()) {
			same_level_table_exprs.emplace_back(table_exprs);
		}
		VisitOperator(*child);

		if (child->split_point) {
			same_level_subqueries.emplace_back(child->Copy(context));
		}
	}

	D_ASSERT(same_level_subqueries.size() <= 2);
	if (!same_level_subqueries.empty()) {
		subqueries.emplace(std::move(same_level_subqueries));
	}
	D_ASSERT(same_level_table_exprs.size() <= 2);
	if (!same_level_table_exprs.empty()) {
		table_expr_queue.emplace(same_level_table_exprs);
	}
}

void TopDownSplit::GetProjTableExpr(const LogicalProjection &proj_op) {
	std::set<TableExpr> proj_pair;
	// check which column match in the projection's expression
	TableExpr current_pair;
	current_pair.table_idx = UINT64_MAX;
	current_pair.column_idx = UINT64_MAX;
	for (size_t expr_id = 0; expr_id < proj_op.expressions.size(); expr_id++) {
		auto expr_name = proj_op.expressions[expr_id]->GetName();
		// find the real column name inside the potential brackets
		auto last_left_bracket = expr_name.find_last_of('(');
		if (std::string::npos != last_left_bracket) {
			auto first_right_bracket = expr_name.find_first_of(')');
			D_ASSERT(first_right_bracket > last_left_bracket);
			expr_name = expr_name.substr(last_left_bracket + 1, first_right_bracket - last_left_bracket - 1);
		}
		current_pair.column_name = expr_name;
		current_pair.return_type = proj_op.expressions[expr_id]->return_type;

		for (const auto &table : used_tables) {
			auto column_idx = table.second->GetTable()->GetColumnIndex(expr_name, true);
			if (column_idx.IsValid()) {
				current_pair.column_idx = column_idx.index;
				current_pair.table_idx = table.first;
				break;
			}
		}
		D_ASSERT(UINT64_MAX != current_pair.table_idx);
		D_ASSERT(UINT64_MAX != current_pair.column_idx);
		proj_pair.emplace(current_pair);
	}

	if (!proj_pair.empty()) {
		std::vector<std::set<TableExpr>> proj_pair_vec {proj_pair};
		table_expr_queue.emplace(proj_pair_vec);
	}
}

void TopDownSplit::GetTargetTables(LogicalOperator &op) {
	if (LogicalOperatorType::LOGICAL_GET == op.type) {
		auto &get_op = op.Cast<LogicalGet>();
		auto current_table_index = get_op.table_index;
		used_tables.emplace(current_table_index, &get_op);
	}
	for (auto &child : op.children) {
		GetTargetTables(*child);
	}
}

std::set<TableExpr> TopDownSplit::GetJoinTableExpr(const LogicalComparisonJoin &join_op) {
	std::set<TableExpr> table_exprs;
	for (const auto &cond : join_op.conditions) {
		D_ASSERT(ExpressionType::BOUND_COLUMN_REF == cond.left->type);
		TableExpr current_left_table;
		auto &left_expr = cond.left->Cast<BoundColumnRefExpression>();
		current_left_table.table_idx = left_expr.binding.table_index;
		// it's the temporary id, but not physical id, e.g. by get.column_ids
		current_left_table.column_idx = left_expr.binding.column_index;
		current_left_table.column_name = left_expr.alias;
		current_left_table.return_type = left_expr.return_type;
		if (used_tables.count(current_left_table.table_idx))
			table_exprs.emplace(current_left_table);

		TableExpr current_right_table;
		auto &right_expr = cond.right->Cast<BoundColumnRefExpression>();
		current_right_table.table_idx = right_expr.binding.table_index;
		current_right_table.column_idx = right_expr.binding.column_index;
		current_right_table.column_name = right_expr.alias;
		current_right_table.return_type = right_expr.return_type;
		if (used_tables.count(current_right_table.table_idx))
			table_exprs.emplace(current_right_table);
	}
	return table_exprs;
}

std::set<TableExpr> TopDownSplit::GetFilterTableExpr(const LogicalFilter &filter_op) {
	std::set<TableExpr> table_exprs;

	auto get_column_ref_expr = [&table_exprs, this](const unique_ptr<Expression> &expr) {
		TableExpr table_expr;
		auto &column_ref_expr = expr->Cast<BoundColumnRefExpression>();
		table_expr.table_idx = column_ref_expr.binding.table_index;
		table_expr.column_idx = column_ref_expr.binding.column_index;
		table_expr.column_name = column_ref_expr.alias;
		table_expr.return_type = column_ref_expr.return_type;
		if (used_tables.count(table_expr.table_idx)) {
			table_exprs.emplace(table_expr);
		}
	};

	for (const auto &expr : filter_op.expressions) {
		if (ExpressionType::BOUND_COLUMN_REF == expr->type) {
			get_column_ref_expr(expr);
		} else if (ExpressionType::BOUND_FUNCTION == expr->type) {
			auto &function_expr = expr->Cast<BoundFunctionExpression>();
			for (const auto &func_child : function_expr.children) {
				if (ExpressionType::BOUND_COLUMN_REF == func_child->type) {
					get_column_ref_expr(func_child);
				} else if (ExpressionType::VALUE_CONSTANT == func_child->type) {
					// it's a constant value, skip it
				} else {
					Printer::Print("Do not support yet");
				}
			}
		} else {
			Printer::Print("Do not support yet");
		}
	}
	return table_exprs;
}

} // namespace duckdb
