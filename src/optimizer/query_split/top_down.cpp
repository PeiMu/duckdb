#include "duckdb/optimizer/query_split/top_down.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> TopDownSplit::Split(unique_ptr<LogicalOperator> plan) {
#ifdef DEBUG
	// debug
	plan->Print();
#endif

	auto new_plan = plan->Copy(context);
	// for the first n-1 subqueries, only select the most related nodes/expressions
	// for the last subquery, merge the previous subqueries
	if (subqueries.empty()) {
		VisitOperator(*plan);
	} else {
		// todo: fuse the current subquery with the last result
	}

	if (subqueries.front().size() > 1) {
		// todo: execute in parallel
	}
	unique_ptr<LogicalOperator> subquery = subqueries.front()[0]->Copy(context);
	subqueries.pop();
#ifdef DEBUG
	// debug: print subquery
	Printer::Print("Current subquery");
	subquery->Print();
#endif

	// Check which tables are used in the subquery
	ProjSelector proj_selector;
	proj_selector.VisitOperator(*subquery);

	D_ASSERT(LogicalOperatorType::LOGICAL_PROJECTION == new_plan->type);
	auto &proj_op = new_plan->Cast<LogicalProjection>();
	auto expr_idx_pair = proj_selector.GetProjExprIndexPair(proj_op);
	vector<unique_ptr<Expression>> new_exprs;
	for (const auto &expr_pair : expr_idx_pair) {
		if (UINT64_MAX == expr_pair.expression_idx) {
			// such table's any column doesn't exist in the expression
			// skip and check for the next table
			continue;
		}
		auto &expr = proj_op.expressions[expr_pair.expression_idx];
		auto &col_ref_expr = expr->Cast<BoundColumnRefExpression>();
		auto select_expr = col_ref_expr.Copy();
		auto &col_ref_select_expr = select_expr->Cast<BoundColumnRefExpression>();
		col_ref_select_expr.binding.table_index = expr_pair.table_idx;
		select_expr->alias = expr_pair.column_name;
		new_exprs.emplace_back(std::move(select_expr));
	}

	proj_op.children.clear();
	proj_op.AddChild(std::move(subquery));
	proj_op.expressions.clear();
	proj_op.expressions = std::move(new_exprs);

#ifdef DEBUG
	// debug: print subquery
	Printer::Print("Current subquery with projection");
	new_plan->Print();
#endif
	return new_plan;
}

void TopDownSplit::VisitOperator(LogicalOperator &op) {
	std::vector<LogicalOperator *> same_level_subqueries;
	// For now, we only check logical_filter and logical_comparison_join.
	// Basically, the split point is based on logical_comparison_join,
	// but if it has a logical_filter parent, then we split at the
	// logical_filter node.
	for (auto &child : op.children) {
		switch (child->type) {
		case LogicalOperatorType::LOGICAL_FILTER:
			filter_parent = true;
			same_level_subqueries.emplace_back(child.get());
			break;
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
			if (filter_parent) {
				filter_parent = false;
			} else {
				same_level_subqueries.emplace_back(child.get());
			}
			break;
		default:
			filter_parent = false;
			break;
		}
		VisitOperator(*child);
	}

	D_ASSERT(same_level_subqueries.size() <= 2);
	if (!same_level_subqueries.empty()) {
		subqueries.emplace(same_level_subqueries);
	}
}

void ProjSelector::VisitOperator(LogicalOperator &op) {
	if (LogicalOperatorType::LOGICAL_GET == op.type) {
		VisitGet(op.Cast<LogicalGet>());
	}
	VisitOperatorChildren(op);
}

void ProjSelector::VisitGet(LogicalGet &op) {
	auto current_table_index = op.table_index;
	auto &table_entry = op.GetTable()->Cast<TableCatalogEntry>();

	target_tables.emplace(current_table_index, &table_entry);
}

std::vector<TableExprPair> ProjSelector::GetProjExprIndexPair(const LogicalProjection &proj_op) {
	std::vector<TableExprPair> res;
	for (const auto &entry : target_tables) {
		// check which column match in the projection's expression
		TableExprPair current_pair;
		current_pair.table_idx = UINT64_MAX;
		current_pair.expression_idx = UINT64_MAX;
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
			if (entry.second->ColumnExists(expr_name)) {
				current_pair.expression_idx = expr_id;
				break;
			}
		}

		// there might be bugs when different tables have the same column name
		auto entry_exist = std::find_if(
		    res.begin(), res.end(), [entry](TableExprPair index_pair) { return index_pair.table_idx = entry.first; });
		D_ASSERT(entry_exist == res.end());
		current_pair.table_idx = entry.first;
		res.emplace_back(current_pair);
	}
	return res;
}

} // namespace duckdb
