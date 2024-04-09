#include "duckdb/optimizer/query_split/top_down.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> TopDownSplit::Split(unique_ptr<LogicalOperator> plan) {
#ifdef DEBUG
	// debug
	plan->Print();
#endif

	// for the first n-1 subqueries, only select the most related nodes/expressions
	// for the last subquery, merge the previous subqueries
	if (subqueries.empty()) {
		VisitOperator(*plan);
	} else {
		// todo: fuse the current subquery with the last result
	}

	//	if (subqueries.front().size() > 1) {
	//		// todo: execute in parallel
	//	}
	//	unique_ptr<LogicalOperator> subquery = subqueries.front()[0]->Copy(context);
	//	subqueries.pop();
	//	subquery->Print();
	unique_ptr<LogicalOperator> subquery = std::move(test_subqueries.front());
#ifdef DEBUG
	// debug: print subquery
	Printer::Print("Current subquery");
	subquery->Print();
#endif

	return subquery;
}

void TopDownSplit::VisitOperator(LogicalOperator &op) {
	idx_t proj_idx = 0;
	vector<unique_ptr<Expression>> select_list;
	unique_ptr<LogicalOperator> test_child;

	if (LogicalOperatorType::LOGICAL_PROJECTION == op.type) {
		auto &proj_op = op.Cast<LogicalProjection>();
		//		for (const auto &expr : proj_op.expressions) {
		//			auto &col_ref_expr = expr->Cast<BoundColumnRefExpression>();
		//			auto select_expr = col_ref_expr.Copy();
		//			int a = 0;
		//		}
		auto &test_expr = proj_op.expressions[0];
		auto &col_ref_expr = test_expr->Cast<BoundColumnRefExpression>();
		auto select_expr = col_ref_expr.Copy();
		auto &col_ref_select_expr = select_expr->Cast<BoundColumnRefExpression>();
		col_ref_select_expr.binding.table_index = 1;
		//		select_list.emplace_back(std::move(select_expr));
		//		proj_idx = proj_op.table_index;

		test_subquery = proj_op.Copy(context);
		test_subquery->children.clear();
		test_subquery->expressions.clear();
		test_subquery->expressions.emplace_back(std::move(select_expr));
	}

	std::vector<LogicalOperator *> same_level_subqueries;
	// for now, we only check logical_filter and logical_comparison_join
	// basically, the split point is based on logical_comparison_join,
	// but if it has a logical_filter parent, then we split at the
	// logical_filter node
	for (auto &child : op.children) {
		switch (child->type) {
		case LogicalOperatorType::LOGICAL_FILTER:
			filter_parent = true;
			same_level_subqueries.emplace_back(child.get());
			test_child = child->Copy(context);
			break;
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
			if (filter_parent) {
				filter_parent = false;
			} else {
				same_level_subqueries.emplace_back(child.get());
				test_child = child->Copy(context);
			}
			break;
		default:
			filter_parent = false;
			break;
		}
		VisitOperator(*child);
	}

	if (1 == same_level_subqueries.size()) {
		//		auto test_subquery = make_uniq<LogicalProjection>(proj_idx, std::move(select_list));
		if (test_subquery) {
			test_subquery->AddChild(std::move(test_child));
			test_subqueries.emplace(std::move(test_subquery));
		}
	}

	D_ASSERT(same_level_subqueries.size() <= 2);
	if (!same_level_subqueries.empty()) {
		subqueries.emplace(same_level_subqueries);
	}
}

} // namespace duckdb
