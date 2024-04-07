#include "duckdb/optimizer/query_split/top_down.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> TopDownSplit::Split(unique_ptr<LogicalOperator> plan) {
	// debug
	plan->Print();

	// for the first n-1 subqueries, only select the most related nodes/expressions
	// for the last subquery, merge the previous subqueries
	if (subqueries.empty()) {
		VisitOperator(*plan);
	} else {
		// todo: fuse the current subquery with the last result
	}

	// debug: print subquery
	Printer::Print("Current subquery");
	unique_ptr<LogicalOperator> subquery = std::move(subqueries.top());
	subqueries.pop();
	subquery->Print();

	return subquery;
}

void TopDownSplit::VisitOperator(LogicalOperator &op) {
	bool same_fuse_level = false;

	// for now, we only check logical_filter and logical_comparison_join
	// basically, the split point is based on logical_comparison_join,
	// but if it has a logical_filter parent, then we split at the
	// logical_filter node
	for (auto &child : op.children) {
		switch (child->type) {
		case LogicalOperatorType::LOGICAL_FILTER:
			filter_parent = true;
			if (!same_fuse_level) {
				current_fuse_level++;
				same_fuse_level = true;
			}
			child->fuse_level = current_fuse_level;
			subqueries.push(child->Copy(context));
			break;
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
			if (filter_parent) {
				filter_parent = false;
			} else {
				if (!same_fuse_level) {
					current_fuse_level++;
					same_fuse_level = true;
				}
				child->fuse_level = current_fuse_level;
				subqueries.push(child->Copy(context));
			}
			break;
		default:
			filter_parent = false;
			break;
		}

		VisitOperator(*child);
	}
}

} // namespace duckdb
