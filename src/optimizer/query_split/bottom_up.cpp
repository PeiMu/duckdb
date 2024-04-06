#include "duckdb/optimizer/query_split/bottom_up.hpp"

namespace duckdb {

std::queue<unique_ptr<LogicalOperator>> BottomUpSplit::Split(unique_ptr<LogicalOperator> plan) {
	// debug
	plan->Print();

	std::queue<unique_ptr<LogicalOperator>> plan_vec;
	plan_vec.emplace(std::move(plan));
	return plan_vec;
}
void BottomUpSplit::VisitOperator(LogicalOperator &op) {
	VisitOperatorChildren(op);
	VisitOperatorExpressions(op);
}

void BottomUpSplit::VisitComparisonJoin(LogicalComparisonJoin &op) {
}

void BottomUpSplit::VisitFilter(LogicalFilter &op) {
}

} // namespace duckdb
