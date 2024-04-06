#include "duckdb/optimizer/query_split/query_split.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> QuerySplit::Optimize(unique_ptr<LogicalOperator> plan) {
	// remove redundant joins if the current query is not a CMD_UTILITY
	// todo: check if the current query is a CMD_UTILITY
	if (LogicalOperatorType::LOGICAL_PROJECTION != plan->type && LogicalOperatorType::LOGICAL_ORDER_BY != plan->type &&
	    LogicalOperatorType::LOGICAL_EXPLAIN != plan->type) {
		return plan;
	}

	EnumSplitAlgorithm split_algorithm = foreign_key_center;

	std::unique_ptr<SplitAlgorithm> query_splitter = SplitAlgorithmFactor::CreateSplitter(context, split_algorithm);

	if (subqueries.empty()) {
		// todo: if the current result is not null, which means not the first time, then exist
		if (false) {
			return nullptr;
		}
		subqueries = query_splitter->Split(std::move(plan));
	} else {
		// fuse the current result with the next subquery
	}

	auto current_subquery = std::move(subqueries.front());
	subqueries.pop();

	return current_subquery;
}

} // namespace duckdb