#include "duckdb/optimizer/query_split/query_split.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> QuerySplit::Optimize(unique_ptr<LogicalOperator> plan) {
	// remove redundant joins if the current query is not a CMD_UTILITY
	// todo: check if the current query is a CMD_UTILITY
	if (LogicalOperatorType::LOGICAL_PROJECTION != plan->type && LogicalOperatorType::LOGICAL_ORDER_BY != plan->type &&
	    LogicalOperatorType::LOGICAL_EXPLAIN != plan->type) {
		return plan;
	}

	EnumSplitAlgorithm split_algorithm = top_down;

	std::unique_ptr<SplitAlgorithm> query_splitter = SplitAlgorithmFactor::CreateSplitter(context, split_algorithm);

	return query_splitter->Split(std::move(plan));
}

} // namespace duckdb