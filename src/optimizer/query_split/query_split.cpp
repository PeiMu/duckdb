#include "duckdb/optimizer/query_split/query_split.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> QuerySplit::Optimize(unique_ptr<LogicalOperator> plan, bool &subquery_loop) {
	// remove redundant joins if the current query is not a CMD_UTILITY
	// todo: check if the current query is a CMD_UTILITY
	if (LogicalOperatorType::LOGICAL_PROJECTION != plan->type && LogicalOperatorType::LOGICAL_ORDER_BY != plan->type &&
	    LogicalOperatorType::LOGICAL_EXPLAIN != plan->type) {
		return plan;
	}

	// todo: move it to the constructor
	EnumSplitAlgorithm split_algorithm = top_down;
	if (nullptr == query_splitter)
		query_splitter = SplitAlgorithmFactor::CreateSplitter(context, split_algorithm);

	return query_splitter->Split(std::move(plan), subquery_loop);
}

} // namespace duckdb