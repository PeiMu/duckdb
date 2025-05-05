#include "duckdb/optimizer/query_split/query_split.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> QuerySplit::Split(unique_ptr<LogicalOperator> plan, bool follow_pipeline_breaker) {
	// remove redundant joins if the current query is not a CMD_UTILITY
	// todo: check if the current query is a CMD_UTILITY
	if (LogicalOperatorType::LOGICAL_PROJECTION != plan->type && LogicalOperatorType::LOGICAL_ORDER_BY != plan->type &&
	    LogicalOperatorType::LOGICAL_LIMIT != plan->type && LogicalOperatorType::LOGICAL_EXPLAIN != plan->type) {
		return std::move(plan);
	}

	return query_splitter->Split(std::move(plan), follow_pipeline_breaker);
}

} // namespace duckdb
