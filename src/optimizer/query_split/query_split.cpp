#include "duckdb/optimizer/query_split/query_split.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> QuerySplit::Split(unique_ptr<LogicalOperator> plan) {
	// remove redundant joins if the current query is not a CMD_UTILITY
	// todo: check if the current query is a CMD_UTILITY
	if (LogicalOperatorType::LOGICAL_PROJECTION != plan->type && LogicalOperatorType::LOGICAL_ORDER_BY != plan->type &&
	    LogicalOperatorType::LOGICAL_EXPLAIN != plan->type) {
		return std::move(plan);
	}

	if (ENABLE_QUERY_SPLIT) {
		return query_splitter->Split(std::move(plan));
	} else {
		return std::move(plan);
	}
}

} // namespace duckdb
