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

void QuerySplit::MergeSubquery(unique_ptr<LogicalOperator> &plan, subquery_queue old_subqueries) {
	query_splitter->MergeSubquery(plan, std::move(old_subqueries));
}

unique_ptr<LogicalOperator> QuerySplit::Rewrite(unique_ptr<LogicalOperator> &plan, bool &needToSplit) {
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_TRANSACTION:
	case LogicalOperatorType::LOGICAL_PRAGMA:
		return std::move(plan); // skip optimizing simple & often-occurring plans unaffected by rewrites
	default:
		break;
	}

	return query_splitter->Rewrite(plan, needToSplit);
}

void QuerySplit::UnMergeSubquery(unique_ptr<LogicalOperator> &plan) {
	query_splitter->UnMergeSubquery(plan);
}

} // namespace duckdb
