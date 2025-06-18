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

unique_ptr<LogicalOperator> QuerySplit::Clear(unique_ptr<LogicalOperator> plan) {
	// reset split_index to 0 and reverted to false
	VisitOperator(*plan);
	if (nullptr != query_splitter) {
		query_splitter->Clear();
		auto top_down_splitter = dynamic_cast<TopDownSplit *>(query_splitter.get());
		top_down_splitter->Clear();
	}
	return std::move(plan);
}

void QuerySplit::VisitOperator(LogicalOperator &op) {
	op.split_index = 0;
	op.reverted = false;
	for (auto &child : op.children) {
		VisitOperator(*child);
	}
};

} // namespace duckdb
