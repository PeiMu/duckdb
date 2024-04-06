//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/bottom_up.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/query_split/split_algorithm.hpp"

#include <queue>

namespace duckdb {

//! Based on the DAG of the logical plan, we generate the subqueries bottom-up
class BottomUpSplit : public SplitAlgorithm {
public:
	explicit BottomUpSplit(ClientContext &context) : SplitAlgorithm(context) {};
	~BottomUpSplit() override = default;
	//! Perform Query Split
	std::queue<unique_ptr<LogicalOperator>> Split(unique_ptr<LogicalOperator> plan) override;

protected:
	void VisitOperator(LogicalOperator &op) override;
	void VisitComparisonJoin(LogicalComparisonJoin &op);
	void VisitFilter(LogicalFilter &op);
};

} // namespace duckdb