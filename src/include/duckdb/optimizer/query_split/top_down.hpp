//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/top_down.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/query_split/split_algorithm.hpp"

#include <queue>

namespace duckdb {

//! Based on the DAG of the logical plan, we generate the subqueries bottom-up
class TopDownSplit : public SplitAlgorithm {
public:
	explicit TopDownSplit(ClientContext &context) : SplitAlgorithm(context) {};
	~TopDownSplit() override = default;
	//! Perform Query Split
	unique_ptr<LogicalOperator> Split(unique_ptr<LogicalOperator> plan) override;

protected:
	void VisitOperator(LogicalOperator &op) override;

private:
	bool filter_parent = false;
	std::queue<std::vector<LogicalOperator*>> subqueries;
	std::queue<unique_ptr<LogicalOperator>> test_subqueries;
	unique_ptr<LogicalOperator> test_subquery;
};

} // namespace duckdb