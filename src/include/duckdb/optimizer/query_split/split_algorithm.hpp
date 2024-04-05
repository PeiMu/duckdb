//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/foreign_key_center.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

#include <queue>

// debug
#include "duckdb/common/printer.hpp"

namespace duckdb {

class SplitAlgorithm : public LogicalOperatorVisitor {
public:
	explicit SplitAlgorithm(ClientContext &context) : context(context) {};
	~SplitAlgorithm() override = default;
	//! Perform Query Split
	virtual std::queue<unique_ptr<LogicalOperator>> Split(unique_ptr<LogicalOperator> plan) {
		std::queue<unique_ptr<LogicalOperator>> plan_vec;
		plan_vec.emplace(std::move(plan));
		return plan_vec;
	};

protected:
	ClientContext &context;
};

} // namespace duckdb