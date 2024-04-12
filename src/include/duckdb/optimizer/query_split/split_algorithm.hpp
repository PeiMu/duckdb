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

enum EnumSplitAlgorithm { foreign_key_center = 1, min_sub_query, top_down };

class SplitAlgorithm : public LogicalOperatorVisitor {
public:
	explicit SplitAlgorithm(ClientContext &context) : context(context) {};
	~SplitAlgorithm() override = default;
	//! Perform Query Split
	virtual unique_ptr<LogicalOperator> Split(unique_ptr<LogicalOperator> plan, bool &subquery_loop) {
		return std::move(plan);
	};

protected:
	ClientContext &context;
	//! record the parent node to replace it to the valid child node
	unique_ptr<LogicalOperator> parent;
};

} // namespace duckdb