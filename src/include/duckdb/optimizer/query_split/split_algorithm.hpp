//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/split_algorithm.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

#include <queue>

// debug
#include "duckdb/common/printer.hpp"
#include "duckdb/optimizer/query_split/query_split_util.h"

namespace duckdb {

enum EnumSplitAlgorithm { foreign_key_center = 1, min_sub_query, top_down };

using subquery_queue = std::deque<std::vector<unique_ptr<LogicalOperator>>>;
using table_expr_info = std::deque<std::vector<std::set<TableExpr>>>;

class SplitAlgorithm : public LogicalOperatorVisitor {
public:
	explicit SplitAlgorithm(ClientContext &context) : context(context) {};
	~SplitAlgorithm() override = default;
	//! Perform Query Split
	virtual unique_ptr<LogicalOperator> Split(unique_ptr<LogicalOperator> plan, bool follow_pipeline_breaker) {
		return plan;
	};

public:
	//! the collection of all levels of subqueries in a bottom-up order, e.g. the lowest level subquery is the first
	//! element in the deque and will be executed first.
	//! subqueries.front()[1] means the sibling subquery.
	subquery_queue subqueries;

protected:
	ClientContext &context;
	//! record the parent node to replace it to the valid child node
};

} // namespace duckdb