//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/query_split.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <unordered_map>
#include <vector>

namespace duckdb {

class JoinPushDown {
public:
	explicit JoinPushDown(ClientContext &context) : context(context) {};
	~JoinPushDown() = default;

	//! Perform cross_product hoist
	//! 1. collect the conditions of the last JOIN
	//! 2. select the needed tables (or the FILTER block) and rest tables from CROSS_PRODUCTs
	//! 3. split the last JOIN
	//! 4. reorder the tables for the splitted JOINs
	//! 5. add the rest of CROSS_PRODUCTs and JOINs on top of the splitted JOINs
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op);

	//! Check if the logical plan has CrossProducts
	bool HasCrossProduct(const unique_ptr<LogicalOperator> &op);

	//! Check if the join order optimization can eliminate the current CROSS_PRODUCTs
	bool IsNecessaryToRewrite(const unique_ptr<LogicalOperator> &op);

private:
	ClientContext &context;
	void InsertTableBlocks(unique_ptr<LogicalOperator> &op,
	                       unordered_map<idx_t, unique_ptr<LogicalOperator>> &table_blocks);
};

} // namespace duckdb
