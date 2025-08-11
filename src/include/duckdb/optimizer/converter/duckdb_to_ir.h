//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duckdb_to_ir.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/optimizer/query_split/split_algorithm.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "read.hpp"
#include "simplest_ir.h"

#define CONVERT_DUCKDB_TO_IR true

namespace duckdb {
class DuckToIRConverter {
public:
	DuckToIRConverter(Binder &binder, ClientContext &context) : binder(binder), context(context) {};
	~DuckToIRConverter() = default;

	unique_ptr<SimplestStmt>
	ConstructSimplestStmt(LogicalOperator *duckdb_plan_pointer,
	                      const std::unordered_map<unsigned int, std::string> &intermediate_table_map);

private:
	unique_ptr<SimplestProjection> ConstructSimplestProj(LogicalProjection &proj_op, unique_ptr<SimplestStmt> child);
	unique_ptr<SimplestJoin> ConstructSimplestJoin(LogicalComparisonJoin &join_op, unique_ptr<SimplestStmt> left_child,
	                                               unique_ptr<SimplestStmt> right_child);
	unique_ptr<SimplestFilter> ConstructSimplestFilter(LogicalFilter &filter_op, unique_ptr<SimplestStmt> child);
	unique_ptr<SimplestScan> ConstructSimplestScan(LogicalGet &get_op);
	unique_ptr<SimplestScan> ConstructSimplestScan(LogicalColumnDataGet &get_op, std::string intermediate_table_name);
	unique_ptr<SimplestChunk> ConstructSimplestChunk(LogicalColumnDataGet &column_data_get_op);

	SimplestExprType ConvertCompType(ExpressionType type);
	SimplestVarType ConvertVarType(LogicalType type);
	SimplestAggFnType ConvertAggFnType(std::string agg_fn_type);

	std::vector<unique_ptr<SimplestExpr>> CollectQualVecExprs(const vector<unique_ptr<Expression>> &exprs);
	unique_ptr<SimplestExpr> CollectScanFilter(const unique_ptr<TableFilter> &filter_cond,
	                                           unique_ptr<SimplestAttr> var_attr);

	Binder &binder;
	ClientContext &context;
	unique_ptr<SimplestCrossProduct> ConstructSimplestCrossProduct(LogicalCrossProduct &cross_product_op,
	                                                               unique_ptr<SimplestStmt> left_child,
	                                                               unique_ptr<SimplestStmt> right_child);
	unique_ptr<SimplestAggregate> ConstructSimplestAggGroup(LogicalAggregate &agg_group_op,
	                                                        unique_ptr<SimplestStmt> child);
};

} // namespace duckdb