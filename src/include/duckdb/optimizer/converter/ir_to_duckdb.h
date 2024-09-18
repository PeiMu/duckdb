//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/ir_to_duckdb.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/printer.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

#include "read.hpp"
#include "simplest_ir.h"

namespace duckdb {
class IRConverter {
public:
	IRConverter() = default;
	~IRConverter() = default;

	unique_ptr<LogicalOperator> InjectPlan(unique_ptr<LogicalOperator> duckdb_plan);

private:
	std::unordered_map<std::string, unique_ptr<LogicalGet>> GetTableMap(unique_ptr<LogicalOperator> &duckdb_plan);
	unique_ptr<LogicalOperator> ConstructPlan(LogicalOperator *new_plan, SimplestStmt *postgres_plan_pointer,
	                   unordered_map<std::string, unique_ptr<LogicalGet>> &table_map);
	ExpressionType ConvertCompType(SimplestComparisonType type);
	LogicalType ConvertVarType(SimplestVarType type);
};

}