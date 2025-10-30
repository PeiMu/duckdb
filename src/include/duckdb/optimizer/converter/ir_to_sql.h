//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duckdb_to_ir.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "simplest_ir.h"

namespace duckdb {
class IRToSQLConverter {
public:
	IRToSQLConverter() {};
	~IRToSQLConverter() = default;

	std::string LogicalPlanToSQL(const unique_ptr<SimplestStmt> &plan);

	void SetTableColumnMappings(const std::unordered_map<std::string, std::vector<std::string>> &mappings) {
		table_column_mappings = mappings;
	}

private:
	void GenerateSQL(const unique_ptr<SimplestStmt> &op);
	std::string TranslateSimplestAggFnType(SimplestAggFnType agg_fn_type);
	std::string CollectFilter(const unique_ptr<SimplestExpr> &qual_expr);

	std::string GetActualColumnName(const std::string &table_name, const std::string &original_col_name,
	                                unsigned int col_position);

	unsigned int agg_field_key(unsigned int table_idx, unsigned int column_idx) {
		return std::hash<unsigned int>()(table_idx) ^ std::hash<unsigned int>()(column_idx);
	}

	std::vector<std::string> select_field;
	// fixme: might have a bug with multiple agg functions on the same attr, can use a std::vector<std::string> to solve
	std::unordered_map<unsigned int, std::string> agg_field;
	std::vector<std::string> filter_field;
	std::vector<std::string> join_field;
	std::vector<std::string> group_by;
	std::vector<std::string> order_by;

	std::unordered_map<unsigned int, std::string> table_names;
	std::unordered_map<unsigned int, std::vector<std::string>> chunk_contents;

	// mapping from table_name -> actual column names in created table
	std::unordered_map<std::string, std::vector<std::string>> table_column_mappings;

	// mapping from table_idx -> original column names (from IR)
	std::unordered_map<unsigned int, std::vector<std::string>> original_column_names;
};
} // namespace duckdb