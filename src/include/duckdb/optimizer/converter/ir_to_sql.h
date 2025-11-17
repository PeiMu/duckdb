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

	void SetTableColumnMappings(
	    const std::unordered_map<std::pair<idx_t, idx_t>, std::string, pair_hash> &mappings) {
		table_column_mappings = mappings;
	}

private:
	void GenerateSQL(const unique_ptr<SimplestStmt> &op);
	std::string TranslateSimplestAggFnType(SimplestAggFnType agg_fn_type);
	std::string CollectFilter(const unique_ptr<SimplestExpr> &qual_expr);

	std::string GetActualColumnName(idx_t table_index, idx_t column_index, const std::string &original_col_name);

	unsigned int agg_field_key(unsigned int table_idx, unsigned int column_idx) {
		return std::hash<unsigned int>()(table_idx) ^ std::hash<unsigned int>()(column_idx);
	}

	std::vector<std::string> select_field;
	// fixme: might have a bug with multiple agg functions on the same attr, can use a std::vector<std::string> to solve
	std::unordered_map<unsigned int, std::string> agg_field;
	std::vector<std::string> filter_field;
	std::vector<std::string> join_field;
	std::vector<unique_ptr<SimplestAttr>> group_by_vec;
	std::vector<std::string> group_by_field;
	std::vector<std::string> order_by_field;
	std::string limit_field;

	std::unordered_map<unsigned int, std::string> table_names;
	std::unordered_map<unsigned int, std::vector<std::string>> chunk_contents;

	// mapping from table_name -> actual column names in created table
	std::unordered_map<std::pair<idx_t, idx_t>, std::string, pair_hash> table_column_mappings;

	std::unordered_map<std::pair<idx_t, idx_t>, idx_t, pair_hash> proj_table_to_real_table;
};
} // namespace duckdb