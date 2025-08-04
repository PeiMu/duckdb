//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duckdb_to_ir.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "simplest_ir.h"

#define CONVERT_IR_TO_SQL true

namespace duckdb {
class IRToSQLConverter {
public:
	IRToSQLConverter() {};
	~IRToSQLConverter() = default;

	std::string LogicalPlanToSQL(const unique_ptr<SimplestStmt> &plan);

private:
	void GenerateSQL(const unique_ptr<SimplestStmt> &op);

	std::vector<std::string> select_field;
	std::vector<std::string> filter_field;
	std::vector<std::string> join_field;
	std::vector<std::string> group_by;
	std::vector<std::string> order_by;

	std::unordered_map<unsigned int, std::string> table_names;
	std::unordered_map<unsigned int, std::vector<std::string>> chunk_contents;
};
} // namespace duckdb