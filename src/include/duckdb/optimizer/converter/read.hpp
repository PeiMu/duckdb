//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/read.hpp
// from Postgres source code: src/backend/nodes/read.c
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/printer.hpp"
#include "nodes/pg_list.hpp"
#include "nodes/value.hpp"
#include "pg_functions.hpp"

#include "readfuncs.hpp"
#include "simplest_ir.h"

namespace duckdb {
class PlanReader {
public:
	PlanReader() = default;
	~PlanReader() = default;

	static const char *pg_strtok(int *length);

	static unique_ptr<SimplestNode> nodeRead(const char *token, int tok_len, bool return_vector = false,
	                                         std::vector<unique_ptr<SimplestNode>> *node_vec = nullptr);

	unique_ptr<SimplestNode> stringToNode(const char *str);

	static char *debackslash(const char *token, int length);

private:
	unique_ptr<SimplestNode> stringToNodeInternal(const char *str, bool restore_loc_fields);
	static int strtoint(const char *str, char **endptr, int base);
	static duckdb_libpgquery::PGNodeTag nodeTokenType(const char *token, int length);

	typedef unsigned int Oid;
};
} // namespace duckdb