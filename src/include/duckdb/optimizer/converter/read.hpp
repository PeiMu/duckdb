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

namespace duckdb {
class PlanReader {
public:
	PlanReader() = default;
	~PlanReader() = default;

	static const char *pg_strtok(int *length);

	static void *nodeRead(const char *token, int tok_len);

	void *stringToNode(const char *str);

	static char *debackslash(const char *token, int length);

private:
	void *stringToNodeInternal(const char *str, bool restore_loc_fields);
	static int strtoint(const char *str, char **endptr, int base);
	static duckdb_libpgquery::PGNodeTag nodeTokenType(const char *token, int length);

	typedef unsigned int Oid;

	friend class PlanReadFuncs;
};
} // namespace duckdb