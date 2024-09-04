//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/readfuncs.hpp
// from Postgres source code: src/backend/nodes/readfuncs.c
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/printer.hpp"
#include "nodes/parsenodes.hpp"
#include "read.hpp"

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
/* And a few guys need only the pg_strtok support fields */
#define READ_TEMP_LOCALS()	\
	const char *token;		\
	int			length

class PlanReadFuncs {
public:
	PlanReadFuncs() = default;
	~PlanReadFuncs() = default;

private:
	static duckdb_libpgquery::PGNode *parseNodeString(void);

	friend class PlanReader;
};
}