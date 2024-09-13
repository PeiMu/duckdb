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
#include "duckdb/planner/logical_operator.hpp"
#include "read.hpp"
#include "simplest_ir.h"

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
	static unique_ptr<SimplestNode> parseNodeString();

	// todo: Do we really need static? Need to refactor code
	static void *ReadBitmapset();
	static void *ReadAttrNumberCols(int numCols);
	static void *ReadOidCols(int numCols);
	static unique_ptr<SimplestStmt> ReadCommonPlan();
	static unique_ptr<SimplestAggregate> ReadAgg();
	static unique_ptr<SimplestAttr> ReadAggref();
	static unique_ptr<SimplestNode> ReadTargetEntry();
	static unique_ptr<SimplestAttr> ReadVar();
	static void ReadGather();
	static unique_ptr<SimplestJoin> ReadCommonJoin();
	static unique_ptr<SimplestHash> ReadHash();
	static unique_ptr<SimplestJoin> ReadHashJoin();
	static unique_ptr<SimplestScan> ReadCommonScan();
	static unique_ptr<SimplestScan> ReadSeqScan();
	static unique_ptr<SimplestComparisonExpr> ReadOpExpr();
	static PGDatum ReadDatum(bool typbyval);
	static unique_ptr<SimplestConstVar> ReadConst();

	static SimplestVarType GetSimplestVarType(unsigned int type_id);
	static SimplestJoinType GetSimplestJoinType(unsigned int type_id);
	static SimplestComparisonType GetSimplestComparisonType(unsigned int type_id);

	friend class PlanReader;
};
}