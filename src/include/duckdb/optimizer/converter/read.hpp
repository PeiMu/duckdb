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
#include "simplest_ir.h"

#include <queue>

namespace duckdb {
/* And a few guys need only the PG_strtok support fields */
#define READ_TEMP_LOCALS()                                                                                             \
	const char *token;                                                                                                 \
	int length

#define nullable_string(token, length) ((length) == 0 ? NULL : DeBackslash(token, length))

#define strtobool(x) ((*(x) == 't') ? true : false)

using table_str = std::unordered_map<std::string, std::vector<unique_ptr<SimplestLiteral>>>;
using Oid = unsigned int;

class PlanReader {
public:
	PlanReader() = default;
	~PlanReader() = default;

	const char *PG_strtok(int *length);

	unique_ptr<SimplestNode> NodeRead(const char *token, int tok_len, bool return_vector = false,
	                                  std::vector<unique_ptr<SimplestNode>> *node_vec = nullptr);

	unique_ptr<SimplestNode> StringToNode(const char *str);

	char *DeBackslash(const char *token, int length);

	std::deque<table_str> table_col_names;

private:
	unique_ptr<SimplestNode> StringToNodeInternal(const char *str, bool restore_loc_fields);
	int StrToInt(const char *str, char **endptr, int base);
	duckdb_libpgquery::PGNodeTag NodeTokenType(const char *token, int length);

	unique_ptr<SimplestNode> ParseNodeString();

	// read postgres nodes
	void *ReadBitmapset();
	void *ReadAttrNumberCols(int numCols);
	void *ReadOidCols(int numCols);
	unique_ptr<SimplestStmt> ReadCommonPlan();
	unique_ptr<SimplestAggregate> ReadAgg();
	unique_ptr<SimplestAttr> ReadAggref();
	unique_ptr<SimplestNode> ReadTargetEntry();
	unique_ptr<SimplestParam> ReadParam();
	unique_ptr<SimplestAttr> ReadVar();
	unique_ptr<SimplestAttr> ReadRelabelType();
	unique_ptr<SimplestConstVar> ReadConst();
	void ReadGather();
	unique_ptr<SimplestJoin> ReadCommonJoin();
	unique_ptr<SimplestHash> ReadHash();
	unique_ptr<SimplestJoin> ReadHashJoin();
	unique_ptr<SimplestJoin> ReadNestLoop();
	unique_ptr<SimplestVarComparison> ReadNestLoopParam();
	unique_ptr<SimplestScan> ReadCommonScan();
	unique_ptr<SimplestScan> ReadSeqScan();
	unique_ptr<SimplestScan> ReadBitmapHeapScan();
	unique_ptr<SimplestNode> ReadBitmapIndexScan();
	unique_ptr<SimplestScan> ReadIndexScan();
	unique_ptr<SimplestScan> ReadIndexOnlyScan();
	unique_ptr<SimplestComparisonExpr> ReadOpExpr();
	void ReadBoolExpr();
	unique_ptr<SimplestComparisonExpr> ReadScalarArrayOpExpr();
	unique_ptr<SimplestStmt> ReadPlannedStmt();
	void ReadRangeTblEntry();
	void ReadAlias();
	void ReadNullTest();

	PGDatum ReadDatum(bool typbyval, unsigned int &datum_len);
	SimplestVarType GetSimplestVarType(unsigned int type_id);
	SimplestJoinType GetSimplestJoinType(unsigned int type_id);
	SimplestComparisonType GetSimplestComparisonType(unsigned int type_id);

	std::vector<unique_ptr<SimplestVarParamComparison>> index_conditions;
};
} // namespace duckdb