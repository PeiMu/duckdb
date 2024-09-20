//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/read.hpp
// from Postgres source code: src/backend/nodes/read.c
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <queue>
#include "duckdb/common/printer.hpp"
#include "nodes/pg_list.hpp"
#include "nodes/value.hpp"
#include "pg_functions.hpp"

#include "simplest_ir.h"


namespace duckdb {
/* And a few guys need only the PG_strtok support fields */
#define READ_TEMP_LOCALS()	\
	const char *token;		\
	int			length

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

	std::queue<table_str> table_col_names;

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
	unique_ptr<SimplestAttr> ReadVar();
	void ReadGather();
	unique_ptr<SimplestJoin> ReadCommonJoin();
	unique_ptr<SimplestHash> ReadHash();
	unique_ptr<SimplestJoin> ReadHashJoin();
	unique_ptr<SimplestScan> ReadCommonScan();
	unique_ptr<SimplestScan> ReadSeqScan();
	unique_ptr<SimplestComparisonExpr> ReadOpExpr();
	unique_ptr<SimplestConstVar> ReadConst();
	unique_ptr<SimplestStmt> ReadPlannedStmt();
	void ReadRangeTblEntry();
	void ReadAlias();

	PGDatum ReadDatum(bool typbyval);
	SimplestVarType GetSimplestVarType(unsigned int type_id);
	SimplestJoinType GetSimplestJoinType(unsigned int type_id);
	SimplestComparisonType GetSimplestComparisonType(unsigned int type_id);
};
} // namespace duckdb