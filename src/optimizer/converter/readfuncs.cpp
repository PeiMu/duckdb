// from Postgres source code: src/backend/nodes/readfuncs.c

#include "duckdb/optimizer/converter/readfuncs.hpp"

namespace duckdb {

using namespace duckdb_libpgquery;

/* And a few guys need only the PlanReader::pg_strtok support fields */
#define READ_TEMP_LOCALS()	\
	const char *token;		\
	int			length

#define nullable_string(token,length)  \
	((length) == 0 ? NULL : PlanReader::debackslash(token, length))

#define atooid(x) ((PGOid) strtoul((x), NULL, 10))

#define strtobool(x)  ((*(x) == 't') ? true : false)

PGBitmapset *_readBitmapset(void) {
	PGBitmapset  *result = NULL;

	READ_TEMP_LOCALS();

	token = PlanReader::pg_strtok(&length);
	if (token == NULL)
		elog(ERROR, "incomplete Bitmapset structure");
	if (length != 1 || token[0] != '(')
		elog(ERROR, "unrecognized token: \"%.*s\"", length, token);

	token = PlanReader::pg_strtok(&length);
	if (token == NULL)
		elog(ERROR, "incomplete Bitmapset structure");
	if (length != 1 || token[0] != 'b')
		elog(ERROR, "unrecognized token: \"%.*s\"", length, token);

	for (;;)
	{
		int			val;
		char	   *endptr;

		token = PlanReader::pg_strtok(&length);
		if (token == NULL)
			elog(ERROR, "unterminated Bitmapset structure");
		if (length == 1 && token[0] == ')')
			break;
		val = (int) strtol(token, &endptr, 10);
		if (endptr != token + length)
			elog(ERROR, "unrecognized integer: \"%.*s\"", length, token);
//		result = bms_add_member(result, val);
	}

	return result;
}

/*
 * readAttrNumberCols
 */
PGAttrNumber *readAttrNumberCols(int numCols)
{
	int tokenLength, i;
	const char *token;
	PGAttrNumber *attr_vals;

	if (numCols <= 0)
		return NULL;

//	attr_vals = (PGAttrNumber *) malloc(numCols * sizeof(PGAttrNumber));
	for (i = 0; i < numCols; i++)
	{
		token = PlanReader::pg_strtok(&tokenLength);
//		attr_vals[i] = atoi(token);
	}

	return attr_vals;
}

/*
 * readOidCols
 */
PGOid *readOidCols(int numCols) {
	int tokenLength, i;
	const char *token;
	PGOid *oid_vals;

	if (numCols <= 0)
		return NULL;

//	oid_vals = (PGOid *) malloc(numCols * sizeof(PGOid));
	for (i = 0; i < numCols; i++)
	{
		token = PlanReader::pg_strtok(&tokenLength);
//		oid_vals[i] = atooid(token);
	}

	return oid_vals;
}

void ReadCommonPlan() {
	READ_TEMP_LOCALS();

	// startup_cost
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// total_cost
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// plan_rows
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// plan_width
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// parallel_awre
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// parallel_safe
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// plan_node_id
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// targetlist
	token = PlanReader::pg_strtok(&length);
	(void) token;
	PlanReader::nodeRead(NULL, 0);
	// qual
	token = PlanReader::pg_strtok(&length);
	(void) token;
	PlanReader::nodeRead(NULL, 0);
	// lefttree
	token = PlanReader::pg_strtok(&length);
	(void) token;
	PlanReader::nodeRead(NULL, 0);
	// righttree
	token = PlanReader::pg_strtok(&length);
	(void) token;
	PlanReader::nodeRead(NULL, 0);
	// initPlan
	token = PlanReader::pg_strtok(&length);
	(void) token;
	PlanReader::nodeRead(NULL, 0);
	// extParam
	token = PlanReader::pg_strtok(&length);
	(void) token;
	_readBitmapset();
	// allParam
	token = PlanReader::pg_strtok(&length);
	(void) token;
	_readBitmapset();
}

void _readAgg(void) {
	READ_TEMP_LOCALS();

	ReadCommonPlan();

	// AggStrategy
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// AggSplit
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// numCols
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	int numCols = atoi(token);
	// grpColIdx
	token = PlanReader::pg_strtok(&length);
	readAttrNumberCols(numCols);
	// grpOperators
	token = PlanReader::pg_strtok(&length);
	readOidCols(numCols);
	// grpCollations
	token = PlanReader::pg_strtok(&length);
	readOidCols(numCols);
	// numGroups
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// aggParams
	token = PlanReader::pg_strtok(&length);
	(void) token;
	_readBitmapset();
	// groupingSets
	token = PlanReader::pg_strtok(&length);
	(void) token;
	PlanReader::nodeRead(NULL, 0);
	// chain
	token = PlanReader::pg_strtok(&length);
	(void) token;
	PlanReader::nodeRead(NULL, 0);
}

void _readAggref(void)
{
	READ_TEMP_LOCALS();

	// aggfnoid
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// aggtype
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// aggcollid
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// inputcollid
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// aggtranstype
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// aggargtypes
	token = PlanReader::pg_strtok(&length);
	(void) token;
	PlanReader::nodeRead(NULL, 0);
	// aggdirectargs
	token = PlanReader::pg_strtok(&length);
	(void) token;
	PlanReader::nodeRead(NULL, 0);
	// args
	token = PlanReader::pg_strtok(&length);
	(void) token;
	PlanReader::nodeRead(NULL, 0);
	// aggorder
	token = PlanReader::pg_strtok(&length);
	(void) token;
	PlanReader::nodeRead(NULL, 0);
	// aggdistinct
	token = PlanReader::pg_strtok(&length);
	(void) token;
	PlanReader::nodeRead(NULL, 0);
	// aggfilter
	token = PlanReader::pg_strtok(&length);
	(void) token;
	PlanReader::nodeRead(NULL, 0);
	// aggstar
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// aggvariadic
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// aggkind
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	auto agg_kind = (length == 0) ? '\0' : (token[0] == '\\' ? token[1] : token[0]);
	// agglevelsup
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// aggsplit
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// location
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	(void) token;
}

/*
 * _readTargetEntry
 */
void _readTargetEntry(void)
{
	READ_TEMP_LOCALS();

	// expr
	token = PlanReader::pg_strtok(&length);
	(void) token;
	PlanReader::nodeRead(NULL, 0);
	// resno
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// resname
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	nullable_string(token, length);
	// ressortgroupref
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// resorigtbl
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// resorigcol
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// resjunk
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
}

void _readVar(void) {
    READ_TEMP_LOCALS();

	// varno
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// varattno
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// vartype
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// vartypmod
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// varcollid
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// varlevelsup
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// varnoold
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// varoattno
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// location
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
}

void _readGather(void) {
    READ_TEMP_LOCALS();

	ReadCommonPlan();

	// num_workers
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// rescan_param
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// single_copy
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// invisible
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// initParam
	token = PlanReader::pg_strtok(&length);
	(void) token;
	_readBitmapset();
}

void ReadCommonJoin() {
	READ_TEMP_LOCALS();

	ReadCommonPlan();

	// jointype
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// inner_unique
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// joinqual
	token = PlanReader::pg_strtok(&length);
	(void) token;
	PlanReader::nodeRead(NULL, 0);
}

void _readHash(void) {
	READ_TEMP_LOCALS();

	ReadCommonPlan();

	// hashkeys
	token = PlanReader::pg_strtok(&length);
	(void) token;
	PlanReader::nodeRead(NULL, 0);
	// skewTable
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// skewColumn
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// skewInherit
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// rows_total
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
}

void _readHashJoin(void) {
    READ_TEMP_LOCALS();

	ReadCommonJoin();

	// hashclauses
	token = PlanReader::pg_strtok(&length);
	(void) token;
	PlanReader::nodeRead(NULL, 0);
	// hashoperators
	token = PlanReader::pg_strtok(&length);
	(void) token;
	PlanReader::nodeRead(NULL, 0);
	// hashcollations
	token = PlanReader::pg_strtok(&length);
	(void) token;
	PlanReader::nodeRead(NULL, 0);
	// hashkeys
	token = PlanReader::pg_strtok(&length);
	(void) token;
	PlanReader::nodeRead(NULL, 0);
}

void ReadCommonScan() {
	READ_TEMP_LOCALS();

	ReadCommonPlan();

	// scanrelid
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
}

void _readSeqScan(void) {
    ReadCommonScan();
}

void _readOpExpr(void) {
    READ_TEMP_LOCALS();

	// opno
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// opfuncid
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// opresulttype
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// opretset
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// opcollid
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// inputcollid
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// args
	token = PlanReader::pg_strtok(&length);
	(void) token;
	PlanReader::nodeRead(NULL, 0);
	// location
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
}

/*
 * readDatum
 *
 * Given a string representation of a constant, recreate the appropriate
 * Datum.  The string representation embeds length info, but not byValue,
 * so we must be told that.
 */
PGDatum readDatum(bool typbyval) {
	size_t length, i;
	int			tokenLength;
	const char *token;
	PGDatum		res;
	char	   *s;

	/*
	 * read the actual length of the value
	 */
	token = PlanReader::pg_strtok(&tokenLength);
	length = atoi(token);

	token = PlanReader::pg_strtok(&tokenLength);	/* read the '[' */
	if (token == NULL || token[0] != '[')
		elog(ERROR, "expected \"[\" to start datum, but got \"%s\"; length = %zu",
		     token ? token : "[NULL]", length);

	if (typbyval)
	{
		if (length > (size_t) sizeof(PGDatum))
			elog(ERROR, "byval datum but length = %zu", length);
		res = (PGDatum) 0;
		s = (char *) (&res);
		for (i = 0; i < (size_t) sizeof(PGDatum); i++)
		{
			token = PlanReader::pg_strtok(&tokenLength);
			s[i] = (char) atoi(token);
		}
	}
	else if (length <= 0)
		res = (PGDatum) NULL;
	else
	{
		s = (char *) malloc(length);
		for (i = 0; i < length; i++)
		{
			token = PlanReader::pg_strtok(&tokenLength);
			s[i] = (char) atoi(token);
		}
		res = (PGDatum)(s);
	}

	token = PlanReader::pg_strtok(&tokenLength);	/* read the ']' */
	if (token == NULL || token[0] != ']')
		elog(ERROR, "expected \"]\" to end datum, but got \"%s\"; length = %zu",
		     token ? token : "[NULL]", length);

	return res;
}

void _readConst(void) {
    READ_TEMP_LOCALS();

	// consttype
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// consttypmod
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// constcollid
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// constlen
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// constbyval
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	auto const_by_val = strtobool(token);
	// constisnull
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	auto const_is_null = strtobool(token);
	// location
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);

	token = PlanReader::pg_strtok(&length);
	if (const_is_null)
		token = PlanReader::pg_strtok(&length); /* skip "<>" */
	else
		readDatum(const_by_val);
}

duckdb_libpgquery::PGNode *PlanReadFuncs::parseNodeString(void) {
	void	   *return_value;

	READ_TEMP_LOCALS();

	/* Guard against stack overflow due to overly complex expressions */
	// check_stack_depth();

	token = PlanReader::pg_strtok(&length);

#define MATCH(tokname, namelen) \
	(length == namelen && memcmp(token, tokname, namelen) == 0)

	// debug
	if (MATCH("AGG", 3))
		_readAgg();
	else if (MATCH("AGGREF", 6))
		_readAggref();
	else if (MATCH("TARGETENTRY", 11))
		_readTargetEntry();
	else if (MATCH("VAR", 3))
		_readVar();
	else if (MATCH("GATHER", 6))
		_readGather();
	else if (MATCH("HASH", 4))
		_readHash();
	else if (MATCH("HASHJOIN", 8))
		_readHashJoin();
	else if (MATCH("SEQSCAN", 7))
		_readSeqScan();
	else if (MATCH("OPEXPR", 6))
		_readOpExpr();
	else if (MATCH("CONST", 5))
		_readConst();

	return (PGNode *) return_value;

}
}
