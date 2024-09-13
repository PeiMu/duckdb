// from Postgres source code: src/backend/nodes/readfuncs.c

#include "duckdb/optimizer/converter/readfuncs.hpp"

namespace duckdb {

/* And a few guys need only the PlanReader::pg_strtok support fields */
#define READ_TEMP_LOCALS()                                                                                             \
	const char *token;                                                                                                 \
	int length

#define nullable_string(token, length) ((length) == 0 ? NULL : PlanReader::debackslash(token, length))

#define atooid(x) ((PGOid)strtoul((x), NULL, 10))

#define strtobool(x) ((*(x) == 't') ? true : false)

void *PlanReadFuncs::ReadBitmapset() {
	READ_TEMP_LOCALS();

	token = PlanReader::pg_strtok(&length);
	if (token == NULL)
		Printer::Print("Error! incomplete Bitmapset structure");
	if (length != 1 || token[0] != '(')
		Printer::Print("Error! unrecognized token: " + std::to_string(length) + token);

	token = PlanReader::pg_strtok(&length);
	if (token == NULL)
		Printer::Print("Error! incomplete Bitmapset structure");
	if (length != 1 || token[0] != 'b')
		Printer::Print("Error! unrecognized token: " + std::to_string(length) + token);

	for (;;) {
		int val;
		char *endptr;

		token = PlanReader::pg_strtok(&length);
		if (token == NULL)
			Printer::Print("Error! unterminated Bitmapset structure");
		if (length == 1 && token[0] == ')')
			break;
		val = (int)strtol(token, &endptr, 10);
		if (endptr != token + length)
			Printer::Print("Error! unrecognized integer: " + std::to_string(length) + token);
		//		result = bms_add_member(result, val);
	}

	return nullptr;
}

/*
 * ReadAttrNumberCols
 */
void *PlanReadFuncs::ReadAttrNumberCols(int numCols) {
	int tokenLength, i;
	const char *token;

	if (numCols <= 0)
		return NULL;

	//	attr_vals = (PGAttrNumber *) malloc(numCols * sizeof(PGAttrNumber));
	for (i = 0; i < numCols; i++) {
		token = PlanReader::pg_strtok(&tokenLength);
		//		attr_vals[i] = atoi(token);
	}

	return nullptr;
}

/*
 * ReadOidCols
 */
void *PlanReadFuncs::ReadOidCols(int numCols) {
	int tokenLength, i;
	const char *token;

	if (numCols <= 0)
		return NULL;

	//	oid_vals = (PGOid *) malloc(numCols * sizeof(PGOid));
	for (i = 0; i < numCols; i++) {
		token = PlanReader::pg_strtok(&tokenLength);
		//		oid_vals[i] = atooid(token);
	}

	return nullptr;
}

unique_ptr<SimplestStmt> PlanReadFuncs::ReadCommonPlan() {
	READ_TEMP_LOCALS();

	std::vector<unique_ptr<SimplestStmt>> children;

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
	(void)token;
	// this should return a vector of vars
	// use a member variable - target_list_vec
	//     nodeRead(NULL, 0, is_a_list)
	std::vector<unique_ptr<SimplestNode>> node_vec;
	std::vector<unique_ptr<SimplestAttr>> attr_vec;
	PlanReader::nodeRead(NULL, 0, true, &node_vec);
	for (auto &node : node_vec) {
		if (node)
			attr_vec.emplace_back(unique_ptr_cast<SimplestNode, SimplestAttr>(std::move(node)));
	}
	// qual
	// e.g. `qual` should be an expr, if we have more than one `qual`,
	// we need to change the `nodeRead()` to `std::vector<unique_ptr<SimplestNode>>`
	token = PlanReader::pg_strtok(&length);
	(void)token;
	node_vec.clear();
	std::vector<unique_ptr<SimplestVarConstComparison>> qual_vec;
	auto qual_node = PlanReader::nodeRead(NULL, 0, true, &node_vec);
	for (auto &node : node_vec) {
		if (node)
			qual_vec.emplace_back(unique_ptr_cast<SimplestNode, SimplestVarConstComparison>(std::move(node)));
	}
	// lefttree
	token = PlanReader::pg_strtok(&length);
	(void)token;
	unique_ptr<SimplestStmt> left_tree_stmt =
	    unique_ptr_cast<SimplestNode, SimplestStmt>(PlanReader::nodeRead(NULL, 0));
	children.emplace_back(std::move(left_tree_stmt));
	// righttree
	token = PlanReader::pg_strtok(&length);
	(void)token;
	unique_ptr<SimplestStmt> right_tree_stmt =
	    unique_ptr_cast<SimplestNode, SimplestStmt>(PlanReader::nodeRead(NULL, 0));
	children.emplace_back(std::move(right_tree_stmt));
	// initPlan
	token = PlanReader::pg_strtok(&length);
	(void)token;
	PlanReader::nodeRead(NULL, 0);
	// extParam
	token = PlanReader::pg_strtok(&length);
	(void)token;
	ReadBitmapset();
	// allParam
	token = PlanReader::pg_strtok(&length);
	(void)token;
	ReadBitmapset();

	if (children.empty())
		return make_uniq<SimplestStmt>(std::move(attr_vec), std::move(qual_vec));
	else
		return make_uniq<SimplestStmt>(std::move(children), std::move(attr_vec), std::move(qual_vec));
}

unique_ptr<SimplestAggregate> PlanReadFuncs::ReadAgg() {
	READ_TEMP_LOCALS();

	unique_ptr<SimplestStmt> common_stmt = ReadCommonPlan();
	std::vector<SimplestVarType> agg_type_vec;
	for (const auto &target : common_stmt->target_list) {
		// todo: need to check the order
		agg_type_vec.emplace_back(target->GetType());
	}
	unique_ptr<SimplestAggregate> agg_stmt = make_uniq<SimplestAggregate>(
	    std::move(common_stmt->children), std::move(common_stmt->target_list), agg_type_vec);

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
	ReadAttrNumberCols(numCols);
	// grpOperators
	token = PlanReader::pg_strtok(&length);
	ReadOidCols(numCols);
	// grpCollations
	token = PlanReader::pg_strtok(&length);
	ReadOidCols(numCols);
	// numGroups
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// aggParams
	token = PlanReader::pg_strtok(&length);
	(void)token;
	ReadBitmapset();
	// groupingSets
	token = PlanReader::pg_strtok(&length);
	(void)token;
	PlanReader::nodeRead(NULL, 0);
	// chain
	token = PlanReader::pg_strtok(&length);
	(void)token;
	PlanReader::nodeRead(NULL, 0);

	return agg_stmt;
}

// It's a confusing design in postgres' node string,
// we are not sure if AGGREF should be a stmt or attr.
// assume it's an attr, and we get the `aggtype` from its var's `vartype`
unique_ptr<SimplestAttr> PlanReadFuncs::ReadAggref() {
	READ_TEMP_LOCALS();

	// aggfnoid
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// aggtype
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	unsigned int agg_type_id = atoi(token);
	SimplestVarType agg_type = GetSimplestVarType(agg_type_id);
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
	(void)token;
	PlanReader::nodeRead(NULL, 0);
	// aggdirectargs
	token = PlanReader::pg_strtok(&length);
	(void)token;
	PlanReader::nodeRead(NULL, 0);
	// args
	token = PlanReader::pg_strtok(&length);
	(void)token;
	auto args_node = PlanReader::nodeRead(NULL, 0);
	unique_ptr<SimplestAttr> aggr_attr = unique_ptr_cast<SimplestNode, SimplestAttr>(std::move(args_node));
	// aggorder
	token = PlanReader::pg_strtok(&length);
	(void)token;
	PlanReader::nodeRead(NULL, 0);
	// aggdistinct
	token = PlanReader::pg_strtok(&length);
	(void)token;
	PlanReader::nodeRead(NULL, 0);
	// aggfilter
	token = PlanReader::pg_strtok(&length);
	(void)token;
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
	(void)token;

	return aggr_attr;
}

/*
 * ReadTargetEntry
 */
unique_ptr<SimplestNode> PlanReadFuncs::ReadTargetEntry(void) {
	READ_TEMP_LOCALS();

	unique_ptr<SimplestNode> node;

	// expr
	// check if TARGETENTRY has only one expr
	token = PlanReader::pg_strtok(&length);
	(void)token;
	node = PlanReader::nodeRead(NULL, 0);
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

	return node;
}

SimplestVarType PlanReadFuncs::GetSimplestVarType(unsigned int type_id) {
	SimplestVarType simplest_var_type = InvalidVarType;
	switch (type_id) {
	case 20:
	case 21:
	case 23:
		simplest_var_type = Int;
		break;
	case 25:
		simplest_var_type = String;
		break;
	case 700:
	case 701:
		simplest_var_type = Float;
		break;
	default:
		Printer::Print("Doesn't support such type yet");
		D_ASSERT(false);
	}

	return simplest_var_type;
}

SimplestJoinType PlanReadFuncs::GetSimplestJoinType(unsigned int type_id) {
	SimplestJoinType simplest_join_type = InvalidJoinType;
	switch (type_id) {
	case 0:
		simplest_join_type = Inner;
		break;
	case 1:
		simplest_join_type = Left;
		break;
	case 2:
		simplest_join_type = Full;
		break;
	case 3:
		simplest_join_type = Right;
		break;
	case 4:
		simplest_join_type = Semi;
		break;
	case 5:
		simplest_join_type = Anti;
		break;
	case 6:
		simplest_join_type = UniqueOuter;
		break;
	case 7:
		simplest_join_type = UniqueInner;
		break;
	default:
		Printer::Print("Doesn't support such type yet");
		D_ASSERT(false);
	}

	return simplest_join_type;
}

SimplestComparisonType PlanReadFuncs::GetSimplestComparisonType(unsigned int type_id) {
	SimplestComparisonType simplest_comprison_type = InvalidComparisonType;
	// from postgres - src/include/catalog/pg_operator.dat
	switch (type_id) {
	case 15:
	case 91:
	case 92:
	case 93:
	case 94:
	case 96:
	case 98:
		simplest_comprison_type = Equal;
		break;
	case 85:
		simplest_comprison_type = Not;
		break;
	case 97:
	case 412:
	case 2799:
	case 672:
		simplest_comprison_type = LessThan;
		break;
	case 521:
		simplest_comprison_type = GreaterThan;
		break;
	default:
		Printer::Print("Doesn't support such type yet");
		D_ASSERT(false);
	}

	return simplest_comprison_type;
}

unique_ptr<SimplestAttr> PlanReadFuncs::ReadVar() {
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
	unsigned int var_type_id = atoi(token);
	SimplestVarType var_type = GetSimplestVarType(var_type_id);
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
	unsigned int table_index = atoi(token);
	// varoattno
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	unsigned int column_index = atoi(token);
	// location
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	return make_uniq<SimplestAttr>(var_type, table_index, column_index, "");
}

void PlanReadFuncs::ReadGather() {
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
	(void)token;
	ReadBitmapset();
}

unique_ptr<SimplestJoin> PlanReadFuncs::ReadCommonJoin() {
	READ_TEMP_LOCALS();

	unique_ptr<SimplestStmt> common_stmt = ReadCommonPlan();

	// jointype
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	SimplestJoinType join_type = GetSimplestJoinType(atoi(token));
	// inner_unique
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// joinqual
	token = PlanReader::pg_strtok(&length);
	(void)token;
	PlanReader::nodeRead(NULL, 0);

	unique_ptr<SimplestJoin> common_join = make_uniq<SimplestJoin>(std::move(common_stmt->children), join_type);

	return common_join;
}

unique_ptr<SimplestHash> PlanReadFuncs::ReadHash() {
	READ_TEMP_LOCALS();

	unique_ptr<SimplestStmt> common_plan = ReadCommonPlan();

	// hashkeys
	token = PlanReader::pg_strtok(&length);
	(void)token;
	std::vector<unique_ptr<SimplestNode>> node_vec;
	std::vector<unique_ptr<SimplestAttr>> attr_vec;
	PlanReader::nodeRead(NULL, 0, true, &node_vec);
	for (auto &node : node_vec) {
		if (node)
			attr_vec.emplace_back(unique_ptr_cast<SimplestNode, SimplestAttr>(std::move(node)));
	}
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

	unique_ptr<SimplestHash> hash_stmt = make_uniq<SimplestHash>(
	    std::move(common_plan->children), std::move(common_plan->target_list), std::move(attr_vec));

	return hash_stmt;
}

unique_ptr<SimplestJoin> PlanReadFuncs::ReadHashJoin() {
	READ_TEMP_LOCALS();

	unique_ptr<SimplestJoin> common_join = ReadCommonJoin();

	// hashclauses
	// get join condition
	token = PlanReader::pg_strtok(&length);
	(void)token;
	// this should get a vector of exprs
	std::vector<unique_ptr<SimplestNode>> node_vec;
	// todo: need to check if it's always SimplestVarComparison
	std::vector<unique_ptr<SimplestVarComparison>> join_conditions;
	auto hash_clauses_node = PlanReader::nodeRead(NULL, 0, true, &node_vec);
	for (auto &node : node_vec) {
		if (node)
			join_conditions.emplace_back(unique_ptr_cast<SimplestNode, SimplestVarComparison>(std::move(node)));
	}
	common_join->AddJoinCondition(std::move(join_conditions));
	// hashoperators
	token = PlanReader::pg_strtok(&length);
	(void)token;
	PlanReader::nodeRead(NULL, 0);
	// hashcollations
	token = PlanReader::pg_strtok(&length);
	(void)token;
	PlanReader::nodeRead(NULL, 0);
	// hashkeys
	token = PlanReader::pg_strtok(&length);
	(void)token;
	PlanReader::nodeRead(NULL, 0);

	return common_join;
}

unique_ptr<SimplestScan> PlanReadFuncs::ReadCommonScan() {
	READ_TEMP_LOCALS();

	unique_ptr<SimplestStmt> common_plan = ReadCommonPlan();
	// todo: add table name
	unique_ptr<SimplestScan> common_scan;
	if (common_plan->qual_vec.empty())
		common_scan = make_uniq<SimplestScan>("", std::move(common_plan->target_list));
	else
		common_scan =
		    make_uniq<SimplestScan>("", std::move(common_plan->target_list), std::move(common_plan->qual_vec));

	// scanrelid
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);

	return common_scan;
}

unique_ptr<SimplestScan> PlanReadFuncs::ReadSeqScan() {
	return ReadCommonScan();
}

unique_ptr<SimplestComparisonExpr> PlanReadFuncs::ReadOpExpr() {
	READ_TEMP_LOCALS();

	// opno
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	SimplestComparisonType op_type = GetSimplestComparisonType(atoi(token));

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
	(void)token;
	std::vector<unique_ptr<SimplestNode>> node_vec;
	std::vector<unique_ptr<SimplestVar>> var_vec;
	PlanReader::nodeRead(NULL, 0, true, &node_vec);
	D_ASSERT(node_vec.size() == 2);
	for (auto &node : node_vec) {
		if (node)
			var_vec.emplace_back(unique_ptr_cast<SimplestNode, SimplestVar>(std::move(node)));
	}
	// todo: need to confirm if the var_vec[0] is always a variable
	D_ASSERT(!var_vec[0]->IsConst());
	// location
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);

	if (var_vec[1]->IsConst()) {
		return make_uniq<SimplestVarConstComparison>(
		    op_type, unique_ptr_cast<SimplestVar, SimplestAttr>(std::move(var_vec[0])),
		    unique_ptr_cast<SimplestVar, SimplestConstVar>(std::move(var_vec[1])));
	} else {
		return make_uniq<SimplestVarComparison>(op_type,
		                                        unique_ptr_cast<SimplestVar, SimplestAttr>(std::move(var_vec[0])),
		                                        unique_ptr_cast<SimplestVar, SimplestAttr>(std::move(var_vec[1])));
	}
}

/*
 * ReadDatum
 *
 * Given a string representation of a constant, recreate the appropriate
 * Datum.  The string representation embeds length info, but not byValue,
 * so we must be told that.
 */
PGDatum PlanReadFuncs::ReadDatum(bool typbyval) {
	size_t length, i;
	int tokenLength;
	const char *token;
	PGDatum res;
	char *s;

	/*
	 * read the actual length of the value
	 */
	token = PlanReader::pg_strtok(&tokenLength);
	length = atoi(token);

	token = PlanReader::pg_strtok(&tokenLength); /* read the '[' */
	if (token == NULL || token[0] != '[') {
		std::string token_str = token ? token : "[NULL]";
		Printer::Print("Error! expected \"[\" to start datum, but got " + token_str +
		               "; length = " + std::to_string(length));
	}

	if (typbyval) {
		if (length > (size_t)sizeof(PGDatum))
			Printer::Print("Error! byval datum but length = " + std::to_string(length));
		res = (PGDatum)0;
		s = (char *)(&res);
		for (i = 0; i < (size_t)sizeof(PGDatum); i++) {
			token = PlanReader::pg_strtok(&tokenLength);
			s[i] = (char)atoi(token);
		}
	} else if (length <= 0)
		res = (PGDatum)NULL;
	else {
		s = (char *)malloc(length);
		for (i = 0; i < length; i++) {
			token = PlanReader::pg_strtok(&tokenLength);
			s[i] = (char)atoi(token);
		}
		res = (PGDatum)(s);
	}

	token = PlanReader::pg_strtok(&tokenLength); /* read the ']' */
	if (token == NULL || token[0] != ']') {
		std::string token_str = token ? token : "[NULL]";
		Printer::Print("Error! expected \"]\" to end datum, but got " + token_str +
		               "; length = " + std::to_string(length));
	}

	return res;
}

unique_ptr<SimplestConstVar> PlanReadFuncs::ReadConst() {
	READ_TEMP_LOCALS();

	// consttype
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	unsigned int const_type_id = atoi(token);
	SimplestVarType const_type = GetSimplestVarType(const_type_id);
	// consttypmod
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// constcollid
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	// constlen
	token = PlanReader::pg_strtok(&length);
	token = PlanReader::pg_strtok(&length);
	unsigned int const_len = atoi(token);
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
	if (const_is_null) {
		token = PlanReader::pg_strtok(&length); /* skip "<>" */
		return nullptr;
	} else {
		PGDatum datum = ReadDatum(const_by_val);
		switch (const_type) {
		case Int: {
			return make_uniq<SimplestConstVar>((int)datum);
		}
		case Float: {
			// todo: check
			return make_uniq<SimplestConstVar>((float)datum);
		}
		case String: {
			// todo: check
			return make_uniq<SimplestConstVar>(std::to_string(datum));
		}
		default:
			Printer::Print("Doesn't support type " + std::to_string(const_type) + " yet!");
			D_ASSERT(false);
			return nullptr;
		}
	}
}

unique_ptr<SimplestNode> PlanReadFuncs::parseNodeString() {
	READ_TEMP_LOCALS();

	/* Guard against stack overflow due to overly complex expressions */
	// check_stack_depth();

	token = PlanReader::pg_strtok(&length);

#define MATCH(tokname, namelen) (length == namelen && memcmp(token, tokname, namelen) == 0)

	unique_ptr<SimplestNode> node;
	// debug
	if (MATCH("AGG", 3))
		node = ReadAgg();
	else if (MATCH("AGGREF", 6))
		node = ReadAggref();
	else if (MATCH("TARGETENTRY", 11))
		node = ReadTargetEntry();
	else if (MATCH("VAR", 3))
		node = ReadVar();
	else if (MATCH("GATHER", 6))
		ReadGather();
	else if (MATCH("HASH", 4))
		node = ReadHash();
	else if (MATCH("HASHJOIN", 8))
		node = ReadHashJoin();
	else if (MATCH("SEQSCAN", 7))
		node = ReadSeqScan();
	else if (MATCH("OPEXPR", 6))
		node = ReadOpExpr();
	else if (MATCH("CONST", 5))
		node = ReadConst();

	return node;
}
} // namespace duckdb
