// from Postgres source code: src/backend/nodes/read.c

#include "duckdb/optimizer/converter/read.hpp"

namespace duckdb {

/* Static state for PG_strtok */
static const char *pg_strtok_ptr = NULL;

/* State flag that determines how readfuncs.c should treat location fields */
#ifdef WRITE_READ_PARSE_PLAN_TREES
bool restore_location_fields = false;
#endif

/*
 * StringToNode -
 *	  builds a Node tree from its string representation (assumed valid)
 *
 * restore_loc_fields instructs readfuncs.c whether to restore location
 * fields rather than set them to -1.  This is currently only supported
 * in builds with the WRITE_READ_PARSE_PLAN_TREES debugging flag set.
 */
unique_ptr<SimplestNode> PlanReader::StringToNodeInternal(const char *str, bool restore_loc_fields) {
	const char *save_strtok;
	unique_ptr<SimplestNode> node;

#ifdef WRITE_READ_PARSE_PLAN_TREES
	bool save_restore_location_fields;
#endif

	/*
	 * We save and restore the pre-existing state of PG_strtok. This makes the
	 * world safe for re-entrant invocation of StringToNode, without incurring
	 * a lot of notational overhead by having to pass the next-character
	 * pointer around through all the readfuncs.c code.
	 */
	save_strtok = pg_strtok_ptr;

	pg_strtok_ptr = str; /* point PG_strtok at the string to read */

	/*
	 * If enabled, likewise save/restore the location field handling flag.
	 */
#ifdef WRITE_READ_PARSE_PLAN_TREES
	save_restore_location_fields = restore_location_fields;
	restore_location_fields = restore_loc_fields;
#endif

	node = NodeRead(NULL, 0); /* do the reading */

	pg_strtok_ptr = save_strtok;

#ifdef WRITE_READ_PARSE_PLAN_TREES
	restore_location_fields = save_restore_location_fields;
#endif

	return node;
}

/*
 * Externally visible entry points
 */
unique_ptr<SimplestNode> PlanReader::StringToNode(const char *str) {
	return StringToNodeInternal(str, false);
}

#ifdef WRITE_READ_PARSE_PLAN_TREES

void *stringToNodeWithLocations(const char *str) {
	return stringToNodeInternal(str, true);
}

#endif

/*****************************************************************************
 *
 * the lisp token parser
 *
 *****************************************************************************/

/*
 * PG_strtok --- retrieve next "token" from a string.
 *
 * Works kinda like strtok, except it never modifies the source string.
 * (Instead of storing nulls into the string, the length of the token
 * is returned to the caller.)
 * Also, the rules about what is a token are hard-wired rather than being
 * configured by passing a set of terminating characters.
 *
 * The string is assumed to have been initialized already by StringToNode.
 *
 * The rules for tokens are:
 *	* Whitespace (space, tab, newline) always separates tokens.
 *	* The characters '(', ')', '{', '}' form individual tokens even
 *	  without any whitespace around them.
 *	* Otherwise, a token is all the characters up to the next whitespace
 *	  or occurrence of one of the four special characters.
 *	* A backslash '\' can be used to quote whitespace or one of the four
 *	  special characters, so that it is treated as a plain token character.
 *	  Backslashes themselves must also be backslashed for consistency.
 *	  Any other character can be, but need not be, backslashed as well.
 *	* If the resulting token is '<>' (with no backslash), it is returned
 *	  as a non-NULL pointer to the token but with length == 0.  Note that
 *	  there is no other way to get a zero-length token.
 *
 * Returns a pointer to the start of the next token, and the length of the
 * token (including any embedded backslashes!) in *length.  If there are
 * no more tokens, NULL and 0 are returned.
 *
 * NOTE: this routine doesn't remove backslashes; the caller must do so
 * if necessary (see "DeBackslash").
 *
 * NOTE: prior to release 7.0, this routine also had a special case to treat
 * a token starting with '"' as extending to the next '"'.  This code was
 * broken, however, since it would fail to cope with a string containing an
 * embedded '"'.  I have therefore removed this special case, and instead
 * introduced rules for using backslashes to quote characters.  Higher-level
 * code should add backslashes to a string constant to ensure it is treated
 * as a single token.
 */
const char *PlanReader::PG_strtok(int *length) {
	const char *local_str; /* working pointer to string */
	const char *ret_str;   /* start of token to return */

	local_str = pg_strtok_ptr;

	while (*local_str == ' ' || *local_str == '\n' || *local_str == '\t')
		local_str++;

	if (*local_str == '\0') {
		*length = 0;
		pg_strtok_ptr = local_str;
		return NULL; /* no more tokens */
	}

	/*
	 * Now pointing at start of next token.
	 */
	ret_str = local_str;

	if (*local_str == '(' || *local_str == ')' || *local_str == '{' || *local_str == '}') {
		/* special 1-character token */
		local_str++;
	} else {
		/* Normal token, possibly containing backslashes */
		while (*local_str != '\0' && *local_str != ' ' && *local_str != '\n' && *local_str != '\t' &&
		       *local_str != '(' && *local_str != ')' && *local_str != '{' && *local_str != '}') {
			if (*local_str == '\\' && local_str[1] != '\0')
				local_str += 2;
			else
				local_str++;
		}
	}

	*length = local_str - ret_str;

	/* Recognize special case for "empty" token */
	if (*length == 2 && ret_str[0] == '<' && ret_str[1] == '>')
		*length = 0;

	pg_strtok_ptr = local_str;

	return ret_str;
}

/*
 * DeBackslash -
 *	  create a palloc'd string holding the given token.
 *	  any protective backslashes in the token are removed.
 */
char *PlanReader::DeBackslash(const char *token, int length) {
	char *result = (char *)malloc(length + 1);
	char *ptr = result;

	while (length > 0) {
		if (*token == '\\' && length > 1)
			token++, length--;
		*ptr++ = *token++;
		length--;
	}
	*ptr = '\0';
	return result;
}

#define RIGHT_PAREN (1000000 + 1)
#define LEFT_PAREN  (1000000 + 2)
#define LEFT_BRACE  (1000000 + 3)
#define OTHER_TOKEN (1000000 + 4)

int PlanReader::StrToInt(const char *str, char **endptr, int base) {
	long val;

	val = strtol(str, endptr, base);
	if (val != (int)val)
		errno = ERANGE;
	return (int)val;
}

/*
 * NodeTokenType -
 *	  returns the type of the node token contained in token.
 *	  It returns one of the following valid NodeTags:
 *		T_Integer, T_Float, T_String, T_BitString
 *	  and some of its own:
 *		RIGHT_PAREN, LEFT_PAREN, LEFT_BRACE, OTHER_TOKEN
 *
 *	  Assumption: the ascii representation is legal
 */
duckdb_libpgquery::PGNodeTag PlanReader::NodeTokenType(const char *token, int length) {
	duckdb_libpgquery::PGNodeTag retval;
	const char *numptr;
	int numlen;

	/*
	 * Check if the token is a number
	 */
	numptr = token;
	numlen = length;
	if (*numptr == '+' || *numptr == '-')
		numptr++, numlen--;
	if ((numlen > 0 && isdigit((unsigned char)*numptr)) ||
	    (numlen > 1 && *numptr == '.' && isdigit((unsigned char)numptr[1]))) {
		/*
		 * Yes.  Figure out whether it is integral or float; this requires
		 * both a syntax check and a range check. strtoint() can do both for
		 * us. We know the token will end at a character that StrToInt will
		 * stop at, so we do not need to modify the string.
		 */
		char *endptr;

		errno = 0;
		(void)StrToInt(token, &endptr, 10);
		if (endptr != token + length || errno == ERANGE)
			return duckdb_libpgquery::PGNodeTag::T_PGFloat;
		return duckdb_libpgquery::PGNodeTag::T_PGInteger;
	}

	/*
	 * these three cases do not need length checks, since PG_strtok() will
	 * always treat them as single-byte tokens
	 */
	else if (*token == '(')
		retval = (duckdb_libpgquery::PGNodeTag)LEFT_PAREN;
	else if (*token == ')')
		retval = (duckdb_libpgquery::PGNodeTag)RIGHT_PAREN;
	else if (*token == '{')
		retval = (duckdb_libpgquery::PGNodeTag)LEFT_BRACE;
	else if (*token == '"' && length > 1 && token[length - 1] == '"')
		retval = duckdb_libpgquery::T_PGString;
	else if (*token == 'b')
		retval = duckdb_libpgquery::T_PGBitString;
	else
		retval = (duckdb_libpgquery::PGNodeTag)OTHER_TOKEN;
	return retval;
}

/*
 * NodeRead -
 *	  Slightly higher-level reader.
 *
 * This routine applies some semantic knowledge on top of the purely
 * lexical tokenizer pg_strtok().   It can read
 *	* Value token nodes (integers, floats, or strings);
 *	* General nodes (via ParseNodeString() from readfuncs.c);
 *	* Lists of the above;
 *	* Lists of integers or OIDs.
 * The return value is declared void *, not Node *, to avoid having to
 * cast it explicitly in callers that assign to fields of different types.
 *
 * External callers should always pass NULL/0 for the arguments.  Internally
 * a non-NULL token may be passed when the upper recursion level has already
 * scanned the first token of a node's representation.
 *
 * We assume PG_strtok is already initialized with a string to read (hence
 * this should only be invoked from within a StringToNode operation).
 */
/// there might be more than one attrs in `targetlist`
// todo: `return_vector` is not elegant, need to refactor
unique_ptr<SimplestNode> PlanReader::NodeRead(const char *token, int tok_len, bool return_vector,
                                              std::vector<unique_ptr<SimplestNode>> *node_vec) {
	duckdb_libpgquery::PGNodeTag type;

	unique_ptr<SimplestNode> node;

	if (token == NULL) /* need to read a token? */
	{
		token = PG_strtok(&tok_len);

		if (token == NULL) /* end of input */
			return NULL;
	}

	type = NodeTokenType(token, tok_len);

	switch ((int)type) {
	case LEFT_BRACE:
		node = ParseNodeString();
		token = PG_strtok(&tok_len);
		if (token == NULL || token[0] != '}')
			Printer::Print("Error! did not find '}' at end of input node in NodeRead");
		break;
	case LEFT_PAREN: {
		//		duckdb_libpgquery::PGList *l = NULL;

		/*----------
		 * Could be an integer list:	(i int int ...)
		 * or an OID list:				(o int int ...)
		 * or a list of nodes/values:	(node node ...)
		 *----------
		 */
		token = PG_strtok(&tok_len);
		if (token == NULL)
			Printer::Print("Error! unterminated List structure in NodeRead");
		if (tok_len == 1 && token[0] == 'i') {
			/* List of integers */
			for (;;) {
				int val;
				char *endptr;

				token = PG_strtok(&tok_len);
				if (token == NULL)
					Printer::Print("Error! unterminated List structure in NodeRead");
				if (token[0] == ')')
					break;
				val = (int)strtol(token, &endptr, 10);
				if (endptr != token + tok_len) {
					auto str = std::to_string(tok_len) + "-" + token;
					Printer::Print("Error! unrecognized integer: \"" + str + "\" in NodeRead");
				}
				//				l = lappend_int(l, val);
			}
		} else if (tok_len == 1 && token[0] == 'o') {
			/* List of OIDs */
			for (;;) {
				Oid val;
				char *endptr;

				token = PG_strtok(&tok_len);
				if (token == NULL)
					Printer::Print("Error! unterminated List structure in NodeRead");
				if (token[0] == ')')
					break;
				val = (Oid)strtoul(token, &endptr, 10);
				if (endptr != token + tok_len) {
					auto str = std::to_string(tok_len) + "." + token;
					Printer::Print("unrecognized OID:" + str);
				}

				//				l = lappend_oid(l, val);
			}
		} else {
			/* List of other node types */
			for (;;) {
				/* We have already scanned next token... */
				if (token[0] == ')')
					break;
				node = NodeRead(token, tok_len, return_vector, node_vec);
				//				l = lappend(l, NodeRead(token, tok_len));
				if (return_vector) {
					node_vec->emplace_back(std::move(node));
				}
				token = PG_strtok(&tok_len);
				if (token == NULL)
					Printer::Print("Error! unterminated List structure in NodeRead");
			}
		}
		//		result = (duckdb_libpgquery::PGNode *)l;
		break;
	}
	case RIGHT_PAREN:
		Printer::Print("Error! unexpected right parenthesis in NodeRead");
		node = NULL; /* keep compiler happy */
		break;
	case OTHER_TOKEN:
		if (tok_len == 0) {
			/* must be "<>" --- represents a null pointer */
			node = NULL;
		} else {
			auto str = std::to_string(tok_len) + "-" + token;
			Printer::Print("Error! unrecognized token: \"" + str + "\" in NodeRead");
			node = NULL; /* keep compiler happy */
		}
		break;
	case duckdb_libpgquery::T_PGInteger:
		/*
		 * we know that the token terminates on a char atoi will stop at
		 */
		//		result = (duckdb_libpgquery::PGNode *)duckdb_libpgquery::makeInteger(atoi(token));
		break;
	case duckdb_libpgquery::T_PGFloat: {
		//		char *fval = (char *)malloc(tok_len + 1);
		//
		//		memcpy(fval, token, tok_len);
		//		fval[tok_len] = '\0';
		//		result = (duckdb_libpgquery::PGNode *)duckdb_libpgquery::makeFloat(fval);
		break;
	}
	case duckdb_libpgquery::T_PGString:
		/* need to remove leading and trailing quotes, and backslashes */
		return make_uniq<SimplestLiteral>(std::string(token + 1, tok_len - 2));
	case duckdb_libpgquery::T_PGBitString: {
		//		char *val = (char *)malloc(tok_len);
		//
		//		/* skip leading 'b' */
		//		memcpy(val, token + 1, tok_len - 1);
		//		val[tok_len - 1] = '\0';
		//		result = (duckdb_libpgquery::PGNode *)duckdb_libpgquery::makeBitString(val);
		break;
	}
	default:
		Printer::Print("Error! unrecognized node type: " + std::to_string(type) + " in NodeRead");
		node = NULL; /* keep compiler happy */
		break;
	}

	return node;
}

void *PlanReader::ReadBitmapset() {
	READ_TEMP_LOCALS();

	token = PG_strtok(&length);
	if (token == NULL)
		Printer::Print("Error! incomplete Bitmapset structure");
	if (length != 1 || token[0] != '(')
		Printer::Print("Error! unrecognized token: (" + std::to_string(length) + ")\"" + token + "\" in ReadBitmapset");

	token = PG_strtok(&length);
	if (token == NULL)
		Printer::Print("Error! incomplete Bitmapset structure");
	if (length != 1 || token[0] != 'b')
		Printer::Print("Error! unrecognized token: (" + std::to_string(length) + ")\"" + token + "\" in ReadBitmapset");

	for (;;) {
		int val;
		char *endptr;

		token = PG_strtok(&length);
		if (token == NULL)
			Printer::Print("Error! unterminated Bitmapset structure");
		if (length == 1 && token[0] == ')')
			break;
		val = (int)strtol(token, &endptr, 10);
		if (endptr != token + length)
			Printer::Print("Error! unrecognized integer: (" + std::to_string(length) + ")\"" + token +
			               "\" in ReadBitmapset");
		//		result = bms_add_member(result, val);
	}

	return nullptr;
}

std::vector<int> PlanReader::ReadAttrNumberCols(int numCols) {
	int tokenLength, i;
	const char *token;
	std::vector<int> attr_cols;

	for (i = 0; i < numCols; i++) {
		token = PG_strtok(&tokenLength);
		attr_cols.emplace_back(atoi(token));
	}

	return attr_cols;
}

std::vector<int> PlanReader::ReadIntCols(int numCols) {
	int tokenLength, i;
	const char *token;
	std::vector<int> oid_cols;

	//	oid_vals = (PGOid *) malloc(numCols * sizeof(PGOid));
	for (i = 0; i < numCols; i++) {
		token = PG_strtok(&tokenLength);
		oid_cols.emplace_back(atoi(token));
	}

	return oid_cols;
}

std::vector<bool> PlanReader::ReadBoolCols(int numCols) {
	int tokenLength, i;
	const char *token;
	std::vector<bool> bool_cols;

	//	oid_vals = (PGOid *) malloc(numCols * sizeof(PGOid));
	for (i = 0; i < numCols; i++) {
		token = PG_strtok(&tokenLength);
		bool_cols.emplace_back(strtobool(token));
	}

	return bool_cols;
}

unique_ptr<SimplestStmt> PlanReader::ReadCommonPlan() {
	READ_TEMP_LOCALS();

	std::vector<unique_ptr<SimplestStmt>> children;

	// startup_cost
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// total_cost
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// plan_rows
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// plan_width
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// parallel_awre
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// parallel_safe
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// plan_node_id
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// targetlist
	token = PG_strtok(&length);
	(void)token;
	// this should return a vector of vars
	// use a member variable - target_list_vec
	//     NodeRead(NULL, 0, is_a_list)
	std::vector<unique_ptr<SimplestNode>> node_vec;
	std::vector<unique_ptr<SimplestAttr>> attr_vec;
	NodeRead(NULL, 0, true, &node_vec);
	for (auto &node : node_vec) {
		if (node)
			attr_vec.emplace_back(unique_ptr_cast<SimplestNode, SimplestAttr>(std::move(node)));
	}
	// qual
	// e.g. `qual` should be an expr, if we have more than one `qual`,
	// we need to change the `NodeRead()` to `std::vector<unique_ptr<SimplestNode>>`
	token = PG_strtok(&length);
	(void)token;
	node_vec.clear();
	std::vector<unique_ptr<SimplestExpr>> qual_vec;
	auto qual_node = NodeRead(NULL, 0, true, &node_vec);
	for (auto &node : node_vec) {
		if (node)
			qual_vec.emplace_back(unique_ptr_cast<SimplestNode, SimplestExpr>(std::move(node)));
	}
	// lefttree
	token = PG_strtok(&length);
	(void)token;
	unique_ptr<SimplestStmt> left_tree_stmt = unique_ptr_cast<SimplestNode, SimplestStmt>(NodeRead(NULL, 0));
	if (left_tree_stmt)
		children.emplace_back(std::move(left_tree_stmt));
	// righttree
	token = PG_strtok(&length);
	(void)token;
	unique_ptr<SimplestStmt> right_tree_stmt = unique_ptr_cast<SimplestNode, SimplestStmt>(NodeRead(NULL, 0));
	if (right_tree_stmt)
		children.emplace_back(std::move(right_tree_stmt));
	// initPlan
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// extParam
	token = PG_strtok(&length);
	(void)token;
	ReadBitmapset();
	// allParam
	token = PG_strtok(&length);
	(void)token;
	ReadBitmapset();

	if (children.empty())
		return make_uniq<SimplestStmt>(std::move(attr_vec), std::move(qual_vec), StmtNode);
	else
		return make_uniq<SimplestStmt>(std::move(children), std::move(attr_vec), std::move(qual_vec), StmtNode);
}

unique_ptr<SimplestAggregate> PlanReader::ReadAgg() {
	READ_TEMP_LOCALS();

	unique_ptr<SimplestStmt> common_stmt = ReadCommonPlan();
	std::vector<SimplestVarType> agg_type_vec;
	for (const auto &target : common_stmt->target_list) {
		// todo: need to check the order
		agg_type_vec.emplace_back(target->GetType());
	}
	unique_ptr<SimplestAggregate> agg_stmt = make_uniq<SimplestAggregate>(std::move(common_stmt), agg_type_vec);

	// AggStrategy
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// AggSplit
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// numCols
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	int numCols = atoi(token);
	// grpColIdx
	token = PG_strtok(&length);
	ReadAttrNumberCols(numCols);
	// grpOperators
	token = PG_strtok(&length);
	ReadIntCols(numCols);
	// grpCollations
	token = PG_strtok(&length);
	ReadIntCols(numCols);
	// numGroups
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// aggParams
	token = PG_strtok(&length);
	(void)token;
	ReadBitmapset();
	// groupingSets
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// chain
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);

	return agg_stmt;
}

unique_ptr<SimplestAttr> PlanReader::ReadAggref() {
	READ_TEMP_LOCALS();

	// aggfnoid
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// aggtype
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	unsigned int agg_type_id = atoi(token);
	SimplestVarType agg_type = GetSimplestVarType(agg_type_id);
	// aggcollid
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// inputcollid
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// aggtranstype
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// aggargtypes
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// aggdirectargs
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// args
	token = PG_strtok(&length);
	(void)token;
	auto args_node = NodeRead(NULL, 0);
	unique_ptr<SimplestAttr> aggr_attr = unique_ptr_cast<SimplestNode, SimplestAttr>(std::move(args_node));
	// aggorder
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// aggdistinct
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// aggfilter
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// aggstar
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// aggvariadic
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// aggkind
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	auto agg_kind = (length == 0) ? '\0' : (token[0] == '\\' ? token[1] : token[0]);
	// agglevelsup
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// aggsplit
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// location
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	(void)token;

	return aggr_attr;
}

unique_ptr<SimplestNode> PlanReader::ReadTargetEntry() {
	READ_TEMP_LOCALS();

	unique_ptr<SimplestNode> node;

	// expr
	// check if TARGETENTRY has only one expr
	token = PG_strtok(&length);
	(void)token;
	node = NodeRead(NULL, 0);
	// resno
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// resname
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	nullable_string(token, length);
	// ressortgroupref
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// resorigtbl
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// resorigcol
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// resjunk
	token = PG_strtok(&length);
	token = PG_strtok(&length);

	return node;
}

unique_ptr<SimplestAttr> PlanReader::ReadVar() {
	READ_TEMP_LOCALS();

	// varno
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// varattno
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// vartype
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	unsigned int var_type_id = atoi(token);
	SimplestVarType var_type = GetSimplestVarType(var_type_id);
	// vartypmod
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// varcollid
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// varlevelsup
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// varnoold
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	unsigned int table_index = atoi(token);
	// varoattno
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	unsigned int column_index = atoi(token);
	// location
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	(void)token;
	return make_uniq<SimplestAttr>(var_type, table_index, column_index, "");
}

unique_ptr<SimplestAttr> PlanReader::ReadRelabelType() {
	READ_TEMP_LOCALS();

	// arg
	token = PG_strtok(&length);
	(void)token;
	auto arg_node = NodeRead(NULL, 0);
	unique_ptr<SimplestAttr> arg_attr = unique_ptr_cast<SimplestNode, SimplestAttr>(std::move(arg_node));
	// resulttype
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	unsigned int var_type_id = atoi(token);
	SimplestVarType new_var_type = GetSimplestVarType(var_type_id);
	arg_attr->ChangeVarType(new_var_type);
	// resulttypmod
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// resultcollid
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// relabelformat
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// location
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	(void)token;

	return arg_attr;
}

unique_ptr<SimplestParam> PlanReader::ReadParam() {
	READ_TEMP_LOCALS();

	// paramkind
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// paramid
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	unsigned int param_id = atoi(token);
	// paramtype
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	unsigned int var_type_id = atoi(token);
	SimplestVarType var_type = GetSimplestVarType(var_type_id);
	// paramtypmod
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// paramcollid
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// location
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	(void)token;

	return make_uniq<SimplestParam>(var_type, param_id);
}

void PlanReader::ReadGather() {
	READ_TEMP_LOCALS();

	ReadCommonPlan();

	// num_workers
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// rescan_param
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// single_copy
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// invisible
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// initParam
	token = PG_strtok(&length);
	(void)token;
	ReadBitmapset();
}

unique_ptr<SimplestJoin> PlanReader::ReadCommonJoin() {
	READ_TEMP_LOCALS();

	unique_ptr<SimplestStmt> common_stmt = ReadCommonPlan();

	// jointype
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	SimplestJoinType join_type = GetSimplestJoinType(atoi(token));
	// inner_unique
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// joinqual
	token = PG_strtok(&length);
	(void)token;
	std::vector<unique_ptr<SimplestNode>> node_vec;
	std::vector<unique_ptr<SimplestVarComparison>> join_conditions;
	auto qual_node = NodeRead(NULL, 0, true, &node_vec);
	for (auto &node : node_vec) {
		if (node)
			join_conditions.emplace_back(unique_ptr_cast<SimplestNode, SimplestVarComparison>(std::move(node)));
	}

	if (join_conditions.empty()) {
		return make_uniq<SimplestJoin>(std::move(common_stmt), join_type);
	} else {
		return make_uniq<SimplestJoin>(std::move(common_stmt), std::move(join_conditions), join_type);
	}
}

unique_ptr<SimplestHash> PlanReader::ReadHash() {
	READ_TEMP_LOCALS();

	unique_ptr<SimplestStmt> common_plan = ReadCommonPlan();

	// hashkeys
	token = PG_strtok(&length);
	(void)token;
	std::vector<unique_ptr<SimplestNode>> node_vec;
	std::vector<unique_ptr<SimplestAttr>> hash_keys;
	NodeRead(NULL, 0, true, &node_vec);
	for (auto &node : node_vec) {
		if (node)
			hash_keys.emplace_back(unique_ptr_cast<SimplestNode, SimplestAttr>(std::move(node)));
	}
	// skewTable
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// skewColumn
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// skewInherit
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// rows_total
	token = PG_strtok(&length);
	token = PG_strtok(&length);

	unique_ptr<SimplestHash> hash_stmt = make_uniq<SimplestHash>(std::move(common_plan), std::move(hash_keys));

	return hash_stmt;
}

unique_ptr<SimplestJoin> PlanReader::ReadHashJoin() {
	READ_TEMP_LOCALS();

	unique_ptr<SimplestJoin> common_join = ReadCommonJoin();

	// hashclauses
	// get join condition
	token = PG_strtok(&length);
	(void)token;
	// this should get a vector of exprs
	std::vector<unique_ptr<SimplestNode>> node_vec;
	// todo: need to check if it's always SimplestVarComparison
	std::vector<unique_ptr<SimplestVarComparison>> join_conditions;
	NodeRead(NULL, 0, true, &node_vec);
	for (auto &node : node_vec) {
		if (node)
			join_conditions.emplace_back(unique_ptr_cast<SimplestNode, SimplestVarComparison>(std::move(node)));
	}
	common_join->AddJoinCondition(std::move(join_conditions));
	// hashoperators
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// hashcollations
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// hashkeys
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);

	return common_join;
}

unique_ptr<SimplestJoin> PlanReader::ReadMergeJoin() {
	READ_TEMP_LOCALS();

	unique_ptr<SimplestJoin> common_join = ReadCommonJoin();
	// skip_mark_restore
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// mergeclauses
	// get join condition
	token = PG_strtok(&length);
	(void)token;
	// this should get a vector of exprs
	std::vector<unique_ptr<SimplestNode>> node_vec;
	// todo: need to check if it's always SimplestVarComparison
	std::vector<unique_ptr<SimplestVarComparison>> join_conditions;
	NodeRead(NULL, 0, true, &node_vec);
	for (auto &node : node_vec) {
		if (node)
			join_conditions.emplace_back(unique_ptr_cast<SimplestNode, SimplestVarComparison>(std::move(node)));
	}
	int cond_num = join_conditions.size();
	common_join->AddJoinCondition(std::move(join_conditions));
	// mergeFamilies
	token = PG_strtok(&length);
	ReadIntCols(cond_num);
	// mergeCollations
	token = PG_strtok(&length);
	ReadIntCols(cond_num);
	// mergeStrategies
	token = PG_strtok(&length);
	ReadIntCols(cond_num);
	// mergeNullsFirst
	token = PG_strtok(&length);
	ReadBoolCols(cond_num);

	return common_join;
}

unique_ptr<SimplestJoin> PlanReader::ReadNestLoop() {
	READ_TEMP_LOCALS();

	unique_ptr<SimplestJoin> nest_loop_join = ReadCommonJoin();

	// nestParams
	token = PG_strtok(&length);
	(void)token;
	unique_ptr<SimplestNode> nest_params_node = NodeRead(NULL, 0);
	unique_ptr<SimplestVarComparison> nest_loop_cond =
	    unique_ptr_cast<SimplestNode, SimplestVarComparison>(std::move(nest_params_node));

	if (nest_loop_cond) {
		// construct the nest loop join
		std::vector<unique_ptr<SimplestVarComparison>> nest_loop_cond_vec;
		nest_loop_cond_vec.emplace_back(std::move(nest_loop_cond));
		nest_loop_join->AddJoinCondition(std::move(nest_loop_cond_vec));
	}

	return nest_loop_join;
}

// todo: check if it only has one condition
unique_ptr<SimplestVarComparison> PlanReader::ReadNestLoopParam() {
	READ_TEMP_LOCALS();

	// paramno
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	unsigned int param_no = atoi(token);
	// paramval
	token = PG_strtok(&length);
	(void)token;
	unique_ptr<SimplestNode> node = NodeRead(NULL, 0);
	unique_ptr<SimplestAttr> param_val = unique_ptr_cast<SimplestNode, SimplestAttr>(std::move(node));
	// update the param_val's table/column index in the nest-loop join condition
	// <paramno, <paramval.varnoold, paramval.varoattno>>
	auto find = std::find_if(index_conditions.begin(), index_conditions.end(),
	                         [param_no](const unique_ptr<SimplestVarParamComparison> &cond) {
		                         return cond->param_var->GetParamId() == param_no;
	                         });
#ifdef DEBUG
	D_ASSERT(find != index_conditions.end());
#endif

	unique_ptr<SimplestVarComparison> nest_loop_cond = make_uniq<SimplestVarComparison>(
	    find->get()->GetSimplestExprType(), std::move(find->get()->attr), std::move(param_val));
	return nest_loop_cond;
}

unique_ptr<SimplestScan> PlanReader::ReadCommonScan() {
	READ_TEMP_LOCALS();

	// common_plan
	unique_ptr<SimplestStmt> common_plan = ReadCommonPlan();
	// scanrelid
	token = PG_strtok(&length);
	token = PG_strtok(&length);

	if (common_plan->target_list.empty())
		return nullptr;

	// get table index from common_plan->target_list
	unsigned int table_index = common_plan->target_list[0]->GetTableIndex();
#ifdef DEBUG
	for (const auto &attr : common_plan->target_list) {
		D_ASSERT(attr->GetTableIndex() == table_index);
	}
#endif
	auto common_scan = make_uniq<SimplestScan>(std::move(common_plan), table_index, "");

	return common_scan;
}

unique_ptr<SimplestScan> PlanReader::ReadSeqScan() {
	return ReadCommonScan();
}

unique_ptr<SimplestScan> PlanReader::ReadIndexScan() {
	READ_TEMP_LOCALS();

	// common_scan
	unique_ptr<SimplestScan> common_scan = ReadCommonScan();
	// indexid
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// indexqual
	token = PG_strtok(&length);
	(void)token;
	std::vector<unique_ptr<SimplestNode>> node_vec;
	NodeRead(NULL, 0, true, &node_vec);
	for (auto &node : node_vec) {
		if (node) {
			// todo: check if it has the same condition as `indexqualorig`?
		}
	}
	// indexqualorig
	// we should rely on this as the nest loop join condition
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0, true, &node_vec);
	for (auto &node : node_vec) {
		if (node)
			index_conditions.emplace_back(unique_ptr_cast<SimplestNode, SimplestVarParamComparison>(std::move(node)));
	}
	// indexorderby
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// indexorderbyorig
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// indexorderbyops
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// indexorderdir
	token = PG_strtok(&length);
	token = PG_strtok(&length);

	return common_scan;
}

unique_ptr<SimplestScan> PlanReader::ReadIndexOnlyScan() {
	READ_TEMP_LOCALS();

	// common_scan
	unique_ptr<SimplestScan> common_scan = ReadCommonScan();
	// indexid
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// indexqual
	std::vector<unique_ptr<SimplestNode>> node_vec;
	// we should rely on this as the nest loop join condition
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0, true, &node_vec);
	for (auto &node : node_vec) {
		if (node)
			index_conditions.emplace_back(unique_ptr_cast<SimplestNode, SimplestVarParamComparison>(std::move(node)));
	}
	// indexorderby
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// indextlist
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// indexorderdir
	token = PG_strtok(&length);
	token = PG_strtok(&length);

	return common_scan;
}

unique_ptr<SimplestScan> PlanReader::ReadBitmapHeapScan() {
	READ_TEMP_LOCALS();

	// common scan
	unique_ptr<SimplestScan> common_scan = ReadCommonScan();
	// bitmapqualorig
	token = PG_strtok(&length);
	(void)token;
	std::vector<unique_ptr<SimplestNode>> node_vec;
	NodeRead(NULL, 0, true, &node_vec);
	for (auto &node : node_vec) {
		if (node) {
			// todo: check if it has the same condition as `indexqualorig`
		}
	}

	return common_scan;
}

unique_ptr<SimplestNode> PlanReader::ReadBitmapIndexScan() {
	READ_TEMP_LOCALS();

	// common scan
	ReadCommonScan();
	// indexid
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// isshared
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// indexqual
	token = PG_strtok(&length);
	(void)token;
	std::vector<unique_ptr<SimplestNode>> node_vec;
	NodeRead(NULL, 0, true, &node_vec);
	for (auto &node : node_vec) {
		if (node) {
			// todo: check if it has the same condition as `indexqualorig`?
		}
	}
	// indexqualorig
	// we should rely on this as the nest loop join condition
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0, true, &node_vec);
	for (auto &node : node_vec) {
		if (node)
			index_conditions.emplace_back(unique_ptr_cast<SimplestNode, SimplestVarParamComparison>(std::move(node)));
	}
	return unique_ptr<SimplestNode>();
}

unique_ptr<SimplestSort> PlanReader::ReadSort() {
	READ_TEMP_LOCALS();

	unique_ptr<SimplestStmt> common_stmt = ReadCommonPlan();
	// numCols
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	int num_col = atoi(token);
	std::vector<SimplestOrderStruct> order_vec;
	// sortColIdx
	token = PG_strtok(&length);
	std::vector<int> sort_col_idx = ReadAttrNumberCols(num_col);
	// sortOperators
	token = PG_strtok(&length);
	std::vector<int> sort_ops = ReadIntCols(num_col);
	// collations
	token = PG_strtok(&length);
	std::vector<int> collations = ReadIntCols(num_col);
	// nullsFirst
	token = PG_strtok(&length);
	std::vector<bool> nulls_first = ReadBoolCols(num_col);

	SimplestOrderStruct current_order_struct;
	for (int i = 0; i < num_col; i++) {
		current_order_struct.sort_col_idx = sort_col_idx[i];
		current_order_struct.order_type = GetSimplestComparisonType(sort_ops[i]);
		current_order_struct.text_order = GetSimplestTextOrderType(collations[i]);
		current_order_struct.nulls_first = nulls_first[i];
		order_vec.emplace_back(current_order_struct);
	}

	return make_uniq<SimplestSort>(std::move(common_stmt), order_vec);
}

unique_ptr<SimplestExpr> PlanReader::ReadOpExpr() {
	READ_TEMP_LOCALS();

	// opno
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	SimplestExprType op_type = GetSimplestComparisonType(atoi(token));
	// opfuncid
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// opresulttype
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// opretset
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// opcollid
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// inputcollid
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// args
	token = PG_strtok(&length);
	(void)token;
	std::vector<unique_ptr<SimplestNode>> node_vec;
	std::vector<unique_ptr<SimplestVar>> var_vec;
	NodeRead(NULL, 0, true, &node_vec);
	D_ASSERT(node_vec.size() == 2);
	for (auto &node : node_vec) {
		if (node)
			var_vec.emplace_back(unique_ptr_cast<SimplestNode, SimplestVar>(std::move(node)));
	}
	// todo: need to confirm if the var_vec[0] is always a variable
	D_ASSERT(!var_vec[0]->IsConst());
	// location
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	(void)token;

	if (var_vec[1]->IsConst()) {
		return make_uniq<SimplestVarConstComparison>(
		    op_type, unique_ptr_cast<SimplestVar, SimplestAttr>(std::move(var_vec[0])),
		    unique_ptr_cast<SimplestVar, SimplestConstVar>(std::move(var_vec[1])));
	} else if (ParamVarNode == var_vec[1]->GetNodeType()) {
		// todo: check if ParamVar only exist at the right part
		return make_uniq<SimplestVarParamComparison>(
		    op_type, unique_ptr_cast<SimplestVar, SimplestAttr>(std::move(var_vec[0])),
		    unique_ptr_cast<SimplestVar, SimplestParam>(std::move(var_vec[1])));
	} else {
		return make_uniq<SimplestVarComparison>(op_type,
		                                        unique_ptr_cast<SimplestVar, SimplestAttr>(std::move(var_vec[0])),
		                                        unique_ptr_cast<SimplestVar, SimplestAttr>(std::move(var_vec[1])));
	}
}

unique_ptr<SimplestLogicalExpr> PlanReader::ReadBoolExpr() {
	READ_TEMP_LOCALS();

	// boolop
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	SimplestLogicalOp logical_op = InvalidLogicalOp;
	if (strncmp(token, "and", 3) == 0)
		logical_op = LogicalAnd;
	else if (strncmp(token, "or", 2) == 0)
		logical_op = LogicalOr;
	else if (strncmp(token, "not", 3) == 0)
		logical_op = LogicalNot;
	else {
		Printer::Print("Doesn't support logical op " + std::to_string((int)logical_op) + " yet!");
		exit(-1);
	}

	// args
	token = PG_strtok(&length);
	(void)token;
	std::vector<unique_ptr<SimplestNode>> node_vec;
	std::vector<unique_ptr<SimplestExpr>> expr_vec;
	NodeRead(NULL, 0, true, &node_vec);
	D_ASSERT((LogicalNot == logical_op && node_vec.size() == 1) ||
	         ((LogicalAnd == logical_op || LogicalOr == logical_op) && node_vec.size() == 2));
	for (auto &node : node_vec) {
		if (node)
			expr_vec.emplace_back(unique_ptr_cast<SimplestNode, SimplestExpr>(std::move(node)));
	}
	// location
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	(void)token;

	if (LogicalNot == logical_op) {
		return make_uniq<SimplestLogicalExpr>(logical_op, nullptr, std::move(expr_vec[0]));
	} else {
		return make_uniq<SimplestLogicalExpr>(logical_op, std::move(expr_vec[0]), std::move(expr_vec[1]));
	}
}

unique_ptr<SimplestIsNullExpr> PlanReader::ReadNullTest() {
	READ_TEMP_LOCALS();

	// arg
	token = PG_strtok(&length);
	(void)token;
	auto arg_node = NodeRead(NULL, 0);
	unique_ptr<SimplestAttr> arg_attr = unique_ptr_cast<SimplestNode, SimplestAttr>(std::move(arg_node));
	// nulltesttype
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// 0 refer null, 1 refer not null
	SimplestExprType is_null_type;
	if (atoi(token))
		is_null_type = NonNullType;
	else
		is_null_type = NullType;
	// argisrow
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// location
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	(void)token;

	return make_uniq<SimplestIsNullExpr>(is_null_type, std::move(arg_attr));
}

unique_ptr<SimplestExpr> PlanReader::ReadScalarArrayOpExpr() {
	READ_TEMP_LOCALS();

	// opno
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	SimplestExprType op_type = GetSimplestComparisonType(atoi(token));
	// opfuncid
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// useOr
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// inputcollid
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// args
	token = PG_strtok(&length);
	(void)token;
	std::vector<unique_ptr<SimplestNode>> node_vec;
	std::vector<unique_ptr<SimplestVar>> var_vec;
	NodeRead(NULL, 0, true, &node_vec);
	D_ASSERT(node_vec.size() == 2);
	for (auto &node : node_vec) {
		if (node)
			var_vec.emplace_back(unique_ptr_cast<SimplestNode, SimplestVar>(std::move(node)));
	}
	// todo: need to confirm if the var_vec[0] is always a variable
	D_ASSERT(!var_vec[0]->IsConst());
	// location
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	(void)token;

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

unique_ptr<SimplestStmt> PlanReader::ReadMaterial() {
	auto material_node = ReadCommonPlan();

	// todo: support MaterialNode
	D_ASSERT(1 == material_node->children.size());

	return std::move(material_node->children[0]);
}

std::string PlanReader::ParseText(PGDatum datum, unsigned int datum_len) {
	const char *ptr = reinterpret_cast<const char *>(datum);

	// todo: check what does the first 4 bytes mean
	ptr += sizeof(int);
	datum_len -= sizeof(int);
	std::string str;
	str.assign(ptr, datum_len);

	return str;
}

std::vector<std::string> PlanReader::ParseTextArray(PGDatum datum, unsigned int datum_len) {
	const char *ptr = reinterpret_cast<const char *>(datum);

	std::vector<std::string> result;
	// todo: check the first 24 bytes
	size_t unused_bytes = 24;
	ptr += unused_bytes;
	datum_len -= unused_bytes;

	std::string str;
	while (datum_len) {
		ptr += sizeof(int);
		datum_len -= sizeof(int);
		auto check_nullptr = ptr;
		size_t str_len = 0;
		while ((datum_len != str_len) && strcmp(check_nullptr, "")) {
			check_nullptr++;
			str_len++;
		}

		str.assign(ptr, str_len);
		result.emplace_back(str);
		ptr += str_len;
		datum_len -= str_len;

		while ((0 != datum_len) && !strcmp(ptr, "")) {
			ptr++;
			datum_len--;
		}
	}

	return result;
}

unique_ptr<SimplestConstVar> PlanReader::ReadConst() {
	READ_TEMP_LOCALS();

	// consttype
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	unsigned int const_type_id = atoi(token);
	SimplestVarType const_type = GetSimplestVarType(const_type_id);
	// consttypmod
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// constcollid
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// constlen
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	unsigned int const_len = atoi(token);
	// constbyval
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	auto const_by_val = strtobool(token);
	// constisnull
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	auto const_is_null = strtobool(token);
	// location
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	(void)token;

	token = PG_strtok(&length);
	if (const_is_null) {
		token = PG_strtok(&length); /* skip "<>" */
		return nullptr;
	} else {
		unsigned int datum_len;
		PGDatum datum = ReadDatum(const_by_val, datum_len);
		switch (const_type) {
		case IntVar: {
			return make_uniq<SimplestConstVar>((int)datum);
		}
		case FloatVar: {
			return make_uniq<SimplestConstVar>((float)datum);
		}
		case StringVar: {
			std::string datum_text = ParseText(datum, datum_len);
			return make_uniq<SimplestConstVar>(datum_text);
		}
		case StringVarArr: {
			// Decode the array from the Datum
			std::vector<std::string> datum_text_arr = ParseTextArray(datum, datum_len);
			return make_uniq<SimplestConstVar>(datum_text_arr);
		}
		default:
			Printer::Print("Doesn't support type " + std::to_string(const_type) + " yet!");
			exit(-1);
		}
	}
}

unique_ptr<SimplestStmt> PlanReader::ReadPlannedStmt() {
	READ_TEMP_LOCALS();

	// commandType
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// queryId
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// hasReturning
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// hasModifyingCTE
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// canSetTag
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// transientPlan
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// dependsOnRole
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// parallelModeNeeded
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// jitFlags
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// planTree
	token = PG_strtok(&length);
	(void)token;
	unique_ptr<SimplestStmt> plan_tree = unique_ptr_cast<SimplestNode, SimplestStmt>(NodeRead(NULL, 0));
	// rtable
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// resultRelations
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// rootResultRelations
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// subplans
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// rewindPlanIDs
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// rowMarks
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// relationOids
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// invalItems
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// paramExecTypes
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// utilityStmt
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// stmt_location
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	(void)token;
	// stmt_len
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	(void)token;

	return plan_tree;
}

void PlanReader::ReadRangeTblEntry() {
	READ_TEMP_LOCALS();

	/* put alias + eref first to make dump more legible */
	// alias
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// eref
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
	// rtekind
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	duckdb_libpgquery::PGRTEKind rte_kind = (duckdb_libpgquery::PGRTEKind)atoi(token);
	switch (rte_kind) {
	case duckdb_libpgquery::PG_RTE_RELATION:
		// relid
		token = PG_strtok(&length);
		token = PG_strtok(&length);
		// relkind
		token = PG_strtok(&length);
		token = PG_strtok(&length);
		// rellockmode
		token = PG_strtok(&length);
		token = PG_strtok(&length);
		// tablesample
		token = PG_strtok(&length);
		(void)token;
		NodeRead(NULL, 0);
		break;
	case duckdb_libpgquery::PG_RTE_SUBQUERY:
		// subquery
		token = PG_strtok(&length);
		(void)token;
		NodeRead(NULL, 0);
		// security_barrier
		token = PG_strtok(&length);
		token = PG_strtok(&length);
		break;
	case duckdb_libpgquery::PG_RTE_JOIN:
		// jointype
		token = PG_strtok(&length);
		token = PG_strtok(&length);
		// joinaliasvars
		token = PG_strtok(&length);
		(void)token;
		NodeRead(NULL, 0);
		break;
	case duckdb_libpgquery::PG_RTE_FUNCTION:
		// functions
		token = PG_strtok(&length);
		(void)token;
		NodeRead(NULL, 0);
		// funcordinality
		token = PG_strtok(&length);
		token = PG_strtok(&length);
		break;
	case duckdb_libpgquery::PG_RTE_TABLEFUNC:
		// tablefunc
		token = PG_strtok(&length);
		(void)token;
		NodeRead(NULL, 0);
		break;
	case duckdb_libpgquery::PG_RTE_VALUES:
		// values_lists
		token = PG_strtok(&length);
		(void)token;
		NodeRead(NULL, 0);
		// coltypes
		token = PG_strtok(&length);
		(void)token;
		NodeRead(NULL, 0);
		// coltypmods
		token = PG_strtok(&length);
		(void)token;
		NodeRead(NULL, 0);
		// colcollations
		token = PG_strtok(&length);
		(void)token;
		NodeRead(NULL, 0);
		break;
	case duckdb_libpgquery::PG_RTE_CTE:
		// ctename
		token = PG_strtok(&length);
		token = PG_strtok(&length);
		// ctelevelsup
		token = PG_strtok(&length);
		token = PG_strtok(&length);
		// self_reference
		token = PG_strtok(&length);
		token = PG_strtok(&length);
		// coltypes
		token = PG_strtok(&length);
		(void)token;
		NodeRead(NULL, 0);
		// coltypmods
		token = PG_strtok(&length);
		(void)token;
		NodeRead(NULL, 0);
		// colcollations
		token = PG_strtok(&length);
		(void)token;
		NodeRead(NULL, 0);
		break;
	case duckdb_libpgquery::RTE_NAMEDTUPLESTORE:
		// enrname
		token = PG_strtok(&length);
		token = PG_strtok(&length);
		// enrtuples
		token = PG_strtok(&length);
		token = PG_strtok(&length);
		// relid
		token = PG_strtok(&length);
		token = PG_strtok(&length);
		// coltypes
		token = PG_strtok(&length);
		(void)token;
		NodeRead(NULL, 0);
		// coltypmods
		token = PG_strtok(&length);
		(void)token;
		NodeRead(NULL, 0);
		// colcollations
		token = PG_strtok(&length);
		(void)token;
		NodeRead(NULL, 0);
		break;
	default:
		Printer::Print("Error! unrecognized RTE kind: " + std::to_string(rte_kind));
		break;
	}
	// lateral
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// inh
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// inFromCl
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// requiredPerms
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// checkAsUser
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	// selectedCols
	token = PG_strtok(&length);
	(void)token;
	ReadBitmapset();
	// insertedCols
	token = PG_strtok(&length);
	(void)token;
	ReadBitmapset();
	// updatedCols
	token = PG_strtok(&length);
	(void)token;
	ReadBitmapset();
	// extraUpdatedCols
	token = PG_strtok(&length);
	(void)token;
	ReadBitmapset();
	// securityQuals
	token = PG_strtok(&length);
	(void)token;
	NodeRead(NULL, 0);
}

void PlanReader::ReadAlias() {
	READ_TEMP_LOCALS();

	// aliasname
	token = PG_strtok(&length);
	token = PG_strtok(&length);
	std::string alias_name(token, length);
	// colnames
	token = PG_strtok(&length);
	(void)token;
	std::vector<unique_ptr<SimplestNode>> node_vec;
	std::vector<unique_ptr<SimplestLiteral>> col_name_vec;
	NodeRead(NULL, 0, true, &node_vec);
	for (auto &node : node_vec) {
		if (node)
			col_name_vec.emplace_back(unique_ptr_cast<SimplestNode, SimplestLiteral>(std::move(node)));
	}
	table_str table_col_pair;
	table_col_pair[alias_name] = std::move(col_name_vec);
	table_col_names.push_back(std::move(table_col_pair));
}

PGDatum PlanReader::ReadDatum(bool typbyval, unsigned int &datum_len) {
	size_t i;
	int tokenLength;
	const char *token;
	PGDatum res;
	char *s;

	/*
	 * read the actual length of the value
	 */
	token = PG_strtok(&tokenLength);
	datum_len = atoi(token);

	token = PG_strtok(&tokenLength); /* read the '[' */
	if (token == NULL || token[0] != '[') {
		std::string token_str = token ? token : "[NULL]";
		Printer::Print("Error! expected \"[\" to start datum, but got " + token_str +
		               "; length = " + std::to_string(datum_len));
	}

	if (typbyval) {
		if (datum_len > (size_t)sizeof(PGDatum))
			Printer::Print("Error! byval datum but length = " + std::to_string(datum_len));
		res = (PGDatum)0;
		s = (char *)(&res);
		for (i = 0; i < (size_t)sizeof(PGDatum); i++) {
			token = PG_strtok(&tokenLength);
			s[i] = (char)atoi(token);
		}
	} else if (datum_len <= 0)
		res = (PGDatum)NULL;
	else {
		// todo: risk of memory leak
		s = (char *)malloc(datum_len);
		for (i = 0; i < datum_len; i++) {
			token = PG_strtok(&tokenLength);
			s[i] = (char)atoi(token);
		}
		res = (PGDatum)(s);
	}

	token = PG_strtok(&tokenLength); /* read the ']' */
	if (token == NULL || token[0] != ']') {
		std::string token_str = token ? token : "[NULL]";
		Printer::Print("Error! expected \"]\" to end datum, but got " + token_str +
		               "; length = " + std::to_string(datum_len));
	}

	return res;
}
SimplestVarType PlanReader::GetSimplestVarType(unsigned int type_id) {
	SimplestVarType simplest_var_type = InvalidVarType;
	switch (type_id) {
	case 20:
	case 21:
	case 23:
	case 26:
		simplest_var_type = IntVar;
		break;
	case 25:
	case 1043:
		simplest_var_type = StringVar;
		break;
	case 700:
	case 701:
		simplest_var_type = FloatVar;
		break;
	case 1009:
		simplest_var_type = StringVarArr;
		break;
	default:
		Printer::Print("Doesn't support type " + std::to_string(type_id) + " yet!");
		exit(-1);
	}

	return simplest_var_type;
}

SimplestJoinType PlanReader::GetSimplestJoinType(unsigned int type_id) {
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
		Printer::Print("Doesn't support type " + std::to_string(type_id) + " yet!");
		exit(-1);
	}

	return simplest_join_type;
}

SimplestExprType PlanReader::GetSimplestComparisonType(unsigned int type_id) {
	SimplestExprType simplest_comprison_type = InvalidExprType;
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
	case 1209:
		simplest_comprison_type = TEXT_LIKE;
		break;
	case 85:
	case 531:
		simplest_comprison_type = NotEqual;
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
	case 523:
		simplest_comprison_type = LessEqual;
		break;
	case 525:
		simplest_comprison_type = GreaterEqual;
		break;
	default:
		Printer::Print("Doesn't support type " + std::to_string(type_id) + " yet!");
		exit(-1);
	}

	return simplest_comprison_type;
}

SimplestTextOrder PlanReader::GetSimplestTextOrderType(int type_id) {
	SimplestTextOrder simplest_text_order = InvalidTextOrder;

	switch (type_id) {
	case 0:
		simplest_text_order = DefaultTextOrder;
		break;
	default:
		Printer::Print("Doesn't support text order type " + std::to_string(type_id) + " yet!");
		exit(-1);
	}

	return simplest_text_order;
}

unique_ptr<SimplestNode> PlanReader::ParseNodeString() {
	READ_TEMP_LOCALS();

	/* Guard against stack overflow due to overly complex expressions */
	// check_stack_depth();

	token = PG_strtok(&length);

#define MATCH(tokname, namelen) (length == namelen && memcmp(token, tokname, namelen) == 0)

	unique_ptr<SimplestNode> node;
	// debug
	if (MATCH("AGG", 3))
		node = ReadAgg();
	else if (MATCH("AGGREF", 6))
		node = ReadAggref();
	else if (MATCH("TARGETENTRY", 11))
		node = ReadTargetEntry();
	else if (MATCH("CONST", 5))
		node = ReadConst();
	else if (MATCH("VAR", 3))
		node = ReadVar();
	else if (MATCH("RELABELTYPE", 11))
		node = ReadRelabelType();
	else if (MATCH("PARAM", 5))
		node = ReadParam();
	else if (MATCH("GATHER", 6))
		ReadGather();
	else if (MATCH("HASH", 4))
		node = ReadHash();
	else if (MATCH("HASHJOIN", 8))
		node = ReadHashJoin();
	else if (MATCH("MERGEJOIN", 9))
		node = ReadMergeJoin();
	else if (MATCH("NESTLOOP", 8))
		node = ReadNestLoop();
	else if (MATCH("NESTLOOPPARAM", 13))
		node = ReadNestLoopParam();
	else if (MATCH("SEQSCAN", 7))
		node = ReadSeqScan();
	else if (MATCH("INDEXSCAN", 9))
		node = ReadIndexScan();
	else if (MATCH("INDEXONLYSCAN", 13))
		node = ReadIndexOnlyScan();
	else if (MATCH("BITMAPHEAPSCAN", 14))
		node = ReadBitmapHeapScan();
	else if (MATCH("BITMAPINDEXSCAN", 15))
		node = ReadBitmapIndexScan();
	else if (MATCH("SORT", 4))
		node = ReadSort();
	else if (MATCH("OPEXPR", 6))
		node = ReadOpExpr();
	else if (MATCH("BOOLEXPR", 8))
		node = ReadBoolExpr();
	else if (MATCH("NULLTEST", 8))
		node = ReadNullTest();
	else if (MATCH("SCALARARRAYOPEXPR", 17))
		node = ReadScalarArrayOpExpr();
	else if (MATCH("MATERIAL", 8))
		node = ReadMaterial();
	else if (MATCH("PLANNEDSTMT", 11))
		node = ReadPlannedStmt();
	else if (MATCH("RTE", 3))
		ReadRangeTblEntry();
	else if (MATCH("ALIAS", 5))
		ReadAlias();
	else {
		Printer::Print("Doesn't support node " + std::string(token) + " yet!");
		exit(-1);
	}

	return node;
}
} // namespace duckdb