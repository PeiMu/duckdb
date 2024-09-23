#include "duckdb/optimizer/converter/ir_to_duckdb.h"

namespace duckdb {
unique_ptr<LogicalOperator> IRConverter::InjectPlan(unique_ptr<LogicalOperator> duckdb_plan) {
	if (LogicalOperatorType::LOGICAL_PROJECTION != duckdb_plan->type &&
	    LogicalOperatorType::LOGICAL_ORDER_BY != duckdb_plan->type &&
	    LogicalOperatorType::LOGICAL_EXPLAIN != duckdb_plan->type) {
		return std::move(duckdb_plan);
	}

#ifdef DEBUG
	Printer::Print("original duckdb plan");
	duckdb_plan->Print();
#endif

	// get the postgres node string
	char *node_str =
	    "{PLANNEDSTMT :commandType 1 :queryId 0 :hasReturning false :hasModifyingCTE false :canSetTag true "
	    ":transientPlan false :dependsOnRole false :parallelModeNeeded false :jitFlags 25 :planTree {AGG :startup_cost "
	    "230537.55 :total_cost 230537.56 :plan_rows 1 :plan_width 36 :parallel_aware false :parallel_safe false "
	    ":plan_node_id 0 :targetlist ({TARGETENTRY :expr {AGGREF :aggfnoid 2145 :aggtype 25 :aggcollid 100 "
	    ":inputcollid 100 :aggtranstype 25 :aggargtypes (o 25) :aggdirectargs <> :args ({TARGETENTRY :expr {VAR :varno "
	    "65001 :varattno 1 :vartype 25 :vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 2 :varoattno 2 :location "
	    "11} :resno 1 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false}) :aggorder <> "
	    ":aggdistinct <> :aggfilter <> :aggstar false :aggvariadic false :aggkind n :agglevelsup 0 :aggsplit 0 "
	    ":location 7} :resno 1 :resname hero_movie :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} "
	    "{TARGETENTRY :expr {AGGREF :aggfnoid 2132 :aggtype 23 :aggcollid 0 :inputcollid 0 :aggtranstype 23 "
	    ":aggargtypes (o 23) :aggdirectargs <> :args ({TARGETENTRY :expr {VAR :varno 65001 :varattno 2 :vartype 23 "
	    ":vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 1 :varoattno 2 :location 50} :resno 1 :resname <> "
	    ":ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false}) :aggorder <> :aggdistinct <> :aggfilter <> "
	    ":aggstar false :aggvariadic false :aggkind n :agglevelsup 0 :aggsplit 0 :location 46} :resno 2 :resname min "
	    ":ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false}) :qual <> :lefttree {HASHJOIN :startup_cost "
	    "93079.94 :total_cost 218123.58 :plan_rows 2482795 :plan_width 21 :parallel_aware false :parallel_safe false "
	    ":plan_node_id 1 :targetlist ({TARGETENTRY :expr {VAR :varno 65000 :varattno 1 :vartype 25 :vartypmod -1 "
	    ":varcollid 100 :varlevelsup 0 :varnoold 2 :varoattno 2 :location 11} :resno 1 :resname <> :ressortgroupref 0 "
	    ":resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR :varno 65001 :varattno 1 :vartype 23 "
	    ":vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 1 :varoattno 2 :location 50} :resno 2 :resname <> "
	    ":ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false}) :qual <> :lefttree {SEQSCAN :startup_cost "
	    "0.00 :total_cost 69693.30 :plan_rows 4523930 :plan_width 4 :parallel_aware false :parallel_safe false "
	    ":plan_node_id 2 :targetlist ({TARGETENTRY :expr {VAR :varno 1 :varattno 2 :vartype 23 :vartypmod -1 "
	    ":varcollid 0 :varlevelsup 0 :varnoold 1 :varoattno 2 :location 50} :resno 1 :resname <> :ressortgroupref 0 "
	    ":resorigtbl 0 :resorigcol 0 :resjunk false}) :qual <> :lefttree <> :righttree <> :initPlan <> :extParam (b) "
	    ":allParam (b) :scanrelid 1} :righttree {HASH :startup_cost 67603.44 :total_cost 67603.44 :plan_rows 1387640 "
	    ":plan_width 21 :parallel_aware false :parallel_safe false :plan_node_id 3 :targetlist ({TARGETENTRY :expr "
	    "{VAR :varno 65001 :varattno 1 :vartype 25 :vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 2 :varoattno "
	    "2 :location -1} :resno 1 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} "
	    "{TARGETENTRY :expr {VAR :varno 65001 :varattno 2 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 "
	    ":varnoold 2 :varoattno 1 :location -1} :resno 2 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 "
	    ":resjunk false}) :qual <> :lefttree {SEQSCAN :startup_cost 0.00 :total_cost 67603.44 :plan_rows 1387640 "
	    ":plan_width 21 :parallel_aware false :parallel_safe false :plan_node_id 4 :targetlist ({TARGETENTRY :expr "
	    "{VAR :varno 2 :varattno 2 :vartype 25 :vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 2 :varoattno 2 "
	    ":location 11} :resno 1 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} "
	    "{TARGETENTRY :expr {VAR :varno 2 :varattno 1 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold "
	    "2 :varoattno 1 :location 147} :resno 2 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk "
	    "false}) :qual ({OPEXPR :opno 521 :opfuncid 147 :opresulttype 16 :opretset false :opcollid 0 :inputcollid 0 "
	    ":args ({VAR :varno 2 :varattno 5 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 2 :varoattno "
	    "5 :location 112} {CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true "
	    ":constisnull false :location 136 :constvalue 4 [ -48 7 0 0 0 0 0 0 ]}) :location 134}) :lefttree <> "
	    ":righttree <> :initPlan <> :extParam (b) :allParam (b) :scanrelid 2} :righttree <> :initPlan <> :extParam (b) "
	    ":allParam (b) :hashkeys ({VAR :varno 65001 :varattno 2 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 "
	    ":varnoold 2 :varoattno 1 :location 147}) :skewTable 136690 :skewColumn 2 :skewInherit false :rows_total 0} "
	    ":initPlan <> :extParam (b) :allParam (b) :jointype 0 :inner_unique true :joinqual <> :hashclauses ({OPEXPR "
	    ":opno 96 :opfuncid 65 :opresulttype 16 :opretset false :opcollid 0 :inputcollid 0 :args ({VAR :varno 65001 "
	    ":varattno 1 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 1 :varoattno 2 :location 158} "
	    "{VAR :varno 65000 :varattno 2 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 2 :varoattno 1 "
	    ":location 147}) :location -1}) :hashoperators (o 96) :hashcollations (o 0) :hashkeys ({VAR :varno 65001 "
	    ":varattno 1 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 1 :varoattno 2 :location 158})} "
	    ":righttree <> :initPlan <> :extParam (b) :allParam (b) :aggstrategy 0 :aggsplit 0 :numCols 0 :grpColIdx  "
	    ":grpOperators  :grpCollations  :numGroups 1 :aggParams (b) :groupingSets <> :chain <>} "
	    ":rtable ({RTE :alias "
	    "<> :eref {ALIAS :aliasname movie_keyword :colnames (\"id\" \"movie_id\" \"keyword_id\")} :rtekind 0 :relid "
	    "136690 :relkind r :rellockmode 1 :tablesample <> :lateral false :inh false :inFromCl true :requiredPerms 2 "
	    ":checkAsUser 0 :selectedCols (b 9) :insertedCols (b) :updatedCols (b) :extraUpdatedCols (b) :securityQuals "
	    "<>} {RTE :alias <> :eref {ALIAS :aliasname title :colnames (\"id\" \"title\" \"imdb_index\" \"kind_id\" "
	    "\"production_year\" \"imdb_id\" \"phonetic_code\" \"episode_of_id\" \"season_nr\" \"episode_nr\" "
	    "\"series_years\" \"md5sum\")} :rtekind 0 :relid 136721 :relkind r :rellockmode 1 :tablesample <> :lateral "
	    "false :inh false :inFromCl true :requiredPerms 2 :checkAsUser 0 :selectedCols (b 8 9 12) :insertedCols (b) "
	    ":updatedCols (b) :extraUpdatedCols (b) :securityQuals <>}) :resultRelations <> :rootResultRelations <> "
	    ":subplans <> :rewindPlanIDs (b) :rowMarks <> :relationOids (o 136690 136721) :invalItems <> :paramExecTypes "
	    "<> :utilityStmt <> :stmt_location 0 :stmt_len 180}";
	PlanReader plan_reader;
	unique_ptr<SimplestNode> postgres_plan = plan_reader.StringToNode(node_str);
	D_ASSERT(AggregateNode == postgres_plan->GetNodeType());
	unique_ptr<SimplestStmt> postgres_stmt = unique_ptr_cast<SimplestNode, SimplestStmt>(std::move(postgres_plan));
	// add table/column name from plan_reader.table_col_names
	AddTableColumnName(postgres_stmt, plan_reader.table_col_names);
#ifdef DEBUG
	postgres_stmt->Print();
#endif
	auto postgres_plan_pointer = postgres_stmt.get();

	// get the table map
	unordered_map<std::string, unique_ptr<LogicalGet>> table_map = GetTableMap(duckdb_plan);

	// get the parent node of JOIN/CROSS_PRODUCT
	auto new_plan = duckdb_plan.get();
	do {
#ifdef DEBUG
		D_ASSERT(new_plan->children.size() >= 1);
		if (LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY == new_plan->type) {
			// todo: check we have the same information in postgres and duckdb
		}
#endif
		new_plan = new_plan->children[0].get();
	} while (LogicalOperatorType::LOGICAL_COMPARISON_JOIN != new_plan->children[0]->type &&
	         LogicalOperatorType::LOGICAL_CROSS_PRODUCT != new_plan->children[0]->type);

	// get the JOIN from postgres
	while (JoinNode != postgres_plan_pointer->GetNodeType()) {
#ifdef DEBUG
		D_ASSERT(postgres_plan_pointer->children.size() >= 1);
#endif
		postgres_plan_pointer = postgres_plan_pointer->children[0].get();
	};

	unordered_map<int, int> pg_duckdb_table_idx = MatchTableIndex(table_map, plan_reader.table_col_names);

	new_plan->children.clear();
	// construct plan from postgres
	auto new_duckdb_plan = ConstructPlan(new_plan, postgres_plan_pointer, table_map, pg_duckdb_table_idx);
	new_plan->AddChild(std::move(new_duckdb_plan));

#ifdef DEBUG
	Printer::Print("new duckdb plan");
	duckdb_plan->Print();
#endif

	return duckdb_plan;
}

unordered_map<std::string, unique_ptr<LogicalGet>> IRConverter::GetTableMap(unique_ptr<LogicalOperator> &duckdb_plan) {
	unordered_map<std::string, unique_ptr<LogicalGet>> table_map;

	std::function<void(unique_ptr<LogicalOperator> & duckdb_plan)> iterate_plan;
	iterate_plan = [&table_map, &iterate_plan](unique_ptr<LogicalOperator> &duckdb_plan) {
		for (auto &child : duckdb_plan->children) {
			if (LogicalOperatorType::LOGICAL_GET == child->type) {
				auto get = unique_ptr_cast<LogicalOperator, LogicalGet>(std::move(child));
				std::string table_name = get->function.to_string(get->bind_data.get());
				table_map.emplace(table_name, std::move(get));
			} else {
				iterate_plan(child);
			}
		}
	};

	iterate_plan(duckdb_plan);

	return table_map;
}

unique_ptr<LogicalOperator> IRConverter::ConstructPlan(LogicalOperator *new_plan, SimplestStmt *postgres_plan_pointer,
                                                       unordered_map<std::string, unique_ptr<LogicalGet>> &table_map,
                                                       const unordered_map<int, int> &pg_duckdb_table_idx) {
	// test
	std::vector<std::string> test_table_name_vec = {"movie_keyword", "title"};
	int test_table_name_idx = 0;

	std::function<unique_ptr<LogicalOperator>(LogicalOperator * new_plan, SimplestStmt * postgres_plan_pointer)>
	    iterate_plan;
	iterate_plan = [&iterate_plan, &table_map, test_table_name_vec, &test_table_name_idx, pg_duckdb_table_idx, this](
	                   LogicalOperator *new_plan, SimplestStmt *postgres_plan_pointer) -> unique_ptr<LogicalOperator> {
		unique_ptr<LogicalOperator> left_child, right_child;
		if (postgres_plan_pointer->children.size() > 0) {
			left_child = iterate_plan(new_plan, postgres_plan_pointer->children[0].get());
			if (postgres_plan_pointer->children.size() == 2)
				right_child = iterate_plan(new_plan, postgres_plan_pointer->children[1].get());
		}
		switch (postgres_plan_pointer->GetNodeType()) {
		case JoinNode: {
			// get info from postgres_join and construct duckdb_join
			auto postgres_join = dynamic_cast<SimplestJoin *>(postgres_plan_pointer);
			auto duckdb_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
			duckdb_join->children.push_back(std::move(left_child));
			duckdb_join->children.push_back(std::move(right_child));
			JoinCondition cond;
			for (const auto &postgres_cond : postgres_join->join_conditions) {
				auto comp_op = postgres_cond->GetSimplestComparisonType();
				cond.comparison = ConvertCompType(comp_op);
				auto &left_pg_cond = postgres_cond->left_attr;
				LogicalType left_type = ConvertVarType(left_pg_cond->GetType());
				auto left_index_find = pg_duckdb_table_idx.find(left_pg_cond->GetTableIndex());
#ifdef DEBUG
				D_ASSERT(left_index_find != pg_duckdb_table_idx.end());
#endif
				cond.left = make_uniq<BoundColumnRefExpression>(
				    left_pg_cond->GetColumnName(), left_type,
				    ColumnBinding(left_index_find->second, left_pg_cond->GetColumnIndex()));
				auto &right_pg_cond = postgres_cond->right_attr;
				LogicalType right_type = ConvertVarType(right_pg_cond->GetType());
				auto right_index_find = pg_duckdb_table_idx.find(right_pg_cond->GetTableIndex());
#ifdef DEBUG
				D_ASSERT(right_index_find != pg_duckdb_table_idx.end());
#endif
				cond.right = make_uniq<BoundColumnRefExpression>(
				    right_pg_cond->GetColumnName(), right_type,
				    ColumnBinding(right_index_find->second, right_pg_cond->GetColumnIndex()));
				duckdb_join->conditions.push_back(std::move(cond));
			}
			return unique_ptr_cast<LogicalComparisonJoin, LogicalOperator>(std::move(duckdb_join));
		}
		case FilterNode:
			Printer::Print("Doesn't support yet!");
			return unique_ptr<LogicalOperator>(static_cast<LogicalOperator *>(new_plan));
		case HashNode:
			// todo: check if HashNode really doesn't have extra info
			return left_child;
		case ScanNode: {
			// get scan node from table_map
			auto duckdb_scan = std::move(table_map[test_table_name_vec[test_table_name_idx]]);
			test_table_name_idx++;
			return unique_ptr_cast<LogicalGet, LogicalOperator>(std::move(duckdb_scan));
		}
		default:
			return unique_ptr<LogicalOperator>();
		}
	};

	auto new_duckdb_plan = iterate_plan(new_plan, postgres_plan_pointer);
#ifdef DEBUG
	Printer::Print("new_duckdb_plan:");
	new_duckdb_plan->Print();
#endif

	return new_duckdb_plan;
}

ExpressionType IRConverter::ConvertCompType(SimplestComparisonType type) {
	switch (type) {
	case Equal:
		return ExpressionType::COMPARE_EQUAL;
	case LessThan:
	case GreaterThan:
	case LessEqual:
	case GreaterEqual:
	case Not:
		Printer::Print("Not supported yet!");
		return ExpressionType::INVALID;
	default:
		Printer::Print("Invalid postgres comparison type!");
		return ExpressionType::INVALID;
	}
}

LogicalType IRConverter::ConvertVarType(SimplestVarType type) {
	switch (type) {
	case IntVar:
		return LogicalType(LogicalTypeId::INTEGER);
	case FloatVar:
	case StringVar:
		Printer::Print("Not supported yet!");
		return LogicalType(LogicalTypeId::INVALID);
	default:
		Printer::Print("Invalid postgres var type!");
		return LogicalType(LogicalTypeId::INVALID);
	}
}

void IRConverter::SetAttrVecName(std::vector<unique_ptr<SimplestAttr>> &attr_vec,
                                 const std::deque<table_str> &table_col_names) {
	for (auto &attr_var_node : attr_vec) {
		auto col_index = attr_var_node->GetColumnIndex();
#ifdef DEBUG
		D_ASSERT(table_col_names[attr_var_node->GetTableIndex() - 1].size() == 1);
#endif
		auto col_name =
		    table_col_names[attr_var_node->GetTableIndex() - 1].begin()->second[col_index - 1]->GetLiteralValue();
		attr_var_node->SetColumnName(col_name);
	}
}

void IRConverter::SetCompExprName(std::vector<unique_ptr<SimplestVarConstComparison>> &comp_vec,
                                  const std::deque<table_str> &table_col_names) {
	for (auto &comp : comp_vec) {
		auto &comp_attr = comp->attr;
		auto col_index = comp_attr->GetColumnIndex();
#ifdef DEBUG
		D_ASSERT(table_col_names[comp_attr->GetTableIndex() - 1].size() == 1);
#endif
		auto col_name =
		    table_col_names[comp_attr->GetTableIndex() - 1].begin()->second[col_index - 1]->GetLiteralValue();
		comp_attr->SetColumnName(col_name);
	}
}

void IRConverter::SetCompExprName(std::vector<unique_ptr<SimplestVarComparison>> &comp_vec,
                                  const std::deque<table_str> &table_col_names) {
	for (auto &comp : comp_vec) {
		auto &left_attr = comp->left_attr;
		auto left_col_index = left_attr->GetColumnIndex();
#ifdef DEBUG
		D_ASSERT(table_col_names[left_attr->GetTableIndex() - 1].size() == 1);
#endif
		auto left_col_name =
		    table_col_names[left_attr->GetTableIndex() - 1].begin()->second[left_col_index - 1]->GetLiteralValue();
		left_attr->SetColumnName(left_col_name);

		auto &right_attr = comp->right_attr;
		auto right_col_index = right_attr->GetColumnIndex();
#ifdef DEBUG
		D_ASSERT(table_col_names[right_attr->GetTableIndex() - 1].size() == 1);
#endif
		auto right_col_name =
		    table_col_names[right_attr->GetTableIndex() - 1].begin()->second[right_col_index - 1]->GetLiteralValue();
		right_attr->SetColumnName(right_col_name);
	}
}

unordered_map<int, int>
IRConverter::MatchTableIndex(const unordered_map<std::string, unique_ptr<LogicalGet>> &table_map,
                             const std::deque<table_str> &table_col_names) {
	unordered_map<int, int> pg_duckdb_table_mapping;
	for (size_t i = 0; i < table_col_names.size(); i++) {
		auto &table_col = std::move(table_col_names[i]);
#ifdef DEBUG
		D_ASSERT(table_col.size() == 1);
#endif
		auto find_table = table_map.find(table_col.begin()->first);
		if (find_table != table_map.end()) {
			pg_duckdb_table_mapping[i + 1] = find_table->second->table_index;
		} else {
			Printer::Print("Error! Couldn't find table \"" + table_col.begin()->first + "\" in duckdb plan");
		}
	}

	return pg_duckdb_table_mapping;
}

void IRConverter::AddTableColumnName(unique_ptr<SimplestStmt> &postgres_plan,
                                     const std::deque<table_str> &table_col_names) {
	std::function<void(unique_ptr<SimplestStmt> & postgres_plan)> iterate_plan;
	iterate_plan = [&table_col_names, &iterate_plan, this](unique_ptr<SimplestStmt> &postgres_plan) {
		// set col attr names in `target_list`
		SetAttrVecName(postgres_plan->target_list, table_col_names);
		SetCompExprName(postgres_plan->qual_vec, table_col_names);

		if (ScanNode == postgres_plan->GetNodeType()) {
			// set scan_node's table_name
			auto &scan_node = postgres_plan->Cast<SimplestScan>();
#ifdef DEBUG
			D_ASSERT(table_col_names[scan_node.GetTableIndex() - 1].size() == 1);
#endif
			auto table_name = table_col_names[scan_node.GetTableIndex() - 1].begin()->first;
			scan_node.SetTableName(table_name);
		} else if (HashNode == postgres_plan->GetNodeType()) {
			// hash_node's `hash_keys` has attr
			auto &hash_node = postgres_plan->Cast<SimplestHash>();
			SetAttrVecName(hash_node.hash_keys, table_col_names);
		} else if (JoinNode == postgres_plan->GetNodeType()) {
			// join condition has attr
			auto &join_node = postgres_plan->Cast<SimplestJoin>();
			SetCompExprName(join_node.join_conditions, table_col_names);
		} else if (FilterNode == postgres_plan->GetNodeType()) {
			// filter condition has attr
			auto &filter_node = postgres_plan->Cast<SimplestFilter>();
			SetCompExprName(filter_node.filter_conditions, table_col_names);
		}

		for (auto &child : postgres_plan->children) {
			iterate_plan(child);
		}
	};

	iterate_plan(postgres_plan);
}
} // namespace duckdb