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
	    ":transientPlan false :dependsOnRole false :parallelModeNeeded false :jitFlags 31 :planTree {AGG :startup_cost "
	    "909915.24 :total_cost 909915.25 :plan_rows 1 :plan_width 96 :parallel_aware false :parallel_safe false "
	    ":plan_node_id 0 :targetlist ({TARGETENTRY :expr {AGGREF :aggfnoid 2145 :aggtype 25 :aggcollid 100 "
	    ":inputcollid 100 :aggtranstype 25 :aggargtypes (o 25) :aggdirectargs <> :args ({TARGETENTRY :expr {VAR :varno "
	    "65001 :varattno 1 :vartype 25 :vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 2 :varoattno 2 :location "
	    "11} :resno 1 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false}) :aggorder <> "
	    ":aggdistinct <> :aggfilter <> :aggstar false :aggvariadic false :aggkind n :agglevelsup 0 :aggsplit 0 "
	    ":location 7} :resno 1 :resname movie_keyword :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} "
	    "{TARGETENTRY :expr {AGGREF :aggfnoid 2145 :aggtype 25 :aggcollid 100 :inputcollid 100 :aggtranstype 25 "
	    ":aggargtypes (o 25) :aggdirectargs <> :args ({TARGETENTRY :expr {VAR :varno 65001 :varattno 2 :vartype 25 "
	    ":vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 4 :varoattno 2 :location 57} :resno 1 :resname <> "
	    ":ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false}) :aggorder <> :aggdistinct <> :aggfilter <> "
	    ":aggstar false :aggvariadic false :aggkind n :agglevelsup 0 :aggsplit 0 :location 53} :resno 2 :resname "
	    "actor_name :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {AGGREF "
	    ":aggfnoid 2145 :aggtype 25 :aggcollid 100 :inputcollid 100 :aggtranstype 25 :aggargtypes (o 25) "
	    ":aggdirectargs <> :args ({TARGETENTRY :expr {VAR :varno 65001 :varattno 3 :vartype 25 :vartypmod -1 "
	    ":varcollid 100 :varlevelsup 0 :varnoold 5 :varoattno 2 :location 94} :resno 1 :resname <> :ressortgroupref 0 "
	    ":resorigtbl 0 :resorigcol 0 :resjunk false}) :aggorder <> :aggdistinct <> :aggfilter <> :aggstar false "
	    ":aggvariadic false :aggkind n :agglevelsup 0 :aggsplit 0 :location 90} :resno 3 :resname hero_movie "
	    ":ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false}) :qual <> :lefttree {NESTLOOP :startup_cost "
	    "819069.38 :total_cost 909845.45 :plan_rows 9305 :plan_width 48 :parallel_aware false :parallel_safe false "
	    ":plan_node_id 1 :targetlist ({TARGETENTRY :expr {VAR :varno 65000 :varattno 2 :vartype 25 :vartypmod -1 "
	    ":varcollid 100 :varlevelsup 0 :varnoold 2 :varoattno 2 :location 11} :resno 1 :resname <> :ressortgroupref 0 "
	    ":resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR :varno 65001 :varattno 2 :vartype 25 "
	    ":vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 4 :varoattno 2 :location 57} :resno 2 :resname <> "
	    ":ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR :varno 65001 :varattno "
	    "3 :vartype 25 :vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 5 :varoattno 2 :location 94} :resno 3 "
	    ":resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false}) :qual <> :lefttree {HASHJOIN "
	    ":startup_cost 819068.96 :total_cost 905762.57 :plan_rows 9305 :plan_width 36 :parallel_aware false "
	    ":parallel_safe false :plan_node_id 2 :targetlist ({TARGETENTRY :expr {VAR :varno 65001 :varattno 3 :vartype "
	    "23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 3 :varoattno 3 :location 286} :resno 1 :resname <> "
	    ":ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR :varno 65000 :varattno "
	    "2 :vartype 25 :vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 4 :varoattno 2 :location 57} :resno 2 "
	    ":resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR :varno "
	    "65000 :varattno 3 :vartype 25 :vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 5 :varoattno 2 :location "
	    "94} :resno 3 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false}) :qual <> :lefttree "
	    "{SEQSCAN :startup_cost 0.00 :total_cost 69693.30 :plan_rows 4523930 :plan_width 8 :parallel_aware false "
	    ":parallel_safe false :plan_node_id 3 :targetlist ({TARGETENTRY :expr {VAR :varno 3 :varattno 1 :vartype 23 "
	    ":vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 3 :varoattno 1 :location -1} :resno 1 :resname <> "
	    ":ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR :varno 3 :varattno 2 "
	    ":vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 3 :varoattno 2 :location -1} :resno 2 "
	    ":resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR :varno 3 "
	    ":varattno 3 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 3 :varoattno 3 :location -1} "
	    ":resno 3 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false}) :qual <> :lefttree <> "
	    ":righttree <> :initPlan <> :extParam (b) :allParam (b) :scanrelid 3} :righttree {HASH :startup_cost 819044.11 "
	    ":total_cost 819044.11 :plan_rows 1988 :plan_width 40 :parallel_aware false :parallel_safe false :plan_node_id "
	    "4 :targetlist ({TARGETENTRY :expr {VAR :varno 65001 :varattno 1 :vartype 23 :vartypmod -1 :varcollid 0 "
	    ":varlevelsup 0 :varnoold 1 :varoattno 3 :location -1} :resno 1 :resname <> :ressortgroupref 0 :resorigtbl 0 "
	    ":resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR :varno 65001 :varattno 2 :vartype 25 :vartypmod -1 "
	    ":varcollid 100 :varlevelsup 0 :varnoold 4 :varoattno 2 :location -1} :resno 2 :resname <> :ressortgroupref 0 "
	    ":resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR :varno 65001 :varattno 3 :vartype 25 "
	    ":vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 5 :varoattno 2 :location -1} :resno 3 :resname <> "
	    ":ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR :varno 65001 :varattno "
	    "4 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 5 :varoattno 1 :location -1} :resno 4 "
	    ":resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false}) :qual <> :lefttree {NESTLOOP "
	    ":startup_cost 107713.82 :total_cost 819044.11 :plan_rows 1988 :plan_width 40 :parallel_aware false "
	    ":parallel_safe false :plan_node_id 5 :targetlist ({TARGETENTRY :expr {VAR :varno 65001 :varattno 1 :vartype "
	    "23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 1 :varoattno 3 :location 368} :resno 1 :resname <> "
	    ":ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR :varno 65001 :varattno "
	    "2 :vartype 25 :vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 4 :varoattno 2 :location 57} :resno 2 "
	    ":resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR :varno "
	    "65000 :varattno 2 :vartype 25 :vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 5 :varoattno 2 :location "
	    "94} :resno 3 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr "
	    "{VAR :varno 65000 :varattno 1 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 5 :varoattno 1 "
	    ":location 317} :resno 4 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false}) :qual <> "
	    ":lefttree {HASHJOIN :startup_cost 107713.39 :total_cost 817396.20 :plan_rows 3622 :plan_width 19 "
	    ":parallel_aware false :parallel_safe false :plan_node_id 6 :targetlist ({TARGETENTRY :expr {VAR :varno 65001 "
	    ":varattno 3 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 1 :varoattno 3 :location 368} "
	    ":resno 1 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR "
	    ":varno 65000 :varattno 1 :vartype 25 :vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 4 :varoattno 2 "
	    ":location 57} :resno 2 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false}) :qual <> "
	    ":lefttree {SEQSCAN :startup_cost 0.00 :total_cost 614663.80 :plan_rows 36197680 :plan_width 8 :parallel_aware "
	    "false :parallel_safe false :plan_node_id 7 :targetlist ({TARGETENTRY :expr {VAR :varno 1 :varattno 1 :vartype "
	    "23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 1 :varoattno 1 :location -1} :resno 1 :resname <> "
	    ":ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR :varno 1 :varattno 2 "
	    ":vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 1 :varoattno 2 :location -1} :resno 2 "
	    ":resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR :varno 1 "
	    ":varattno 3 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 1 :varoattno 3 :location -1} "
	    ":resno 3 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR "
	    ":varno 1 :varattno 4 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 1 :varoattno 4 :location "
	    "-1} :resno 4 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr "
	    "{VAR :varno 1 :varattno 5 :vartype 25 :vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 1 :varoattno 5 "
	    ":location -1} :resno 5 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} "
	    "{TARGETENTRY :expr {VAR :varno 1 :varattno 6 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold "
	    "1 :varoattno 6 :location -1} :resno 6 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk "
	    "false} {TARGETENTRY :expr {VAR :varno 1 :varattno 7 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 "
	    ":varnoold 1 :varoattno 7 :location -1} :resno 7 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 "
	    ":resjunk false}) :qual <> :lefttree <> :righttree <> :initPlan <> :extParam (b) :allParam (b) :scanrelid 1} "
	    ":righttree {HASH :startup_cost 107708.18 :total_cost 107708.18 :plan_rows 417 :plan_width 19 :parallel_aware "
	    "false :parallel_safe false :plan_node_id 8 :targetlist ({TARGETENTRY :expr {VAR :varno 65001 :varattno 1 "
	    ":vartype 25 :vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 4 :varoattno 2 :location -1} :resno 1 "
	    ":resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR :varno "
	    "65001 :varattno 2 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 4 :varoattno 1 :location "
	    "-1} :resno 2 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false}) :qual <> :lefttree "
	    "{SEQSCAN :startup_cost 0.00 :total_cost 107708.18 :plan_rows 417 :plan_width 19 :parallel_aware false "
	    ":parallel_safe false :plan_node_id 9 :targetlist ({TARGETENTRY :expr {VAR :varno 4 :varattno 2 :vartype 25 "
	    ":vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 4 :varoattno 2 :location 57} :resno 1 :resname <> "
	    ":ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR :varno 4 :varattno 1 "
	    ":vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 4 :varoattno 1 :location 443} :resno 2 "
	    ":resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false}) :qual ({OPEXPR :opno 1209 "
	    ":opfuncid 850 :opresulttype 16 :opretset false :opcollid 0 :inputcollid 100 :args ({VAR :varno 4 :varattno 2 "
	    ":vartype 25 :vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 4 :varoattno 2 :location 199} {CONST "
	    ":consttype 25 :consttypmod -1 :constcollid 100 :constlen -1 :constbyval false :constisnull false :location "
	    "214 :constvalue 19 [ 76 0 0 0 37 68 111 119 110 101 121 37 82 111 98 101 114 116 37 ]}) :location 209}) "
	    ":lefttree <> :righttree <> :initPlan <> :extParam (b) :allParam (b) :scanrelid 4} :righttree <> :initPlan <> "
	    ":extParam (b) :allParam (b) :hashkeys ({VAR :varno 65001 :varattno 2 :vartype 23 :vartypmod -1 :varcollid 0 "
	    ":varlevelsup 0 :varnoold 4 :varoattno 1 :location 443}) :skewTable 136604 :skewColumn 2 :skewInherit false "
	    ":rows_total 0} :initPlan <> :extParam (b) :allParam (b) :jointype 0 :inner_unique true :joinqual <> "
	    ":hashclauses ({OPEXPR :opno 96 :opfuncid 65 :opresulttype 16 :opretset false :opcollid 0 :inputcollid 0 :args "
	    "({VAR :varno 65001 :varattno 2 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 1 :varoattno 2 "
	    ":location 453} {VAR :varno 65000 :varattno 2 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold "
	    "4 :varoattno 1 :location 443}) :location -1}) :hashoperators (o 96) :hashcollations (o 0) :hashkeys ({VAR "
	    ":varno 65001 :varattno 2 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 1 :varoattno 2 "
	    ":location 453})} :righttree {INDEXSCAN :startup_cost 0.43 :total_cost 0.45 :plan_rows 1 :plan_width 21 "
	    ":parallel_aware false :parallel_safe false :plan_node_id 10 :targetlist ({TARGETENTRY :expr {VAR :varno 5 "
	    ":varattno 1 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 5 :varoattno 1 :location -1} "
	    ":resno 1 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR "
	    ":varno 5 :varattno 2 :vartype 25 :vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 5 :varoattno 2 "
	    ":location -1} :resno 2 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} "
	    "{TARGETENTRY :expr {VAR :varno 5 :varattno 3 :vartype 1043 :vartypmod 16 :varcollid 100 :varlevelsup 0 "
	    ":varnoold 5 :varoattno 3 :location -1} :resno 3 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 "
	    ":resjunk false} {TARGETENTRY :expr {VAR :varno 5 :varattno 4 :vartype 23 :vartypmod -1 :varcollid 0 "
	    ":varlevelsup 0 :varnoold 5 :varoattno 4 :location -1} :resno 4 :resname <> :ressortgroupref 0 :resorigtbl 0 "
	    ":resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR :varno 5 :varattno 5 :vartype 23 :vartypmod -1 "
	    ":varcollid 0 :varlevelsup 0 :varnoold 5 :varoattno 5 :location -1} :resno 5 :resname <> :ressortgroupref 0 "
	    ":resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR :varno 5 :varattno 6 :vartype 23 "
	    ":vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 5 :varoattno 6 :location -1} :resno 6 :resname <> "
	    ":ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR :varno 5 :varattno 7 "
	    ":vartype 1043 :vartypmod 9 :varcollid 100 :varlevelsup 0 :varnoold 5 :varoattno 7 :location -1} :resno 7 "
	    ":resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR :varno 5 "
	    ":varattno 8 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 5 :varoattno 8 :location -1} "
	    ":resno 8 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR "
	    ":varno 5 :varattno 9 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 5 :varoattno 9 :location "
	    "-1} :resno 9 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr "
	    "{VAR :varno 5 :varattno 10 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 5 :varoattno 10 "
	    ":location -1} :resno 10 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} "
	    "{TARGETENTRY :expr {VAR :varno 5 :varattno 11 :vartype 1043 :vartypmod 53 :varcollid 100 :varlevelsup 0 "
	    ":varnoold 5 :varoattno 11 :location -1} :resno 11 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 "
	    ":resjunk false} {TARGETENTRY :expr {VAR :varno 5 :varattno 12 :vartype 1043 :vartypmod 36 :varcollid 100 "
	    ":varlevelsup 0 :varnoold 5 :varoattno 12 :location -1} :resno 12 :resname <> :ressortgroupref 0 :resorigtbl 0 "
	    ":resorigcol 0 :resjunk false}) :qual ({OPEXPR :opno 521 :opfuncid 147 :opresulttype 16 :opretset false "
	    ":opcollid 0 :inputcollid 0 :args ({VAR :varno 5 :varattno 5 :vartype 23 :vartypmod -1 :varcollid 0 "
	    ":varlevelsup 0 :varnoold 5 :varoattno 5 :location 238} {CONST :consttype 23 :consttypmod -1 :constcollid 0 "
	    ":constlen 4 :constbyval true :constisnull false :location 262 :constvalue 4 [ -48 7 0 0 0 0 0 0 ]}) :location "
	    "260}) :lefttree <> :righttree <> :initPlan <> :extParam (b 0) :allParam (b 0) :scanrelid 5 :indexid 136727 "
	    ":indexqual ({OPEXPR :opno 96 :opfuncid 65 :opresulttype 16 :opretset false :opcollid 0 :inputcollid 0 :args "
	    "({VAR :varno 65002 :varattno 1 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 5 :varoattno 1 "
	    ":location 317} {PARAM :paramkind 1 :paramid 0 :paramtype 23 :paramtypmod -1 :paramcollid 0 :location 368}) "
	    ":location -1}) :indexqualorig ({OPEXPR :opno 96 :opfuncid 65 :opresulttype 16 :opretset false :opcollid 0 "
	    ":inputcollid 0 :args ({VAR :varno 5 :varattno 1 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 "
	    ":varnoold 5 :varoattno 1 :location 317} {PARAM :paramkind 1 :paramid 0 :paramtype 23 :paramtypmod -1 "
	    ":paramcollid 0 :location 368}) :location -1}) :indexorderby <> :indexorderbyorig <> :indexorderbyops <> "
	    ":indexorderdir 1} :initPlan <> :extParam (b) :allParam (b) :jointype 0 :inner_unique true :joinqual <> "
	    ":nestParams ({NESTLOOPPARAM :paramno 0 :paramval {VAR :varno 65001 :varattno 1 :vartype 23 :vartypmod -1 "
	    ":varcollid 0 :varlevelsup 0 :varnoold 1 :varoattno 3 :location 368}})} :righttree <> :initPlan <> :extParam "
	    "(b) :allParam (b) :hashkeys ({VAR :varno 65001 :varattno 4 :vartype 23 :vartypmod -1 :varcollid 0 "
	    ":varlevelsup 0 :varnoold 5 :varoattno 1 :location 317}) :skewTable 136690 :skewColumn 2 :skewInherit false "
	    ":rows_total 0} :initPlan <> :extParam (b) :allParam (b) :jointype 0 :inner_unique false :joinqual <> "
	    ":hashclauses ({OPEXPR :opno 96 :opfuncid 65 :opresulttype 16 :opretset false :opcollid 0 :inputcollid 0 :args "
	    "({VAR :varno 65001 :varattno 2 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 3 :varoattno 2 "
	    ":location 328} {VAR :varno 65000 :varattno 4 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold "
	    "5 :varoattno 1 :location 317}) :location -1}) :hashoperators (o 96) :hashcollations (o 0) :hashkeys ({VAR "
	    ":varno 65001 :varattno 2 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 3 :varoattno 2 "
	    ":location 328})} :righttree {INDEXSCAN :startup_cost 0.42 :total_cost 0.44 :plan_rows 1 :plan_width 20 "
	    ":parallel_aware false :parallel_safe false :plan_node_id 11 :targetlist ({TARGETENTRY :expr {VAR :varno 2 "
	    ":varattno 1 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 2 :varoattno 1 :location -1} "
	    ":resno 1 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} {TARGETENTRY :expr {VAR "
	    ":varno 2 :varattno 2 :vartype 25 :vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 2 :varoattno 2 "
	    ":location -1} :resno 2 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 :resjunk false} "
	    "{TARGETENTRY :expr {VAR :varno 2 :varattno 3 :vartype 1043 :vartypmod 9 :varcollid 100 :varlevelsup 0 "
	    ":varnoold 2 :varoattno 3 :location -1} :resno 3 :resname <> :ressortgroupref 0 :resorigtbl 0 :resorigcol 0 "
	    ":resjunk false}) :qual <> :lefttree <> :righttree <> :initPlan <> :extParam (b 1) :allParam (b 1) :scanrelid "
	    "2 :indexid 136654 :indexqual ({OPEXPR :opno 96 :opfuncid 65 :opresulttype 16 :opretset false :opcollid 0 "
	    ":inputcollid 0 :args ({VAR :varno 65002 :varattno 1 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 "
	    ":varnoold 2 :varoattno 1 :location 273} {PARAM :paramkind 1 :paramid 1 :paramtype 23 :paramtypmod -1 "
	    ":paramcollid 0 :location 286}) :location -1}) :indexqualorig ({OPEXPR :opno 96 :opfuncid 65 :opresulttype 16 "
	    ":opretset false :opcollid 0 :inputcollid 0 :args ({VAR :varno 2 :varattno 1 :vartype 23 :vartypmod -1 "
	    ":varcollid 0 :varlevelsup 0 :varnoold 2 :varoattno 1 :location 273} {PARAM :paramkind 1 :paramid 1 :paramtype "
	    "23 :paramtypmod -1 :paramcollid 0 :location 286}) :location -1}) :indexorderby <> :indexorderbyorig <> "
	    ":indexorderbyops <> :indexorderdir 1} :initPlan <> :extParam (b) :allParam (b) :jointype 0 :inner_unique true "
	    ":joinqual <> :nestParams ({NESTLOOPPARAM :paramno 1 :paramval {VAR :varno 65001 :varattno 1 :vartype 23 "
	    ":vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 3 :varoattno 3 :location 286}})} :righttree <> :initPlan "
	    "<> :extParam (b) :allParam (b) :aggstrategy 0 :aggsplit 0 :numCols 0 :grpColIdx  :grpOperators  "
	    ":grpCollations  :numGroups 1 :aggParams (b) :groupingSets <> :chain <>} :rtable ({RTE :alias <> :eref {ALIAS "
	    ":aliasname cast_info :colnames (\"id\" \"person_id\" \"movie_id\" \"person_role_id\" \"note\" \"nr_order\" "
	    "\"role_id\")} :rtekind 0 :relid 136604 :relkind r :rellockmode 1 :tablesample <> :lateral false :inh false "
	    ":inFromCl true :requiredPerms 2 :checkAsUser 0 :selectedCols (b 9 10) :insertedCols (b) :updatedCols (b) "
	    ":extraUpdatedCols (b) :securityQuals <>} {RTE :alias <> :eref {ALIAS :aliasname keyword :colnames (\"id\" "
	    "\"keyword\" \"phonetic_code\")} :rtekind 0 :relid 136648 :relkind r :rellockmode 1 :tablesample <> :lateral "
	    "false :inh false :inFromCl true :requiredPerms 2 :checkAsUser 0 :selectedCols (b 8 9) :insertedCols (b) "
	    ":updatedCols (b) :extraUpdatedCols (b) :securityQuals <>} {RTE :alias <> :eref {ALIAS :aliasname "
	    "movie_keyword :colnames (\"id\" \"movie_id\" \"keyword_id\")} :rtekind 0 :relid 136690 :relkind r "
	    ":rellockmode 1 :tablesample <> :lateral false :inh false :inFromCl true :requiredPerms 2 :checkAsUser 0 "
	    ":selectedCols (b 9 10) :insertedCols (b) :updatedCols (b) :extraUpdatedCols (b) :securityQuals <>} {RTE "
	    ":alias <> :eref {ALIAS :aliasname name :colnames (\"id\" \"name\" \"imdb_index\" \"imdb_id\" \"gender\" "
	    "\"name_pcode_cf\" \"name_pcode_nf\" \"surname_pcode\" \"md5sum\")} :rtekind 0 :relid 136700 :relkind r "
	    ":rellockmode 1 :tablesample <> :lateral false :inh false :inFromCl true :requiredPerms 2 :checkAsUser 0 "
	    ":selectedCols (b 8 9) :insertedCols (b) :updatedCols (b) :extraUpdatedCols (b) :securityQuals <>} {RTE :alias "
	    "<> :eref {ALIAS :aliasname title :colnames (\"id\" \"title\" \"imdb_index\" \"kind_id\" \"production_year\" "
	    "\"imdb_id\" \"phonetic_code\" \"episode_of_id\" \"season_nr\" \"episode_nr\" \"series_years\" \"md5sum\")} "
	    ":rtekind 0 :relid 136721 :relkind r :rellockmode 1 :tablesample <> :lateral false :inh false :inFromCl true "
	    ":requiredPerms 2 :checkAsUser 0 :selectedCols (b 8 9 12) :insertedCols (b) :updatedCols (b) :extraUpdatedCols "
	    "(b) :securityQuals <>}) :resultRelations <> :rootResultRelations <> :subplans <> :rewindPlanIDs (b) :rowMarks "
	    "<> :relationOids (o 136604 136648 136690 136700 136721) :invalItems <> :paramExecTypes (o 23 23) :utilityStmt "
	    "<> :stmt_location 0 :stmt_len 472}";
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

	// todo: It's better to generate the Filter Expression from postgres, but needs a lot of engineering work.
	//  Currently, we reuse the Filter Expression from duckdb
	std::vector<unique_ptr<Expression>> expr_vec = CollectFilterExpressions(duckdb_plan);

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

	std::unordered_map<int, int> pg_duckdb_table_idx = MatchTableIndex(table_map, plan_reader.table_col_names);

	new_plan->children.clear();
	// construct plan from postgres
	auto new_duckdb_plan = ConstructPlan(new_plan, postgres_plan_pointer, table_map, pg_duckdb_table_idx, expr_vec);
#ifdef DEBUG
	D_ASSERT(expr_vec.empty());
#endif
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

std::vector<unique_ptr<Expression>> IRConverter::CollectFilterExpressions(unique_ptr<LogicalOperator> &duckdb_plan) {
	std::vector<unique_ptr<Expression>> expr_vec;
	std::function<void(unique_ptr<LogicalOperator> & duckdb_plan)> iterate_plan;
	iterate_plan = [&expr_vec, &iterate_plan](unique_ptr<LogicalOperator> &duckdb_plan) {
		for (auto &child : duckdb_plan->children) {
			if (LogicalOperatorType::LOGICAL_FILTER == child->type) {
				auto &filter_node = child->Cast<LogicalFilter>();
				for (auto &expr : filter_node.expressions) {
					expr_vec.emplace_back(std::move(expr));
				}
			} else {
				iterate_plan(child);
			}
		}
	};

	iterate_plan(duckdb_plan);

	return expr_vec;
}

unique_ptr<LogicalOperator> IRConverter::ConstructPlan(LogicalOperator *new_plan, SimplestStmt *postgres_plan_pointer,
                                                       unordered_map<std::string, unique_ptr<LogicalGet>> &table_map,
                                                       const unordered_map<int, int> &pg_duckdb_table_idx,
                                                       std::vector<unique_ptr<Expression>> &expr_vec) {
	std::function<unique_ptr<LogicalOperator>(LogicalOperator * new_plan, SimplestStmt * postgres_plan_pointer)>
	    iterate_plan;
	iterate_plan = [&iterate_plan, &table_map, pg_duckdb_table_idx, &expr_vec, this](
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
				auto left_table_index = left_index_find->second;
				auto find_col_idx =
				    std::find(column_idx_mapping[left_table_index].begin(), column_idx_mapping[left_table_index].end(),
				              left_pg_cond->GetColumnIndex() - 1);
#ifdef DEBUG
				D_ASSERT(find_col_idx != column_idx_mapping[left_table_index].end());
#endif
				auto left_column_index = find_col_idx - column_idx_mapping[left_table_index].begin();
				cond.left = make_uniq<BoundColumnRefExpression>(left_pg_cond->GetColumnName(), left_type,
				                                                ColumnBinding(left_table_index, left_column_index));
				auto &right_pg_cond = postgres_cond->right_attr;
				LogicalType right_type = ConvertVarType(right_pg_cond->GetType());
				auto right_index_find = pg_duckdb_table_idx.find(right_pg_cond->GetTableIndex());
#ifdef DEBUG
				D_ASSERT(right_index_find != pg_duckdb_table_idx.end());
#endif
				auto right_table_index = right_index_find->second;
				find_col_idx =
				    std::find(column_idx_mapping[right_table_index].begin(),
				              column_idx_mapping[right_table_index].end(), right_pg_cond->GetColumnIndex() - 1);
#ifdef DEBUG
				D_ASSERT(find_col_idx != column_idx_mapping[right_table_index].end());
#endif
				auto right_column_index = find_col_idx - column_idx_mapping[right_table_index].begin();
				cond.right = make_uniq<BoundColumnRefExpression>(right_pg_cond->GetColumnName(), right_type,
				                                                 ColumnBinding(right_table_index, right_column_index));
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
			auto postgres_scan = dynamic_cast<SimplestScan *>(postgres_plan_pointer);
			// get scan node from table_map
			auto duckdb_scan = std::move(table_map[postgres_scan->GetTableName()]);
#ifdef DEBUG
			D_ASSERT(0 == column_idx_mapping.count(duckdb_scan->table_index));
#endif
			auto attr_table_idx = duckdb_scan->table_index;
			column_idx_mapping[attr_table_idx] = duckdb_scan->column_ids;
			unique_ptr<LogicalOperator> logical_get =
			    unique_ptr_cast<LogicalGet, LogicalOperator>(std::move(duckdb_scan));

			// check if it's necessary to add FILTER by the `qual_vec`
			vector<unique_ptr<Expression>> filter_expressions;
			for (const auto &qual : postgres_scan->qual_vec) {
				// currently we get filter expressions from duckdb plan
				// todo: construct filter expressions from postgres info
				for (auto it = expr_vec.begin(); it != expr_vec.end();) {
					auto &expr = *it;
					bool find_filter_expr = false;
					if (ExpressionClass::BOUND_BETWEEN == expr->GetExpressionClass()) {
						auto &bound_between_expr = expr->Cast<BoundBetweenExpression>();
						auto &expr_input = bound_between_expr.input;
						if (ExpressionType::BOUND_COLUMN_REF == expr_input->GetExpressionType()) {
							auto &input_ref = expr_input->Cast<BoundColumnRefExpression>();
							if (input_ref.binding.table_index == attr_table_idx) {
								filter_expressions.emplace_back(std::move(expr));
								it = expr_vec.erase(it);
								find_filter_expr = true;
							}
						}
					} else if (ExpressionClass::BOUND_FUNCTION == expr->GetExpressionClass()) {
						auto &bound_func_expr = expr->Cast<BoundFunctionExpression>();
						for (const auto &child : bound_func_expr.children) {
							if (ExpressionType::BOUND_COLUMN_REF == child->GetExpressionType()) {
								auto &child_ref = child->Cast<BoundColumnRefExpression>();
								if (child_ref.binding.table_index == attr_table_idx) {
									filter_expressions.emplace_back(std::move(expr));
									it = expr_vec.erase(it);
									find_filter_expr = true;
								}
							}
						}
					}
					if (!find_filter_expr)
						it++;
				}
			}
			if (!filter_expressions.empty()) {
				auto scan_filter = make_uniq<LogicalFilter>();
				scan_filter->expressions = std::move(filter_expressions);
				scan_filter->AddChild(std::move(logical_get));
				return unique_ptr_cast<LogicalFilter, LogicalOperator>(std::move(scan_filter));
			} else {
				return logical_get;
			}
		}
		default:
			return unique_ptr<LogicalOperator>();
		}
	};

	auto new_duckdb_plan = iterate_plan(new_plan, postgres_plan_pointer);

	return new_duckdb_plan;
}

ExpressionType IRConverter::ConvertCompType(SimplestComparisonType type) {
	switch (type) {
	case Equal:
		return ExpressionType::COMPARE_EQUAL;
	case LessThan:
		return ExpressionType::COMPARE_LESSTHAN;
	case GreaterThan:
		return ExpressionType::COMPARE_GREATERTHAN;
	case LessEqual:
		return ExpressionType::COMPARE_LESSTHANOREQUALTO;
	case GreaterEqual:
		return ExpressionType::COMPARE_GREATERTHANOREQUALTO;
	case Not:
		return ExpressionType::COMPARE_NOTEQUAL;
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
		return LogicalType(LogicalTypeId::FLOAT);
	case StringVar:
		return LogicalType(LogicalTypeId::STRING_LITERAL);
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