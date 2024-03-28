//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/query_split.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/printer.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

//! The QuerySplit optimizer follows the algorithm in https://dl.acm.org/doi/10.1145/3589330 .
//! It first splits the long query into a set of subqueries and build a DAG. Then it selects
//! the lowest-cost subquery from the DAG, optimize it and execute it to get and update the
//! cardinality.
class QuerySplit {
public:
	QuerySplit() {}
	~QuerySplit() {}
	//! Perform Query Split
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

private:
	unique_ptr<LogicalOperator> RemoveDedundantJoin(unique_ptr<LogicalOperator> original_plan);
	//! The range table is a list of relations that are used in the query.
	//! In a SELECT statement these are the relations given after the FROM key word.
	uint64_t CollectRangeTableLength(const unique_ptr<LogicalOperator> &plan);
	//! Split parent query by foreign key
	unique_ptr<LogicalOperator> Recon(unique_ptr<LogicalOperator> original_plan, uint64_t join_column_pairs);

private:
	enum EnumSplitAlgorithm {
		foreign_key_center = 1,
		min_sub_query
	};
	EnumSplitAlgorithm split_algorithm = foreign_key_center;
};

} // namespace duckdb
