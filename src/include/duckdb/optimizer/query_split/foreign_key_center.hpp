//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/foreign_key_center.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/optimizer/query_split/split_algorithm.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/constraints/bound_foreign_key_constraint.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include <queue>

namespace duckdb {

using fk_map = std::unordered_map<ColumnBinding, std::pair<ColumnDefinition, bool>, ColumnBinding::ColumnBindingHash>;

//! The ForeignKeyCenterSplit optimizer follows the algorithm in https://dl.acm.org/doi/10.1145/3589330 .
//! It first splits the long query into a set of subqueries and build a DAG. Then it selects
//! the lowest-cost subquery from the DAG, optimize it and execute it to get and update the
//! cardinality.
class ForeignKeyCenterSplit : public SplitAlgorithm {
public:
	explicit ForeignKeyCenterSplit(ClientContext &context, EnumSplitAlgorithm split_algorithm)
	    : SplitAlgorithm(context), split_algorithm(split_algorithm) {};
	~ForeignKeyCenterSplit() override = default;
	//! Perform Query Split
	unique_ptr<LogicalOperator> Split(unique_ptr<LogicalOperator> plan, bool &subquery_loop) override;

protected:
	void VisitOperator(LogicalOperator &op) override;

	// delete the invalid expressions and invalid the operators without expressions

	void VisitProjection(LogicalProjection &op);
	void VisitAggregate(LogicalAggregate &op);
	void VisitComparisonJoin(LogicalComparisonJoin &op);
	void VisitFilter(LogicalFilter &op);
	void VisitGet(LogicalGet &op);
	void VisitColumnDataGet(LogicalColumnDataGet &op);

	// invalid/delete the expressions without target tables

	unique_ptr<Expression> VisitReplace(BoundAggregateExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	unique_ptr<Expression> VisitReplace(BoundFunctionExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;

private:
	unique_ptr<LogicalOperator> RemoveRedundantJoin(unique_ptr<LogicalOperator> original_plan);
	//! The range table is a list of relations that are used in the query.
	//! In a SELECT statement these are the relations given after the FROM key word.
	uint64_t CollectRangeTableLength(const unique_ptr<LogicalOperator> &plan);
	//! Split parent query by foreign key
	unique_ptr<LogicalOperator> Recon(unique_ptr<LogicalOperator> original_plan);
	//! Check join operations to get the join relations
	void CheckJoin(std::vector<std::pair<ColumnBinding, ColumnBinding>> &join_column_pairs, const LogicalOperator &op);
	//! Check set operations to get the tables and columns
	void CheckSet(fk_map &foreign_key_represent, std::unordered_map<idx_t, TableCatalogEntry *> &used_table_entries,
	              const LogicalOperator &op,
	              const std::vector<std::pair<ColumnBinding, ColumnBinding>> &join_column_pairs);

private:
	//! For the current subquery, we only keep nodes related with the target_tables
	std::unordered_map<idx_t, TableCatalogEntry *> target_tables;

	EnumSplitAlgorithm split_algorithm;
};

} // namespace duckdb