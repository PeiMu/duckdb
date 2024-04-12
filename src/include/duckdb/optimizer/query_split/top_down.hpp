//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/top_down.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/optimizer/query_split/split_algorithm.hpp"

#include <queue>

namespace duckdb {

struct TableExprPair {
	idx_t table_idx;
	idx_t expression_idx;
	std::string column_name;
};

class ProjSelector : public LogicalOperatorVisitor {
public:
	explicit ProjSelector() = default;
	~ProjSelector() = default;

	void VisitOperator(LogicalOperator &op) override;
	//! get the <table_index, expression_index> pair by checking which column is used in the projection
	std::vector<TableExprPair> GetProjExprIndexPair(const LogicalProjection &proj_op);

private:
	void VisitGet(LogicalGet &op);

private:
	std::unordered_map<idx_t, TableCatalogEntry *> target_tables;
};

//! Based on the DAG of the logical plan, we generate the subqueries bottom-up
class TopDownSplit : public SplitAlgorithm {
public:
	explicit TopDownSplit(ClientContext &context) : SplitAlgorithm(context) {};
	~TopDownSplit() override = default;
	//! Perform Query Split
	unique_ptr<LogicalOperator> Split(unique_ptr<LogicalOperator> plan, bool &subquery_loop) override;

protected:
	//! Extract the subquery in the top-down order, and insert
	//! the operations of the same level to `subqueries`
	void VisitOperator(LogicalOperator &op) override;

private:
	bool filter_parent = false;
	std::queue<std::vector<LogicalOperator *>> subqueries;
};

} // namespace duckdb