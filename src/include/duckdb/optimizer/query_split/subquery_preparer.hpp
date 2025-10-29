//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/subquery_preparer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/optimizer/query_split/query_split_util.h"
#include "duckdb/optimizer/query_split/top_down.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

//! Prepare the subquery, including
//! 1. merging the data chunk (temp table) to the logical plan,
//! 2. creating the projection head at the top of the logical plan,
//! 3. adapting the selection node to the query AST
class SubqueryPreparer : public LogicalOperatorVisitor {
public:
	SubqueryPreparer(Binder &binder, ClientContext &context) : binder(binder), context(context) {};
	~SubqueryPreparer() = default;

	//! Merge the data chunk to the current subquery
	int64_t MergeDataChunk(std::vector<unique_ptr<LogicalOperator>> &current_level_subqueries,
	                       unique_ptr<ColumnDataCollection> previous_result, idx_t estimated_card);
	//! Merge the created temp table to the current subquery
	unique_ptr<LogicalGet> MergeCreatedTable(std::vector<unique_ptr<LogicalOperator>> &current_level_subqueries,
	                       TableCatalogEntry& created_table_entry);

	//! Merge the previous sibling node. If merged to the main stream (left node), we add the sibling expr to proj.
	bool MergeSibling(std::vector<unique_ptr<LogicalOperator>> &current_level_subqueries,
	                  unique_ptr<LogicalOperator> last_sibling_node);

	//! Generate the projection head node at the top of the current subquery
	unique_ptr<LogicalOperator> GenerateProjHead(const unique_ptr<LogicalOperator> &original_plan,
	                                             unique_ptr<LogicalOperator> subquery,
	                                             const table_expr_info &table_expr_queue,
	                                             const std::vector<TableExpr> &original_proj_expr,
	                                             bool merge_sibling_expr);

	//! Adapt the selection node to the query AST
	shared_ptr<PreparedStatementData> AdaptSelect(shared_ptr<PreparedStatementData> original_stmt_data,
	                                              const unique_ptr<LogicalOperator> &subquery);

	table_expr_info UpdateTableExpr(table_expr_info table_expr_queue, std::vector<TableExpr> &original_proj_expr);

	unique_ptr<LogicalOperator> UpdateProjHead(unique_ptr<LogicalOperator> plan,
	                                           const std::vector<TableExpr> &original_proj_expr);

	// todo: refactor to a standalone class
	void CanonicalizeCrossProduct(unique_ptr<LogicalOperator> &plan);
	//! Check if current `subqueries_vec` has CROSS_PRODUCT that cannot be simplified
	bool NeedRewrite(const std::vector<unique_ptr<LogicalOperator>> &subqueries_vec);
	bool NeedReorder(const std::vector<unique_ptr<LogicalOperator>> &subqueries_vec,
	                 std::deque<std::pair<idx_t, idx_t>> table_card_order, idx_t previous_result_card);
	void MergeSubquery(unique_ptr<LogicalOperator> &plan, subquery_queue old_subqueries);

	//! update the table_idx and column_idx
	void UpdateSubqueriesIndex(subquery_queue &subqueries);

	void SetMergeIndex(int index) {
		merge_index = index;
	}

	void AddOldTableIndex(const unique_ptr<LogicalOperator> &op);

	// todo: refactor to a standalone class
	void ExplainAnalyzeSubQuery(ClientContextLock &lock, shared_ptr<PreparedStatementData> original_stmt_data,
	                            unique_ptr<LogicalOperator> explain_sub_plan, idx_t catalog_version,
	                            string statement_query, idx_t n_param, case_insensitive_map_t<idx_t> named_param_map);

	// todo: refactor to a standalone class
	unique_ptr<LogicalOperator> MergeBack(unique_ptr<LogicalOperator> last_sub_plan,
	                                      const unique_ptr<LogicalOperator> &sub_plan);

	idx_t GetEstCard(const unique_ptr<LogicalOperator> &sub_plan);

	std::set<idx_t> GetOldTableIndex() const {
		return old_table_idx;
	}
	void ClearOldTableIndex() {
		old_table_idx.clear();
	}
	void SetNewTableIndex(idx_t table_index) {
		new_table_idx = table_index;
	}

private:
	//! 1. find the insert point and insert the `ColumnDataGet` node to the logical plan;
	//! 2. update the table_idx and column_idx
	void MergeToSubquery(LogicalOperator &op, bool &merged);
	void MergeToSubquery(LogicalOperator &op, unique_ptr<LogicalGet> &created_table, bool &merged);
	//! Because the `chunk_scan` will create a new table index and contains the result of all tables (SEQ SCAN) of the
	//! current level, it is necessary to replace the index of the related expressions
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;

	void InsertTableBlocks(unique_ptr<LogicalOperator> &op,
	                       unordered_map<idx_t, unique_ptr<LogicalOperator>> &table_blocks,
	                       std::deque<idx_t> &table_blocks_key_order);

	bool BlockUsed(const unordered_set<idx_t> &left_cond_table_index, const unique_ptr<LogicalOperator> &op);

	void RevertSubqueriesIndex(unique_ptr<Expression> &expr);

	//! collect necessary info and return the last (lowest) non-CROSS_PRODUCT block
	unique_ptr<LogicalOperator> CheckTableUsage(LogicalOperator *current_join_pointer,
	                                            unordered_set<idx_t> &left_cond_table_index,
	                                            std::unordered_map<idx_t, unique_ptr<LogicalOperator>> &table_blocks,
	                                            std::deque<idx_t> &table_blocks_key_order,
	                                            std::queue<unique_ptr<LogicalOperator>> &unused_blocks);

	//! revert the plan, keep the same table order
	void RevertUsedBlocks(LogicalOperator *current_join_pointer, unique_ptr<LogicalOperator> last_block,
	                      deque<idx_t> &table_blocks_key_order,
	                      std::unordered_map<idx_t, unique_ptr<LogicalOperator>> &table_blocks);
	void RevertUnusedBlocks(LogicalOperator *current_join_pointer,
	                        std::queue<unique_ptr<LogicalOperator>> &unused_blocks);

private:
	Binder &binder;
	ClientContext &context;
	// all columns needed of the current level are shown in the proj's expression
	std::set<TableExpr> proj_exprs;
	// so far we only execute the first child node and will miss the sibling info
	// todo: should be changed when supporting the parallel execution of sibling execution
	std::set<TableExpr> last_sibling_exprs;
	// a new chunk scan node with the last level's result, generated and merged in `MergeDataChunk`
	unique_ptr<LogicalColumnDataGet> chunk_scan = nullptr;
	// `chunk_scan` will be moved, and we need one extra member to remember the new table index
	idx_t new_table_idx = DConstants::INVALID_INDEX;
	// the collection of the old table indexes, to detect and be replaced to the new index by `UpdateTableExpr`
	std::set<idx_t> old_table_idx;

	int merge_index = 0;

	// store the sub_plans and sub_plan_exprs in case the current_sub_plan doesn't have the CHUNK_GET node
	// (in `MergeBack`)
	std::unordered_map<int, unique_ptr<LogicalOperator>> stored_sub_plans;
	std::unordered_map<int, vector<unique_ptr<Expression>>> stored_sub_plan_exprs;
};

// find the tables in table_blocks are separate or union
class UnionFind {
public:
	unordered_map<int, int> parent;

	int find_parent(int x) {
		if (parent.find(x) == parent.end()) {
			parent[x] = x;
		}
		if (parent[x] != x) {
			parent[x] = find_parent(parent[x]);
		}
		return parent[x];
	}

	void unite(int x, int y) {
		int rootX = find_parent(x);
		int rootY = find_parent(y);
		if (rootX != rootY) {
			parent[rootX] = rootY;
		}
	}
};
} // namespace duckdb
