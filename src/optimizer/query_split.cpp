#include "duckdb/optimizer/query_split.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> QuerySplit::Optimize(unique_ptr<LogicalOperator> plan) {
	// remove redundant joins if the current query is not a CMD_UTILITY
	// todo: check if the current query is a CMD_UTILITY
	if (LogicalOperatorType::LOGICAL_PROJECTION != plan->type && LogicalOperatorType::LOGICAL_ORDER_BY != plan->type &&
	    LogicalOperatorType::LOGICAL_EXPLAIN != plan->type) {
		return plan;
	}

	Printer::Print("In QuerySplit\n");
	plan->Print();

	unique_ptr<LogicalOperator> new_logical_plan = std::move(plan);
	unique_ptr<LogicalOperator> plan_without_redundant_jon = RemoveDedundantJoin(std::move(new_logical_plan));

	uint64_t length = CollectRangeTableLength(plan_without_redundant_jon);
	if (length <= 2) {
		return plan;
	}

	return Recon(std::move(plan_without_redundant_jon), length);
}

unique_ptr<LogicalOperator> QuerySplit::RemoveDedundantJoin(unique_ptr<LogicalOperator> original_plan) {
	// todo
	return original_plan;
}

uint64_t QuerySplit::CollectRangeTableLength(const unique_ptr<LogicalOperator> &plan) {
	// skip order by and explain
	auto op = plan.get();
	while (LogicalOperatorType::LOGICAL_PROJECTION != op->type) {
		op = op->children[0].get();
	}

	vector<idx_t> table_index = op->GetTableIndex();
	D_ASSERT(1 == table_index.size());
	return table_index[0];
}

unique_ptr<LogicalOperator> QuerySplit::Recon(unique_ptr<LogicalOperator> original_plan, uint64_t length) {
	auto op = original_plan.get();

	// <join_left_column_binding, join_right_column_binding>
	std::vector<std::pair<ColumnBinding, ColumnBinding>> join_column_pairs;
	// <column_binding, <column_definition, is_foreign_key>>
	fk_map foreign_key_represent;
	// <table_index, table_entry>
	std::unordered_map<idx_t, TableCatalogEntry *> used_table_entries;

	// 1. DFS search to find the COMPARISON_JOIN (or Any_JOIN? todo: confirm)
	// 2. collect the table/columns used in the join operation
	// 3. check it is a primary key or foreign key
	CheckJoin(join_column_pairs, *op);
	CheckSet(foreign_key_represent, used_table_entries, *op, join_column_pairs);

	// todo
	// if the DAG has multi-centers, e.g.
	// an example from job benchmark 6d.sql
	// k <- mk -> t <- ci -> n
	// we can decide a better split algorithm
	// here we use foreign_key_center by default

	if (EnumSplitAlgorithm::min_sub_query != split_algorithm) {
		// delete the pair when left and right column are both foreign key
		join_column_pairs.erase(
		    std::remove_if(join_column_pairs.begin(), join_column_pairs.end(),
		                   [&foreign_key_represent](const std::pair<ColumnBinding, ColumnBinding> &column_pair) {
			                   return (foreign_key_represent.at(column_pair.first).second &&
			                           foreign_key_represent.at(column_pair.second).second);
		                   }),
		    join_column_pairs.end());
	}

	//  ┌─────────────┴─────────────┐
	//  │      COMPARISON_JOIN      │
	//  │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
	//  │            MARK           ├
	//  │    (keyword = #[11.0])    │
	//  └─────────────┬─────────────┘
	// not sure what is this, but we should delete it from `join_column_pairs`
	join_column_pairs.erase(std::remove_if(join_column_pairs.begin(), join_column_pairs.end(),
	                                       [used_table_entries](std::pair<ColumnBinding, ColumnBinding> column_pair) {
		                                       return (0 == used_table_entries.count(column_pair.first.table_index)) ||
		                                              (0 == used_table_entries.count(column_pair.second.table_index));
	                                       }),
	                        join_column_pairs.end());

	// vertexes are columns used in the join operation
	// edges are the join relation
	// direction is from foreign key to primary key
	// print as "left_table -> right_table"
	std::vector<std::string> join_dag_str;
	for (auto &column_pair : join_column_pairs) {
		// if the right column_pair is the foreign key,
		// swap the pair to make sure foreign key is always on the left
		if (foreign_key_represent.at(column_pair.second).second)
			std::swap(column_pair.first, column_pair.second);
		join_dag_str.emplace_back(used_table_entries[column_pair.first.table_index]->name + "." +
		                          foreign_key_represent.at(column_pair.first).first.Name() + " -> " +
		                          used_table_entries[column_pair.second.table_index]->name + "." +
		                          foreign_key_represent.at(column_pair.second).first.Name());
	}
	Printer::Print("Join Relations (foreign_key -> primary_key):");
	for (const auto &str : join_dag_str) {
		Printer::Print(str);
	}

	return original_plan;
}

void QuerySplit::CheckJoin(std::vector<std::pair<ColumnBinding, ColumnBinding>> &join_column_pairs,
                           const LogicalOperator &op) {

	for (const auto &child : op.children) {
		auto &child_op = child;
		if (LogicalOperatorType::LOGICAL_COMPARISON_JOIN == child_op->type) {
			auto &join = child_op->Cast<LogicalComparisonJoin>();
			const auto &cond = join.conditions;

			for (const auto &cond_it : cond) {
				// collect the columns used
				auto &left_colref = cond_it.left->Cast<BoundColumnRefExpression>();
				auto &right_colref = cond_it.right->Cast<BoundColumnRefExpression>();

				join_column_pairs.emplace_back(left_colref.binding, right_colref.binding);
			}
		}
		CheckJoin(join_column_pairs, *child_op);
	}
}

void QuerySplit::CheckSet(fk_map &foreign_key_represent,
                          std::unordered_map<idx_t, TableCatalogEntry *> &used_table_entries, const LogicalOperator &op,
                          const std::vector<std::pair<ColumnBinding, ColumnBinding>> &join_column_pairs) {
	for (const auto &child : op.children) {
		auto child_op = child.get();
		if (LogicalOperatorType::LOGICAL_GET == child_op->type) {
			auto &get = child_op->Cast<LogicalGet>();
			auto current_table_index = get.table_index;
			auto &table_entry = get.GetTable()->Cast<TableCatalogEntry>();

			used_table_entries.emplace(current_table_index, &table_entry);

			// use the `column_ids` to find the physical/logical index,
			// then search through the `fk.info.fk_keys` to check if current column has a foreign key
			auto check_foreign_key = [current_table_index, &foreign_key_represent](const ColumnBinding &column,
			                                                                       const LogicalGet &get,
			                                                                       TableCatalogEntry &table_entry) {
				if (current_table_index == column.table_index) {
					auto physical_idx = get.column_ids[column.column_index];
					auto logical_idx = table_entry.GetColumns().PhysicalToLogical(PhysicalIndex(physical_idx));
					bool is_foreign_key = false;
					for (const auto &constraint : table_entry.GetBoundConstraints()) {
						if (ConstraintType::FOREIGN_KEY == constraint->type) {
							auto &fk = constraint->Cast<BoundForeignKeyConstraint>();
							if (auto find_left = std::find_if(
							        fk.info.fk_keys.begin(), fk.info.fk_keys.end(),
							        [physical_idx](const PhysicalIndex &idx) { return idx.index == physical_idx; });
							    find_left != fk.info.fk_keys.end()) {
								is_foreign_key = true;
								break;
							}
						}
					}
					foreign_key_represent.emplace(
					    column, std::make_pair(table_entry.GetColumn(logical_idx).Copy(), is_foreign_key));
				}
			};

			// find the foreign key through iteration tables
			for (const auto &column_pair : join_column_pairs) {
				check_foreign_key(column_pair.first, get, table_entry);
				check_foreign_key(column_pair.second, get, table_entry);
			}
		}
		CheckSet(foreign_key_represent, used_table_entries, *child_op, join_column_pairs);
	}
}

} // namespace duckdb