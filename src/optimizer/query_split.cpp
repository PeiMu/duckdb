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
	// <column_binding, is_foreign_key>
	std::unordered_map<ColumnBinding, bool, ColumnBinding::ColumnBindingHash> foreign_key_represent;
	// <table_index, table_entry>
	std::unordered_map<idx_t, TableCatalogEntry *> used_table_entries;

	// collect the primary/foreign key information from table entry
	std::function<void(const LogicalOperator &, std::vector<std::pair<ColumnBinding, ColumnBinding>>)> find_set;
	find_set = [&find_set, &used_table_entries,
	            &foreign_key_represent](const LogicalOperator &join_op,
	                                    const std::vector<std::pair<ColumnBinding, ColumnBinding>> &join_column_pairs) {
		for (const auto &child : join_op.children) {
			auto child_op = child.get();
			if (LogicalOperatorType::LOGICAL_GET == child_op->type) {
				auto &get = child_op->Cast<LogicalGet>();
				auto current_table_index = get.table_index;
				auto &table_entry = get.GetTable()->Cast<TableCatalogEntry>();

				used_table_entries.emplace(current_table_index, &table_entry);

				// find the foreign key through iteration tables,
				// but this method cost a lot of time due to the big loop
				for (const auto &constraint : table_entry.GetConstraints()) {
					if (ConstraintType::FOREIGN_KEY == constraint->type) {
						auto &fk = constraint->Cast<ForeignKeyConstraint>();
						for (const auto &column_pair : join_column_pairs) {
							if (auto find_left =
							        std::find_if(fk.info.fk_keys.begin(), fk.info.fk_keys.end(),
							                     [column_pair, current_table_index, &get](const PhysicalIndex &idx) {
								                     // todo: confirm if the `get.column_ids` is a mapping
								                     // from ColumnBinding to PhysicalIndex
								                     return current_table_index == column_pair.first.table_index &&
								                            idx.index == get.column_ids[column_pair.first.column_index];
							                     });
							    find_left != fk.info.fk_keys.end()) {
								foreign_key_represent[column_pair.first] = true;
							}
						}

						for (const auto &column_pair : join_column_pairs) {
							if (auto find_right = std::find_if(
							        fk.info.fk_keys.begin(), fk.info.fk_keys.end(),
							        [column_pair, current_table_index, &get](const PhysicalIndex &idx) {
								        return current_table_index == column_pair.second.table_index &&
								               idx.index == get.column_ids[column_pair.second.column_index];
							        });
							    find_right != fk.info.fk_keys.end()) {
								foreign_key_represent[column_pair.second] = true;
							}
						}
					}
				}
			}
			find_set(*child_op, join_column_pairs);
		}
	};

	// find the join operation
	// check children node of the join operation, it should be a LOGICAL_GET
	std::function<void(const LogicalOperator &)> find_join;
	find_join = [&find_join, &join_column_pairs, &foreign_key_represent](const LogicalOperator &op) -> void {
		for (const auto &child : op.children) {
			auto child_op = child.get();
			if (LogicalOperatorType::LOGICAL_COMPARISON_JOIN == child_op->type) {
				auto &join = child_op->Cast<LogicalComparisonJoin>();
				const auto &cond = join.conditions;

				for (const auto &cond_it : cond) {
					// collect the columns used
					auto &left_colref = cond_it.left->Cast<BoundColumnRefExpression>();
					auto &right_colref = cond_it.right->Cast<BoundColumnRefExpression>();

					join_column_pairs.emplace_back(left_colref.binding, right_colref.binding);
					// initialize the umap by false
					if (!foreign_key_represent.count(left_colref.binding))
						foreign_key_represent.emplace(left_colref.binding, false);
					if (!foreign_key_represent.count(right_colref.binding))
						foreign_key_represent.emplace(right_colref.binding, false);
				}
			}
			find_join(*child_op);
		}
	};

	// 1. DFS search to find the COMPARISON_JOIN (or Any_JOIN? todo: confirm)
	// 2. collect the table/columns used in the join operation
	// 3. check it is a primary key or foreign key
	find_join(*op);
	find_set(*op, join_column_pairs);

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
			                   return (foreign_key_represent[column_pair.first] &&
			                           foreign_key_represent[column_pair.second]);
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

	//	// <column, table>
	//	std::unordered_map<std::string, std::string> column_table_map;
	//	// collect tables based on the columns
	//	for (const auto& fk : foreign_key_represent) {
	//		for (const auto& table_entry : used_table_entries) {
	//			if (table_entry->ColumnExists(fk.first)) {
	//				column_table_map[fk.first] = table_entry->name;
	//				break;
	//			}
	//		}
	//	}

	// vertexes are columns used in the join operation
	// edges are the join relation
	// direction is from foreign key to primary key
	// print as "left_table -> right_table"
	std::vector<std::string> join_dag_str;
	for (auto &column : join_column_pairs) {
		// if the right column is the foreign key,
		// swap the pair to make sure foreign key is always on the left
		if (foreign_key_represent[column.second])
			std::swap(column.first, column.second);
		join_dag_str.emplace_back(used_table_entries[column.first.table_index]->name + " -> " +
		                          used_table_entries[column.second.table_index]->name);
	}
	Printer::Print("Join Relations (foreign_key -> primary_key):");
	for (const auto &str : join_dag_str) {
		Printer::Print(str);
	}

	return original_plan;
}

} // namespace duckdb