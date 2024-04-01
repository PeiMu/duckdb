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

	// devide subqueries into groups
	// <foreign key table_index, std::vector<primary key table_index>>
	std::unordered_map<idx_t, std::vector<ColumnBinding>> subquery_group;

	// vertexes are columns used in the join operation
	// edges are the join relation
	// direction is from foreign key to primary key
	std::vector<std::string> join_dag_str;
	for (auto &column_pair : join_column_pairs) {
		// if the right column_pair is the foreign key,
		// swap the pair to make sure foreign key is always on the left
		if (foreign_key_represent.at(column_pair.second).second)
			std::swap(column_pair.first, column_pair.second);
		subquery_group[column_pair.first.table_index].emplace_back(column_pair.first);
		subquery_group[column_pair.first.table_index].emplace_back(column_pair.second);
		// debug: print as "foreign_key -> primary_key"
		join_dag_str.emplace_back(used_table_entries[column_pair.first.table_index]->name + "." +
		                          foreign_key_represent.at(column_pair.first).first.Name() + " -> " +
		                          used_table_entries[column_pair.second.table_index]->name + "." +
		                          foreign_key_represent.at(column_pair.second).first.Name());
	}
	Printer::Print("Join Relations (foreign_key -> primary_key):");
	for (const auto &str : join_dag_str) {
		Printer::Print(str);
	}

	// debug: try to generate the subqueries
	std::vector<unique_ptr<LogicalOperator>> sub_queries;
	for (const auto &ele : subquery_group) {
		unique_ptr<LogicalOperator> subquery = CreateSubQuery(original_plan, ele.second, used_table_entries);
		sub_queries.emplace_back(std::move(subquery));
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

unique_ptr<LogicalOperator>
QuerySplit::CreateSubQuery(const unique_ptr<LogicalOperator> &original_plan,
                           const std::vector<ColumnBinding> &target_tables,
                           const std::unordered_map<idx_t, TableCatalogEntry *> &used_table_entries) {
	// debug: print the foreign key table and primary key table
	Printer::Print("Target tables: ");
	for (const auto &primary_table : target_tables) {
		Printer::Print(used_table_entries.at(primary_table.table_index)->name);
	}

	unique_ptr<LogicalOperator> subquery;
	std::function<void(LogicalOperator * op, const std::vector<ColumnBinding> &target_tables)> collect_related_ops;
	collect_related_ops = [&collect_related_ops, &subquery](LogicalOperator *op,
	                                                        const std::vector<ColumnBinding> &target_tables) {
		// check if current op needs the target tables
		switch (op->type) {
		case LogicalOperatorType::LOGICAL_PROJECTION:
			break;
		default:
			Printer::Print("Do not support such operation yet");
			Printer::Print(LogicalOperatorToString(op->type));
			break;
		}

		if (LogicalOperatorType::LOGICAL_PROJECTION == op->type) {
			auto &projection_op = op->Cast<LogicalProjection>();
			auto table_index = projection_op.table_index;
			//			auto exprs = projection_op.expressions;
			auto table_indexs = projection_op.GetTableIndex();
			auto col = projection_op.GetColumnBindings();

		} else if (LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY == op->type) {
			auto &agg_group_op = op->Cast<LogicalAggregate>();
			auto table_index = agg_group_op.GetTableIndex();
			auto col = agg_group_op.GetColumnBindings();
			for (auto expr_it = agg_group_op.expressions.begin(); expr_it != agg_group_op.expressions.end();) {
				if (ExpressionClass::BOUND_AGGREGATE == (*expr_it)->expression_class) {
					auto &agg_expr = (*expr_it)->Cast<BoundAggregateExpression>();
					for (auto bound_col_it = agg_expr.children.begin(); bound_col_it != agg_expr.children.end();) {
						if (ExpressionClass::BOUND_COLUMN_REF == (*bound_col_it)->expression_class) {
							auto &col_ref = (*bound_col_it)->Cast<BoundColumnRefExpression>();
							if (std::find_if(target_tables.begin(), target_tables.end(),
							                 [&col_ref](const ColumnBinding &current_col) {
								                 return col_ref.binding.table_index == current_col.table_index;
							                 }) == target_tables.end()) {
								// col_ref is not a target table, remove it from agg_expr
								bound_col_it = agg_expr.children.erase(bound_col_it);
							} else {
								bound_col_it++;
							}
						} else {
							Printer::Print("Do not support yet");
						}
					}
					if (agg_expr.children.empty()) {
						expr_it = agg_group_op.expressions.erase(expr_it);
					} else {
						expr_it++;
					}
				} else {
					Printer::Print("Do not support yet");
				}
			}
		}

		for (const auto &child : op->children) {
			auto child_op = child.get();
			collect_related_ops(child_op, target_tables);
			switch (child_op->type) {
			case LogicalOperatorType::LOGICAL_GET:
			case LogicalOperatorType::LOGICAL_CHUNK_GET:
				break;
			case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
			default:
				Printer::Print("Do not support such operation yet");
				Printer::Print(LogicalOperatorToString(child_op->type));
			}
		}
	};

	collect_related_ops(original_plan.get(), target_tables);

	return subquery;
}

} // namespace duckdb