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
	std::unordered_map<idx_t, std::vector<idx_t>> subquery_group;

	// vertexes are columns used in the join operation
	// edges are the join relation
	// direction is from foreign key to primary key
	std::vector<std::string> join_dag_str;
	for (auto &column_pair : join_column_pairs) {
		//		// skip the user-defined columns, e.g. `WHERE k.keyword IN ('superhero', 'sequel', ...)`
		//		if (0 == foreign_key_represent.count(column_pair.first) || 0 ==
		// foreign_key_represent.count(column_pair.second)) 			continue;

		// if the right column_pair is the foreign key,
		// swap the pair to make sure foreign key is always on the left
		if (foreign_key_represent.at(column_pair.second).second)
			std::swap(column_pair.first, column_pair.second);
		subquery_group[column_pair.first.table_index].emplace_back(column_pair.second.table_index);
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
		for (const auto &primary_table_idx : ele.second) {
			target_tables.emplace(primary_table_idx, used_table_entries[primary_table_idx]);
			// debug: print the foreign key table and primary key table
			Printer::Print("Target primary tables: ");
			Printer::Print(used_table_entries[primary_table_idx]->name);
		}
		target_tables.emplace(ele.first, used_table_entries[ele.first]);
		// debug: print the foreign key table and primary key table
		Printer::Print("Target foreign tables: ");
		Printer::Print(used_table_entries[ele.first]->name);

		unique_ptr<LogicalOperator> subquery = original_plan->Copy(context);
		// for the first n-1 subqueries, only select the most related nodes/expressions
		// for the last subquery, merge the previous subqueries
		VisitOperator(*subquery);
		// debug: print subquery
		Printer::Print("Current subquery");
		subquery->Print();

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

unique_ptr<Expression> QuerySplit::VisitReplace(BoundAggregateExpression &expr, unique_ptr<Expression> *expr_ptr) {
	// delete the invalid expr
	VisitExpressionChildren(expr);
	for (auto bound_col_it = expr.children.begin(); bound_col_it != expr.children.end();) {
		if (ExpressionType::INVALID == (*bound_col_it)->type) {
			bound_col_it = expr.children.erase(bound_col_it);
		} else {
			bound_col_it++;
		}
	}

	if (expr.children.empty()) {
		// if it doesn't have any child, make it invalid
		auto empty_expr = expr.Copy();
		empty_expr->type = ExpressionType::INVALID;
		return empty_expr;
	} else {
		return nullptr;
	}
}

unique_ptr<Expression> QuerySplit::VisitReplace(BoundFunctionExpression &expr, unique_ptr<Expression> *expr_ptr) {
	// delete the invalid expr
	VisitExpressionChildren(expr);
	bool disable = false;
	for (const auto &bound_col : expr.children) {
		if (ExpressionType::INVALID == bound_col->type) {
			disable = true;
			break;
		}
	}

	if (disable) {
		// if it doesn't have any child, make it invalid
		auto empty_expr = expr.Copy();
		empty_expr->type = ExpressionType::INVALID;
		return empty_expr;
	} else {
		return nullptr;
	}
}

unique_ptr<Expression> QuerySplit::VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) {
	if (0 == target_tables.count(expr.binding.table_index)) {
		// check projection's alias first
		// todo: this should have a better way than matching string... it might introduce bugs...
		for (const auto &used_table : target_tables) {
			if (std::string::npos != expr.alias.find(used_table.second->name)) {
				std::string warn = "Warning: We assume " + expr.alias + " uses " + used_table.second->name;
				Printer::Print(warn);
				return nullptr;
			}
		}
		// expr is not a target table, remove it from expr
		auto empty_expr = expr.Copy();
		empty_expr->type = ExpressionType::INVALID;
		return empty_expr;
	} else {
		return nullptr;
	}
}

void QuerySplit::VisitOperator(LogicalOperator &op) {
	// visit expressions and delete the unrelated child node first
	VisitOperatorExpressions(op);

	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
		VisitProjection(op.Cast<LogicalProjection>());
		break;
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		VisitAggregate(op.Cast<LogicalAggregate>());
		break;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		VisitComparisonJoin(op.Cast<LogicalComparisonJoin>());
		break;
	case LogicalOperatorType::LOGICAL_FILTER:
		VisitFilter(op.Cast<LogicalFilter>());
		break;
	case LogicalOperatorType::LOGICAL_GET:
		VisitGet(op.Cast<LogicalGet>());
		break;
	default:
		break;
	}

	VisitOperatorChildren(op);
	// todo: lift the child node to replace it
	bool find_valid_child = false;
	for (const auto &child_op : op.children) {
		if (LogicalOperatorType::LOGICAL_INVALID != child_op->type) {
			find_valid_child = true;
		}
	}
	int a = 0;
}

void QuerySplit::VisitAggregate(LogicalAggregate &op) {
	for (auto expr_it = op.expressions.begin(); expr_it != op.expressions.end();) {
		if (ExpressionType::INVALID == (*expr_it)->type) {
			expr_it = op.expressions.erase(expr_it);
		} else {
			expr_it++;
		}
	}
}

void QuerySplit::VisitComparisonJoin(LogicalComparisonJoin &op) {
	for (auto cond_it = op.conditions.begin(); cond_it != op.conditions.end();) {
		if (ExpressionType::INVALID == cond_it->left->type || ExpressionType::INVALID == cond_it->right->type) {
			cond_it = op.conditions.erase(cond_it);
		} else {
			cond_it++;
		}
	}
}

void QuerySplit::VisitFilter(LogicalFilter &op) {
	for (auto expr_it = op.expressions.begin(); expr_it != op.expressions.end();) {
		if (ExpressionType::INVALID == (*expr_it)->type) {
			expr_it = op.expressions.erase(expr_it);
		} else {
			expr_it++;
		}
	}
}

void QuerySplit::VisitProjection(LogicalProjection &op) {
	for (auto expr_it = op.expressions.begin(); expr_it != op.expressions.end();) {
		if (ExpressionType::INVALID == (*expr_it)->type) {
			expr_it = op.expressions.erase(expr_it);
		} else {
			expr_it++;
		}
	}
}

void QuerySplit::VisitGet(LogicalGet &op) {
	if (0 == target_tables.count(op.table_index)) {
		op.type = LogicalOperatorType::LOGICAL_INVALID;
	}
}

} // namespace duckdb