#include "duckdb/optimizer/join_push_down/join_push_down.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> duckdb::JoinPushDown::Rewrite(unique_ptr<LogicalOperator> op) {
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_TRANSACTION:
		return op; // skip optimizing simple & often-occurring plans unaffected by rewrites
	default:
		break;
	}

	// todo: 1. collect the condition of the last join
	auto op_child = op->Copy(context);
	while (LogicalOperatorType::LOGICAL_CROSS_PRODUCT != op_child->children[0]->type) {
		op_child = std::move(op_child->children[0]);
	}
	auto &last_join = op_child->Cast<LogicalComparisonJoin>();

	// debug
	Printer::Print("last join: ");
	last_join.Print();


	// todo: 2. select the needed tables (or the FILTER block) and rest tables from CROSS_PRODUCTs
	// collect all tables below (and included) the last JOIN
	std::unordered_map<idx_t, unique_ptr<LogicalOperator>> table_blocks;
	InsertTableBlocks(last_join.children[0], table_blocks);
	InsertTableBlocks(last_join.children[1], table_blocks);

	op_child = op->Copy(context);
	while (op_child) {
		if (LogicalOperatorType::LOGICAL_CROSS_PRODUCT == op_child->type) {
			auto &cross_product_op = op_child->Cast<LogicalCrossProduct>();
			InsertTableBlocks(cross_product_op.children[0], table_blocks);
			InsertTableBlocks(cross_product_op.children[1], table_blocks);
		} else {
			// we only check the main thunk of the tree
			op_child = std::move(op_child->children[0]);
		}
	}

	// debug
	Printer::Print("table_blocks");
	for (const auto &block : table_blocks) {
		Printer::Print("block index: " + std::to_string(block.first));
		Printer::Print("operator: ");
		block.second->Print();
	}

	// select the needed tables
	std::vector<std::pair<idx_t, idx_t>> last_join_table_index;
	for (const auto &cond : last_join.conditions) {
		D_ASSERT(ExpressionType::BOUND_COLUMN_REF == cond.left->type);
		auto &left_expr = cond.left->Cast<BoundColumnRefExpression>();
		auto &right_expr = cond.right->Cast<BoundColumnRefExpression>();
		last_join_table_index.emplace_back(
		    std::make_pair(left_expr.binding.table_index, right_expr.binding.table_index));
	}

	// debug
	for (const auto &index_pair : last_join_table_index) {
		D_ASSERT(table_blocks.count(index_pair.first));
		D_ASSERT(table_blocks.count(index_pair.second));
	}

	// todo: 3. split the last JOIN and construct new JOINs in the bottom up order
	auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);

	// todo: 4. reorder the tables for the splitted JOINs

	// todo: 5. add the rest of CROSS_PRODUCTs and JOINs on top of the splitted JOINs

	return op;
}

bool JoinPushDown::HasCrossProduct(const unique_ptr<LogicalOperator> &op) {
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_TRANSACTION:
		return false; // skip optimizing simple & often-occurring plans unaffected by rewrites
	default:
		break;
	}

	return false;
}
void JoinPushDown::InsertTableBlocks(unique_ptr<LogicalOperator> &op,
                                     unordered_map<idx_t, unique_ptr<LogicalOperator>> &table_blocks) {
	if (LogicalOperatorType::LOGICAL_GET == op->type) {
		auto &get_op = op->Cast<LogicalGet>();
		table_blocks.emplace(get_op.table_index, &get_op);
	} else if (LogicalOperatorType::LOGICAL_FILTER == op->type) {
		idx_t table_index;
		std::function<void (unique_ptr<LogicalOperator> &current_op)> find_get;
		find_get = [find_get, &table_index](unique_ptr<LogicalOperator> &current_op) {
			for (auto& child_op : current_op->children) {
				if (LogicalOperatorType::LOGICAL_GET != child_op->type)
					find_get(child_op);
				else {
					auto &get_op = child_op->Cast<LogicalGet>();
					table_index = get_op.table_index;
				}
			}
		};
		auto current_op = op->Copy(context);
		find_get(current_op);
		table_blocks.emplace(table_index, std::move(op));
	}
}

bool JoinPushDown::IsNecessaryToRewrite(const unique_ptr<LogicalOperator> &op) {
	return false;
}

} // namespace duckdb
