#include "duckdb/optimizer/join_push_down/join_push_down.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> duckdb::JoinPushDown::Rewrite(unique_ptr<LogicalOperator> op) {
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_TRANSACTION:
	case LogicalOperatorType::LOGICAL_PRAGMA:
		return op; // skip optimizing simple & often-occurring plans unaffected by rewrites
	default:
		break;
	}

	auto new_plan = op->Copy(context);
	// 1. collect the condition of the last join
	auto op_child = op.get();
	int op_levels = 0;
	while (LogicalOperatorType::LOGICAL_CROSS_PRODUCT != op_child->children[0]->type) {
		op_child = op_child->children[0].get();
		op_levels++;
	}
	auto &last_join = op_child->Cast<LogicalComparisonJoin>();

	// debug
	Printer::Print("last join: ");
	last_join.Print();

	// select the needed tables
//	std::vector<std::pair<idx_t, idx_t>> last_join_table_index;
	std::unordered_set<idx_t> left_cond_table_index;
	for (const auto &cond : last_join.conditions) {
		D_ASSERT(ExpressionType::BOUND_COLUMN_REF == cond.left->type);
		auto &left_expr = cond.left->Cast<BoundColumnRefExpression>();
		auto &right_expr = cond.right->Cast<BoundColumnRefExpression>();
//		last_join_table_index.emplace_back(
//		    std::make_pair(left_expr.binding.table_index, right_expr.binding.table_index));
		left_cond_table_index.emplace(left_expr.binding.table_index);
	}

	// 2. collect all tables below the last JOIN
	std::unordered_map<idx_t, unique_ptr<LogicalOperator>> table_blocks;
	//	InsertTableBlocks(last_join.children[0], table_blocks);
	//	InsertTableBlocks(last_join.children[1], table_blocks);

	auto below_op = op_child;
	while (LogicalOperatorType::LOGICAL_CROSS_PRODUCT == below_op->children[0]->type) {
		below_op = below_op->children[0].get();
		auto &cross_product_op = below_op->Cast<LogicalCrossProduct>();
		InsertTableBlocks(cross_product_op.children[1], table_blocks);
	}
	auto &last_block = below_op->Cast<LogicalCrossProduct>().children[0];

	// debug
	Printer::Print("table_blocks");
	last_block->Print();
	for (const auto &block : table_blocks) {
		Printer::Print("block index: " + std::to_string(block.first));
		Printer::Print("operator: ");
		block.second->Print();
	}

	std::queue<unique_ptr<LogicalOperator>> unused_blocks;
	unique_ptr<LogicalOperator> below_cross_product = std::move(last_block);
	for (auto &block : table_blocks) {
		if (left_cond_table_index.count(block.first)) {
			below_cross_product = LogicalCrossProduct::Create(std::move(below_cross_product), std::move(block.second));
		} else {
			unused_blocks.push(std::move(block.second));
		}
	}
	auto right_child = last_join.children[1]->Copy(context);
	last_join.children.clear();
	//	last_join.children.push_back(std::move(below_cross_product));
	//	last_join.children.push_back(std::move(right_child));
	last_join.AddChild(std::move(below_cross_product));
	last_join.AddChild(std::move(right_child));

	auto above_cross_product = last_join.Copy(context);
	while (!unused_blocks.empty()) {
		above_cross_product =
		    LogicalCrossProduct::Create(std::move(above_cross_product), std::move(unused_blocks.front()));
		unused_blocks.pop();
	}

	// get the position to reorder
	auto reordered_plan = new_plan.get();
	for (int level_id = 0; level_id < op_levels - 1; level_id++) {
		reordered_plan = reordered_plan->children[0].get();
	}
	right_child = reordered_plan->children[1]->Copy(context);
	reordered_plan->children.clear();
	reordered_plan->AddChild(std::move(above_cross_product));
	reordered_plan->AddChild(std::move(right_child));

	// todo: 3. split the last JOIN and construct new JOINs in the bottom up order
//	auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);

	// todo: 4. reorder the tables for the splitted JOINs

	// todo: 5. add the rest of CROSS_PRODUCTs and JOINs on top of the splitted JOINs

	return new_plan;
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
		table_blocks.emplace(get_op.table_index, get_op.Copy(context));
	} else if (LogicalOperatorType::LOGICAL_FILTER == op->type) {
		idx_t table_index;
		std::function<void(unique_ptr<LogicalOperator> & current_op)> find_get;
		find_get = [find_get, &table_index](unique_ptr<LogicalOperator> &current_op) {
			for (auto &child_op : current_op->children) {
				if (LogicalOperatorType::LOGICAL_GET != child_op->type)
					find_get(child_op);
				else {
					auto &get_op = child_op->Cast<LogicalGet>();
					table_index = get_op.table_index;
				}
			}
		};
		find_get(op);
		table_blocks.emplace(table_index, op->Copy(context));
	} else {
		Printer::Print(
		    StringUtil::Format("Do not support yet, block_op->type:  %s", LogicalOperatorToString(op->type)));
		D_ASSERT(false);
	}
}

bool JoinPushDown::IsNecessaryToRewrite(const unique_ptr<LogicalOperator> &op) {
	return false;
}

} // namespace duckdb
