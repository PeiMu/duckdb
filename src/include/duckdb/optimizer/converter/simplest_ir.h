//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/simplest_ir.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/printer.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "read.hpp"

#include <memory>

namespace duckdb {

enum SimplestVarType { InvalidVarType = 0, IntVar, FloatVar, StringVar };
enum SimplestJoinType { InvalidJoinType = 0, Inner, Left, Full, Right, Semi, Anti, UniqueOuter, UniqueInner };
enum SimplestComparisonType { InvalidComparisonType = 0, Equal, LessThan, GreaterThan, LessEqual, GreaterEqual, Not };
enum SimplestNodeType {
	InvalidNodeType = 0,
	LiteralNode,
	VarNode,
	ConstVarNode,
	AttrVarNode,
	ComparisonExprNode,
	VarComparisonNode,
	VarConstComparisonNode,
	StmtNode,
	AggregateNode,
	JoinNode,
	FilterNode,
	ScanNode,
	HashNode
};

class SimplestNode {
public:
	SimplestNode(SimplestNodeType node_type) : node_type(node_type) {};
	virtual ~SimplestNode() = default;

	template <class TARGET>
	TARGET &Cast() {
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		return reinterpret_cast<const TARGET &>(*this);
	}

	virtual std::string Print(bool print = true) = 0;

	SimplestNodeType GetNodeType() const {
		return node_type;
	}

private:
	SimplestNodeType node_type;
};

class SimplestLiteral : public SimplestNode {
public:
	SimplestLiteral(std::string literal_value) : SimplestNode(LiteralNode), literal_value(literal_value) {};
	SimplestLiteral(const SimplestLiteral &other)
	    : SimplestNode(other.GetNodeType()), literal_value(other.literal_value) {};
	~SimplestLiteral() = default;

	std::string Print(bool print = true) {
		std::string str = " \"" + literal_value + "\" ";
		if (print)
			Printer::Print(str);
		return str;
	};

	std::string GetLiteralValue() {
		return literal_value;
	}

private:
	std::string literal_value;
};

class SimplestVar : public SimplestNode {
public:
	SimplestVar(SimplestVarType type, bool is_const, SimplestNodeType node_type)
	    : SimplestNode(node_type), type(type), is_const(is_const) {};
	SimplestVar(const SimplestVar &other)
	    : SimplestNode(other.GetNodeType()), type(other.type), is_const(other.is_const) {};
	~SimplestVar() = default;

	SimplestVarType GetType() const {
		return type;
	}

	bool IsConst() {
		return is_const;
	}

	virtual std::string Print(bool print = true) = 0;

private:
	SimplestVarType type;
	bool is_const;
};

class SimplestConstVar : public SimplestVar {
public:
	SimplestConstVar(int int_value) : SimplestVar(SimplestVarType::IntVar, true, ConstVarNode), int_value(int_value) {};
	SimplestConstVar(float float_value)
	    : SimplestVar(SimplestVarType::FloatVar, true, ConstVarNode), float_value(float_value) {};
	SimplestConstVar(std::string str_value)
	    : SimplestVar(SimplestVarType::StringVar, true, ConstVarNode), str_value(str_value) {};
	SimplestConstVar(const SimplestConstVar &other)
	    : SimplestVar(other.GetType(), true, ConstVarNode), int_value(other.int_value), float_value(other.float_value),
	      str_value(other.str_value) {};
	SimplestConstVar(unique_ptr<SimplestConstVar> other)
	    : SimplestVar(other->GetType(), true, ConstVarNode), int_value(other->int_value),
	      float_value(other->float_value), str_value(other->str_value) {};
	~SimplestConstVar() = default;

	int GetIntValue() const {
		return int_value;
	}
	float GetFloatValue() const {
		return float_value;
	}
	std::string GetStringValue() const {
		return str_value;
	}

	std::string Print(bool print = true) override {
		std::string str;
		switch (GetType()) {
		case InvalidVarType:
			Printer::Print("\ninvalid Vary Type!!!");
			return str;
		case IntVar:
			str = "Integer const value: " + std::to_string(int_value);
			break;
		case FloatVar:
			str = "Float const value: " + std::to_string(float_value);
			break;
		case StringVar:
			str = "String const value: " + str_value;
			break;
		}
		if (print)
			Printer::Print(str);

		return str;
	}

private:
	int int_value;
	float float_value;
	std::string str_value;
};

class SimplestAttr : public SimplestVar {
public:
	SimplestAttr(SimplestVarType var_type, unsigned int table_index, unsigned int column_index, std::string column_name)
	    : SimplestVar(var_type, false, AttrVarNode), table_index(table_index), column_index(column_index),
	      column_name(column_name) {};
	SimplestAttr(const SimplestAttr &other)
	    : SimplestVar(other.GetType(), false, AttrVarNode), table_index(other.table_index),
	      column_index(other.column_index), column_name(other.column_name) {};
	SimplestAttr(unique_ptr<SimplestAttr> other)
	    : SimplestVar(other->GetType(), false, AttrVarNode), table_index(other->table_index),
	      column_index(other->column_index) {};
	~SimplestAttr() = default;

	unsigned int GetTableIndex() const {
		return table_index;
	}
	unsigned int GetColumnIndex() const {
		return column_index;
	}
	std::string GetColumnName() const {
		return column_name;
	}
	void SetColumnName(std::string col_name) {
		column_name = col_name;
	}

	std::string Print(bool print = true) override {
		std::string str;
		switch (GetType()) {
		case InvalidVarType:
			Printer::Print("\ninvalid Vary Type!!!");
			return str;
		case IntVar:
			str = "Integer variable ";
			break;
		case FloatVar:
			str = "Float variable ";
			break;
		case StringVar:
			str = "String variable ";
			break;
		}

		std::string info =
		    "#(" + std::to_string(table_index) + ", " + std::to_string(column_index) + "): " + column_name;
		str += info;

		if (print)
			Printer::Print(str);

		return str;
	}

private:
	unsigned int table_index;
	unsigned int column_index;
	std::string column_name;
};

class SimplestComparisonExpr : public SimplestNode {
public:
	SimplestComparisonExpr(SimplestComparisonType comparison_type, SimplestNodeType node_type)
	    : SimplestNode(node_type), comparison_type(comparison_type) {};
	SimplestComparisonExpr(const SimplestComparisonExpr &other)
	    : SimplestComparisonExpr(other.comparison_type, other.GetNodeType()) {};
	~SimplestComparisonExpr() = default;

	SimplestComparisonType GetSimplestComparisonType() const {
		return comparison_type;
	}

	virtual std::string Print(bool print = true) = 0;

private:
	SimplestComparisonType comparison_type;
};

class SimplestVarComparison : public SimplestComparisonExpr {
public:
	SimplestVarComparison(SimplestComparisonType comparison_type, unique_ptr<SimplestAttr> left_attr,
	                      unique_ptr<SimplestAttr> right_attr)
	    : SimplestComparisonExpr(comparison_type, VarComparisonNode), left_attr(std::move(left_attr)),
	      right_attr(std::move(right_attr)) {};
	~SimplestVarComparison() = default;

	std::string Print(bool print = true) override {
		std::string str;
		std::string comparison_op;
		switch (GetSimplestComparisonType()) {
		case InvalidComparisonType:
			Printer::Print("Invalid Comparison Type!!!");
			return str;
		case Equal:
			comparison_op = " = ";
			break;
		case LessThan:
			comparison_op = " < ";
			break;
		case GreaterThan:
			comparison_op = " > ";
			break;
		case LessEqual:
			comparison_op = " <= ";
			break;
		case GreaterEqual:
			comparison_op = " >= ";
			break;
		case Not:
			comparison_op = " != ";
			break;
		}

		str = left_attr->Print(false);
		str += comparison_op;
		str += right_attr->Print(false);
		str += "\n";

		if (print)
			Printer::Print(str);

		return str;
	}

	unique_ptr<SimplestAttr> left_attr;
	unique_ptr<SimplestAttr> right_attr;
};

class SimplestVarConstComparison : public SimplestComparisonExpr {
public:
	SimplestVarConstComparison(SimplestComparisonType comparison_type, unique_ptr<SimplestAttr> attr,
	                           unique_ptr<SimplestConstVar> const_var)
	    : SimplestComparisonExpr(comparison_type, VarConstComparisonNode), attr(std::move(attr)),
	      const_var(std::move(const_var)) {};
	~SimplestVarConstComparison() = default;

	std::string Print(bool print = true) override {
		std::string str;
		std::string comparison_op;
		switch (GetSimplestComparisonType()) {
		case InvalidComparisonType:
			Printer::Print("Invalid Comparison Type!!!");
			return str;
		case Equal:
			comparison_op = " = ";
			break;
		case LessThan:
			comparison_op = " < ";
			break;
		case GreaterThan:
			comparison_op = " > ";
			break;
		case LessEqual:
			comparison_op = " <= ";
			break;
		case GreaterEqual:
			comparison_op = " >= ";
			break;
		case Not:
			comparison_op = " != ";
			break;
		}

		str = attr->Print(false);
		str += comparison_op;
		str += const_var->Print(false);
		str += "\n";

		if (print)
			Printer::Print(str);

		return str;
	}

	unique_ptr<SimplestAttr> attr;
	unique_ptr<SimplestConstVar> const_var;
};

class SimplestStmt : public SimplestNode {
public:
	SimplestStmt(SimplestNodeType node_type) : SimplestNode(node_type) {};
	SimplestStmt(std::vector<unique_ptr<SimplestStmt>> children, SimplestNodeType node_type)
	    : SimplestNode(node_type), children(std::move(children)) {};
	SimplestStmt(std::vector<unique_ptr<SimplestStmt>> children, std::vector<unique_ptr<SimplestAttr>> target_list,
	             SimplestNodeType node_type)
	    : SimplestNode(node_type), target_list(std::move(target_list)), children(std::move(children)) {};
	SimplestStmt(std::vector<unique_ptr<SimplestAttr>> target_list, SimplestNodeType node_type)
	    : SimplestNode(node_type), target_list(std::move(target_list)) {};
	SimplestStmt(std::vector<unique_ptr<SimplestStmt>> children, std::vector<unique_ptr<SimplestAttr>> target_list,
	             std::vector<unique_ptr<SimplestVarConstComparison>> qual_vec, SimplestNodeType node_type)
	    : SimplestNode(node_type), target_list(std::move(target_list)), children(std::move(children)),
	      qual_vec(std::move(qual_vec)) {};
	SimplestStmt(std::vector<unique_ptr<SimplestAttr>> target_list,
	             std::vector<unique_ptr<SimplestVarConstComparison>> qual_vec, SimplestNodeType node_type)
	    : SimplestNode(node_type), target_list(std::move(target_list)), qual_vec(std::move(qual_vec)) {};
	~SimplestStmt() = default;

	void SimplestAddChild(unique_ptr<SimplestStmt> child) {
		children.emplace_back(std::move(child));
	}

	std::string Print(bool print = true) {
		std::string str;

		str = "\nTarget List:";
		for (size_t i = 0; i < target_list.size(); i++) {
			str += "\n" + target_list[i]->Print(false);
		}

		if (!qual_vec.empty()) {
			str += "\nCondition:";
			for (const auto &qual : qual_vec) {
				str += "\n" + qual->Print(false);
			}
		}

		for (size_t i = 0; i < children.size(); i++) {
			if (children[i]) {
				str += "\nchild[" + std::to_string(i) + "]:";
				str += children[i]->Print(false);
			}
		}

		str += "\n";

		return str;
	};

	std::vector<unique_ptr<SimplestAttr>> target_list;

	// children[0] is the left node, children[1] is the right node
	std::vector<unique_ptr<SimplestStmt>> children;

	// implicitly condition - from postgres
	// todo: need to check if only var-const comparison exists
	std::vector<unique_ptr<SimplestVarConstComparison>> qual_vec;
};

class SimplestAggregate : public SimplestStmt {
public:
	SimplestAggregate(std::vector<unique_ptr<SimplestStmt>> children,
	                  std::vector<unique_ptr<SimplestAttr>> aggregate_columns, std::vector<SimplestVarType> agg_types)
	    : SimplestStmt(std::move(children), std::move(aggregate_columns), AggregateNode), agg_types(agg_types) {};
	SimplestAggregate(std::vector<unique_ptr<SimplestAttr>> aggregate_columns, std::vector<SimplestVarType> agg_types)
	    : SimplestStmt(std::move(aggregate_columns), AggregateNode), agg_types(agg_types) {};
	SimplestAggregate(std::vector<SimplestVarType> agg_types) : SimplestStmt(AggregateNode), agg_types(agg_types) {};
	~SimplestAggregate() = default;

	std::vector<SimplestVarType> GetAggTypes() {
		return agg_types;
	}

	std::string Print(bool print = true) override {
		std::string str = "\n";
		str += "╔══════════════════╗\n";
		D_ASSERT(agg_types.size() == target_list.size());
		size_t attr_num = agg_types.size();
		str += "Aggregate:";

		str += SimplestStmt::Print(false);

		str += "╚══════════════════╝\n";

		if (print)
			Printer::Print(str);

		return str;
	}

private:
	std::vector<SimplestVarType> agg_types;
};

class SimplestJoin : public SimplestStmt {
public:
	SimplestJoin(std::vector<unique_ptr<SimplestStmt>> children,
	             std::vector<unique_ptr<SimplestVarComparison>> join_conditions, SimplestJoinType join_type)
	    : SimplestStmt(std::move(children), JoinNode), join_conditions(std::move(join_conditions)),
	      join_type(join_type) {};
	SimplestJoin(std::vector<unique_ptr<SimplestStmt>> children, SimplestJoinType join_type)
	    : SimplestStmt(std::move(children), JoinNode), join_type(join_type) {};
	SimplestJoin(std::vector<unique_ptr<SimplestVarComparison>> join_conditions, SimplestJoinType join_type)
	    : SimplestStmt(JoinNode), join_conditions(std::move(join_conditions)), join_type(join_type) {};
	~SimplestJoin() = default;

	SimplestJoinType GetSimplestJoinType() const {
		return join_type;
	}

	void AddJoinCondition(std::vector<unique_ptr<SimplestVarComparison>> cond) {
		join_conditions = std::move(cond);
	}

	void AddJoinType(SimplestJoinType type) {
		join_type = type;
	}

	std::string Print(bool print = true) override {
		std::string str = "\n";
		str += "╔══════════════════╗\n";

		size_t attr_num = target_list.size();
		switch (join_type) {
		case InvalidJoinType:
			Printer::Print("Invalid Join Type!!!");
			return str;
		case Inner:
			str += "Inner";
			break;
		case Left:
			str += "Left";
			break;
		case Full:
			str += "Full";
			break;
		case Right:
			str += "Right";
			break;
		case Semi:
			str += "Semi";
			break;
		case Anti:
			str += "Anti";
			break;
		case UniqueOuter:
			str += "UniqueOuter";
			break;
		case UniqueInner:
			str += "UniqueInner";
			break;
		}
		str += " Join:";

		str += "\nJoin Condition:\n";
		for (const auto &cond : join_conditions) {
			str += cond->Print(false);
		}

		str += SimplestStmt::Print(false);

		str += "╚══════════════════╝\n";

		if (print)
			Printer::Print(str);

		return str;
	}

	std::vector<unique_ptr<SimplestVarComparison>> join_conditions;

private:
	SimplestJoinType join_type;
};

class SimplestFilter : public SimplestStmt {
public:
	SimplestFilter(std::vector<unique_ptr<SimplestStmt>> children,
	               std::vector<unique_ptr<SimplestVarConstComparison>> filter_conditions)
	    : SimplestStmt(std::move(children), FilterNode), filter_conditions(std::move(filter_conditions)) {};
	SimplestFilter(std::vector<unique_ptr<SimplestVarConstComparison>> filter_conditions)
	    : SimplestStmt(FilterNode), filter_conditions(std::move(filter_conditions)) {};
	~SimplestFilter() = default;

	std::string Print(bool print = true) override {
		std::string str = "\n";
		str += "╔══════════════════╗\n";

		str += "Filter:";

		str += "\nFilter Condition:\n";
		for (const auto &cond : filter_conditions) {
			str += cond->Print(false);
		}

		str += SimplestStmt::Print(false);
		str += "╚══════════════════╝\n";

		if (print)
			Printer::Print(str);

		return str;
	}

	std::vector<unique_ptr<SimplestVarConstComparison>> filter_conditions;
};

class SimplestScan : public SimplestStmt {
public:
	SimplestScan(unsigned int table_index, std::string table_name, std::vector<unique_ptr<SimplestAttr>> scan_columns)
	    : SimplestStmt(std::move(scan_columns), ScanNode), table_index(table_index), table_name(table_name) {};
	SimplestScan(unsigned int table_index, std::string table_name, std::vector<unique_ptr<SimplestAttr>> scan_columns,
	             std::vector<unique_ptr<SimplestVarConstComparison>> qual_vec)
	    : SimplestStmt(std::move(scan_columns), std::move(qual_vec), ScanNode), table_index(table_index),
	      table_name(table_name) {};
	~SimplestScan() = default;

	unsigned int GetTableIndex() {
		return table_index;
	}
	std::string GetTableName() {
		return table_name;
	}
	void SetTableName(std::string tbl_name) {
		table_name = tbl_name;
	}

	std::string Print(bool print = true) override {
		std::string str = "\n";
		str += "╔══════════════════╗\n";

		str += "Table Scan \"" + table_name + "\":";

		str += SimplestStmt::Print(false);
		str += "╚══════════════════╝\n";

		if (print)
			Printer::Print(str);

		return str;
	}

private:
	unsigned int table_index;
	std::string table_name;
};

class SimplestHash : public SimplestStmt {
public:
	SimplestHash(std::vector<unique_ptr<SimplestAttr>> target_lists, std::vector<unique_ptr<SimplestAttr>> hash_keys)
	    : SimplestStmt(std::move(target_lists), HashNode), hash_keys(std::move(hash_keys)) {};
	SimplestHash(std::vector<unique_ptr<SimplestStmt>> children, std::vector<unique_ptr<SimplestAttr>> target_lists,
	             std::vector<unique_ptr<SimplestAttr>> hash_keys)
	    : SimplestStmt(std::move(children), std::move(target_lists), HashNode), hash_keys(std::move(hash_keys)) {};
	~SimplestHash() = default;

	std::string Print(bool print = true) override {
		std::string str = "\n";
		str += "╔══════════════════╗\n";

		str += "Hash:\nHash Keys:";
		for (const auto &hk : hash_keys) {
			str += "\n" + hk->Print(false);
		}

		str += SimplestStmt::Print(false);
		str += "╚══════════════════╝\n";

		if (print)
			Printer::Print(str);

		return str;
	}

	std::vector<unique_ptr<SimplestAttr>> hash_keys;
};
} // namespace duckdb