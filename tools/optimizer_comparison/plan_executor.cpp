#include "duckdb.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_explain.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "ir_to_duckdb_plan.h"
#include "cpp_interface.h"
#include "simplest_ir.h"
#include <chrono>
#include <iostream>
#include <regex>
#include <memory>

using namespace duckdb;

#define FROM_BINARY false
#define PRINT_PHYSICAL_PLAN false
#define RUN_EXPLAIN_ANALYZE false

struct PlanMetadata {
	std::string version;
	size_t split_index;
	std::string filename;
};

// Helper to get output table_index from IR (usually the top Projection's index)
idx_t GetOutputTableIndex(const std::unique_ptr<ir_sql_converter::SimplestStmt> &ir) {
	if (ir->GetNodeType() == ir_sql_converter::ProjectionNode) {
		return ir->Cast<ir_sql_converter::SimplestProjection>().GetIndex();
	}
	// For other top-level nodes, we might need different handling
	// For now, throw an error if we can't determine it
	throw std::runtime_error("Cannot determine output table_index from IR - top node is not a Projection");
}

// Helper to materialize query result into ColumnDataCollection
unique_ptr<ColumnDataCollection> MaterializeResult(ClientContext &context, unique_ptr<QueryResult> &result) {
	if (!result || result->HasError()) {
		std::runtime_error("error result!!!");
		return nullptr;
	}

	auto &materialized = result->Cast<MaterializedQueryResult>();
	auto collection = make_uniq<ColumnDataCollection>(context, materialized.types);

	ColumnDataAppendState append_state;
	collection->InitializeAppend(append_state);

	for (auto &chunk : materialized.Collection().Chunks()) {
		collection->Append(append_state, chunk);
	}

	return collection;
}

// Run EXPLAIN ANALYZE on a logical plan to get execution statistics
void RunExplainAnalyze(Connection &conn, unique_ptr<LogicalOperator> logical_plan, const PlanMetadata &plan_meta) {
	// Wrap the plan in LogicalExplain with EXPLAIN_ANALYZE
	auto explain_plan = make_uniq<LogicalExplain>(std::move(logical_plan), ExplainType::EXPLAIN_ANALYZE,
	                                               ExplainFormat::DEFAULT);

	// Run through optimizer (PostOptimize) - need to create a Binder first
	auto binder = Binder::CreateBinder(*conn.context);
	Optimizer optimizer(*binder, *conn.context);
	explain_plan = unique_ptr_cast<LogicalOperator, LogicalExplain>(optimizer.PostOptimize(std::move(explain_plan)));

	// Resolve types
	explain_plan->ResolveOperatorTypes();

	// Get column names and types
	vector<string> names = {"explain_key", "explain_value"};
	vector<LogicalType> types = {LogicalType::VARCHAR, LogicalType::VARCHAR};

	// Generate physical plan
	PhysicalPlanGenerator physical_planner(*conn.context);
	auto physical_plan = physical_planner.Plan(std::move(explain_plan));

	// Create and execute
	auto prepared = make_shared_ptr<PreparedStatementData>(StatementType::EXPLAIN_STATEMENT);
	prepared->names = std::move(names);
	prepared->types = std::move(types);
	prepared->physical_plan = std::move(physical_plan);

	auto select_stmt = make_uniq<SelectStatement>();
	auto select_node = make_uniq<SelectNode>();
	select_stmt->node = std::move(select_node);
	prepared->unbound_statement = std::move(select_stmt);

	case_insensitive_map_t<BoundParameterData> values;
	auto result = conn.context->Execute("", prepared, values, false);

	if (result && !result->HasError()) {
		Printer::Print("=== EXPLAIN ANALYZE for " + plan_meta.version + " split " + std::to_string(plan_meta.split_index) + " ===");
		result->Print();
	} else if (result) {
		std::cerr << "EXPLAIN ANALYZE error: " << result->GetError() << "\n";
	}
}

unique_ptr<ColumnDataCollection> RunLogicalPlanWithExecuteRow(Connection &conn, unique_ptr<LogicalOperator> logical_plan,
                                                              const PlanMetadata &plan_meta, bool print_physical_plan = false) {
	// Resolve types
	logical_plan->ResolveOperatorTypes();

	// Get column names and types from the plan
	vector<string> names;
	vector<LogicalType> types;
	for (auto &expr : logical_plan->expressions) {
		names.push_back(expr->alias);
		types.push_back(expr->return_type);
	}
	if (names.empty() && !logical_plan->types.empty()) {
		// Fallback to plan types if no expressions
		types = logical_plan->types;
		for (idx_t i = 0; i < types.size(); i++) {
			names.push_back("col" + std::to_string(i));
		}
	}

	// Generate physical plan WITHOUT running optimizer
	PhysicalPlanGenerator physical_planner(*conn.context);
	auto physical_plan = physical_planner.Plan(std::move(logical_plan));

	// Print physical plan before execution (if requested)
	if (print_physical_plan) {
		Printer::Print("=== Physical Plan for " + plan_meta.version + " split " + std::to_string(plan_meta.split_index) + " ===");
		physical_plan->Root().Print();
	}

	// Create PreparedStatementData with the pre-built plan
	auto prepared_data = make_shared_ptr<PreparedStatementData>(StatementType::SELECT_STATEMENT);
	prepared_data->names = std::move(names);
	prepared_data->types = std::move(types);
	prepared_data->physical_plan = std::move(physical_plan);

	// Create a dummy unbound_statement (required by PreparedStatementData)
	auto select_stmt = make_uniq<SelectStatement>();
	auto select_node = make_uniq<SelectNode>();
	select_stmt->node = std::move(select_node);
	prepared_data->unbound_statement = std::move(select_stmt);

	// Create PreparedStatement object (like in client_context.cpp)
	case_insensitive_map_t<idx_t> named_param_map;
	auto prepared_stmt = make_uniq<PreparedStatement>(conn.context->shared_from_this(), std::move(prepared_data),
	                                                  "", named_param_map);

	// Execute using ExecuteRow (public API without lock)
	duckdb::vector<Value> bound_values;

	auto timer = chrono_tic();
	unique_ptr<ColumnDataCollection> result = prepared_stmt->ExecuteRow(bound_values, false);
	auto execute_time = chrono_toc(&timer, "Optimizer Comparison Tool Execute time is, ", false);

	// save time to a file
	std::ofstream log_file;
	log_file.open("optimizer_comparison_time_log.csv", std::ios_base::app);
	log_file << std::to_string(execute_time / 1000) + ", ";
	log_file.close();

	if (result) {
		std::cout << plan_meta.version << ", " << plan_meta.split_index << ", " << execute_time << ", "
		          << "SUCCESS, rows=" << result->Count() << "\n";

		// Commit successful execution to close transaction
		try {
			conn.Commit();
		} catch (...) {
			// Transaction might be auto-committed, ignore errors
		}
	} else {
		std::cout << plan_meta.version << ", " << plan_meta.split_index << ", "
		          << "ERROR, null result\n";

		// Rollback on error
		try {
			conn.Rollback();
		} catch (...) {
			// Ignore rollback errors
		}
	}
	return result;
}

unique_ptr<QueryResult> RunLogicalPlan(Connection &conn, unique_ptr<LogicalOperator> logical_plan,
                                       const PlanMetadata &plan_meta) {
	// Resolve types
	logical_plan->ResolveOperatorTypes();

	// Get column names and types from the plan
	vector<string> names;
	vector<LogicalType> types;
	for (auto &expr : logical_plan->expressions) {
		names.push_back(expr->alias);
		types.push_back(expr->return_type);
	}
	if (names.empty() && !logical_plan->types.empty()) {
		// Fallback to plan types if no expressions
		types = logical_plan->types;
		for (idx_t i = 0; i < types.size(); i++) {
			names.push_back("col" + std::to_string(i));
		}
	}

	// Generate physical plan WITHOUT running optimizer
	PhysicalPlanGenerator physical_planner(*conn.context);
	auto physical_plan = physical_planner.Plan(std::move(logical_plan));

	// Create PreparedStatementData with the pre-built plan
	auto prepared = make_shared_ptr<PreparedStatementData>(StatementType::SELECT_STATEMENT);
	prepared->names = std::move(names);
	prepared->types = std::move(types);
	prepared->physical_plan = std::move(physical_plan);

	// Create a dummy unbound_statement (required by PreparedStatementData)
	auto select_stmt = make_uniq<SelectStatement>();
	auto select_node = make_uniq<SelectNode>();
	select_stmt->node = std::move(select_node);
	prepared->unbound_statement = std::move(select_stmt);

	// Execute the prepared statement (commits the transaction)
	case_insensitive_map_t<BoundParameterData> values;
	// todo: provide correct SQL string by the IR_SQL_Converter
	auto timer = chrono_tic();
	auto result = conn.context->Execute("", prepared, values, false);

	auto execute_time = chrono_toc(&timer, "Optimizer Comparison Tool Execute time is, ", false);
	// save time to a file
	std::ofstream log_file;
	log_file.open("optimizer_comparison_time_log.csv", std::ios_base::app);
	log_file << std::to_string(execute_time / 1000) + ", ";
	log_file.close();

	if (result->HasError()) {
		std::cout << plan_meta.version << ", " << plan_meta.split_index << ", "
		          << "ERROR, "
		          << "\"" << result->GetError() << "\"\n";

		// Rollback on error
		try {
			conn.Rollback();
		} catch (...) {
			// Ignore rollback errors
		}
		return nullptr;
	} else {
		std::cout << plan_meta.version << ", " << plan_meta.split_index << ", " << execute_time << ", "
		          << "SUCCESS\n";

		// Commit successful execution to close transaction
		try {
			conn.Commit();
		} catch (...) {
			// Transaction might be auto-committed, ignore errors
		}
		return result;
	}
}

int main(int argc, char **argv) {
	if (argc < 3) {
		std::cerr << "Usage: " << argv[0] << " <db_path> <plan_dir>\n";
		return 1;
	}

	std::string db_path = argv[1];
	std::string plan_dir = argv[2];

	DuckDB db(db_path);
	Connection conn(db);
	auto &fs = FileSystem::GetFileSystem(*conn.context);

#if FROM_BINARY
	// Parse filenames: e.g., logical_plan_v0.6.1_split_0.bin
	std::regex filename_pattern(R"(logical_plan_(v\d+\.\d+\.\d+)_split_(\d+)\.bin)");
#else
	// Parse filenames: e.g., logical_plan_v0.6.1_split_0.ir
	std::regex filename_pattern(R"(logical_plan_(v\d+\.\d+\.\d+)_split_(\d+)\.ir)");
#endif

	std::vector<PlanMetadata> plans;

	// Use DuckDB's FileSystem API with callback
	fs.ListFiles(plan_dir, [&](const std::string &filename, bool is_dir) {
		// Skip directories
		if (is_dir) {
			return;
		}

		// Skip non-.ir files
		if (filename.size() < 3 || filename.substr(filename.size() - 3) != ".ir") {
			return;
		}

		std::smatch match;
		if (std::regex_match(filename, match, filename_pattern)) {
			PlanMetadata meta;
			meta.version = match[1].str();
			meta.split_index = std::stoull(match[2].str());
			meta.filename = fs.JoinPath(plan_dir, filename);
			plans.push_back(meta);
		}
	});

	// Sort by version, then split_index
	std::sort(plans.begin(), plans.end(), [](const PlanMetadata &a, const PlanMetadata &b) {
		if (a.version != b.version) {
			return a.version < b.version;
		}
		return a.split_index < b.split_index;
	});

	std::cout << "Version, SplitIndex, ExecutionTime(us), Status\n";

	unique_ptr<ColumnDataCollection> final_result;

	// Track intermediate results from previous subplans (same-engine execution)
	std::unordered_map<idx_t, unique_ptr<ColumnDataCollection>> intermediate_results;

	for (size_t i = 0; i < plans.size(); i++) {
		const auto &plan_meta = plans[i];
		bool is_last_subplan = (i == plans.size() - 1);
		try {
			//  Start transaction (needed for catalog access)
			conn.BeginTransaction();
#if FROM_BINARY
			// Deserialize
			BufferedFileReader reader(fs, plan_meta.filename);
			BinaryDeserializer deserializer(reader);
			deserializer.Set<ClientContext &>(*conn.context);

			deserializer.Begin();
			auto logical_plan = LogicalOperator::Deserialize(deserializer);
			deserializer.End();
#ifdef DEBUG
			// print out
			Printer::Print("Deserialize:");
#endif
#else
			// Load SimplestIR from file
			auto simplest_ir = ir_sql_converter::LoadSimplestIRFromFile(plan_meta.filename);
#ifdef DEBUG
			// print out
			Printer::Print("LoadSimplestIRFromFile:");
			simplest_ir->Print();
#endif

			// Get output table_index before converting (needed for storing intermediate result)
			// fixme: it is a temp impl
			idx_t output_table_idx = GetOutputTableIndex(simplest_ir) + 1;

			// Convert SimplestIR back to DuckDB logical plan
			auto binder = Binder::CreateBinder(*conn.context);
			auto logical_plan =
			    ir_sql_converter::ConvertIRToDuckDBPlan(*binder, *conn.context, simplest_ir, &intermediate_results);

			// fixme: necessary optimizer
			Optimizer optimizer(*binder, *conn.context);
			if ("v1.3.2" == plan_meta.version) {
				logical_plan = optimizer.TestOptimize(std::move(logical_plan), false);
			} else {
				logical_plan = optimizer.TestOptimize(std::move(logical_plan), true);
			}
#ifdef DEBUG
			// print out
			Printer::Print("ConvertIRToDuckDBPlan:");
#endif

#endif

#ifdef DEBUG
			logical_plan->Print();
#endif


#if RUN_EXPLAIN_ANALYZE
			// Run EXPLAIN ANALYZE on a copy to see execution statistics
			{
				auto explain_plan_copy = logical_plan->Copy(*conn.context);
				RunExplainAnalyze(conn, std::move(explain_plan_copy), plan_meta);
				// Commit and start new transaction for actual execution
				try { conn.Commit(); } catch (...) {}
				conn.BeginTransaction();
			}
#endif

			// Execute the plan using ExecuteRow (returns ColumnDataCollection directly)
			auto result = RunLogicalPlanWithExecuteRow(conn, std::move(logical_plan), plan_meta, PRINT_PHYSICAL_PLAN);

			// If not the last subplan, store the result for next subplan
			if (!is_last_subplan && result) {
				auto row_count = result->Count();
				intermediate_results[output_table_idx] = std::move(result);
#ifdef DEBUG
				Printer::Print("Stored intermediate result at table_index " + std::to_string(output_table_idx) +
				               " with " + std::to_string(row_count) + " rows");
#endif
			}

			// Store the final result
			if (is_last_subplan) {
				final_result = std::move(result);
			}

		} catch (std::exception &e) {
			std::cerr << "Error processing " << plan_meta.filename << ": " << e.what() << '\n';

			// Try to rollback if we're in a transaction
			try {
				conn.Rollback();
			} catch (...) {
				// Ignore rollback errors
			}

			std::cout << plan_meta.version << "," << plan_meta.split_index << ","
			          << "0,ERROR,0,UNKNOWN,"
			          << "\"" << e.what() << "\"\n";
		}
	}

	// Print the final query result
	if (final_result) {
		std::cout << "\n=== FINAL QUERY RESULT ===\n";
		final_result->Print();

		// save \n to the file
		std::ofstream log_file;
		log_file.open("optimizer_comparison_time_log.csv", std::ios_base::app);
		log_file << "\n";
		log_file.close();
	}

	return 0;
}
