#include "duckdb.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include <chrono>
#include <iostream>
#include <regex>

using namespace duckdb;

struct PlanMetadata {
	std::string version;
	size_t split_index;
	std::string filename;
};

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

	// Parse filenames: logical_plan_v0.6.1_split_0.bin
	std::regex filename_pattern(R"(logical_plan_(v\d+\.\d+\.\d+)_split_(\d+)\.bin)");

	std::vector<PlanMetadata> plans;

	// Use DuckDB's FileSystem API with callback
	fs.ListFiles(plan_dir, [&](const std::string &filename, bool is_dir) {
		// Skip directories
		if (is_dir) {
			return;
		}

		// Skip non-.bin files
		if (filename.size() < 4 || filename.substr(filename.size() - 4) != ".bin") {
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

	unique_ptr<QueryResult> final_result;

	for (const auto &plan_meta : plans) {
		try {
			// Start transaction for deserialization (needed for catalog access)
			conn.BeginTransaction();

			// Deserialize
			BufferedFileReader reader(fs, plan_meta.filename.c_str());
			BinaryDeserializer deserializer(reader);
			deserializer.Set<ClientContext &>(*conn.context);

			deserializer.Begin();
			auto logical_plan = LogicalOperator::Deserialize(deserializer);
			deserializer.End();

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
			} else {
				std::cout << plan_meta.version << ", " << plan_meta.split_index << ", " << execute_time << ", "
				          << "SUCCESS\n";

				// Save the last successful result for final output
				final_result = std::move(result);

				// Commit successful execution to close transaction
				try {
					conn.Commit();
				} catch (...) {
					// Transaction might be auto-committed, ignore errors
				}
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
	if (final_result && !final_result->HasError()) {
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
