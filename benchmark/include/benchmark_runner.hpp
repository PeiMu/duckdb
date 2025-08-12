//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// benchmark_runner.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "benchmark_configuration.hpp"
#include "benchmark.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/fstream.hpp"
#include <thread>

namespace duckdb {
class DuckDB;

//! The benchmark runner class is responsible for running benchmarks
class BenchmarkRunner {
	BenchmarkRunner();

public:
	static constexpr const char *DUCKDB_BENCHMARK_DIRECTORY = "duckdb_benchmark_data";
	BenchmarkConfiguration configuration;

	static BenchmarkRunner &GetInstance() {
		static BenchmarkRunner instance;
		return instance;
	}

	static void InitializeBenchmarkDirectory();

	//! Register a benchmark in the Benchmark Runner, this is done automatically
	//! as long as the proper macro's are used
	static void RegisterBenchmark(Benchmark *benchmark);

	void Log(string message);
	void LogLine(string message);
	void LogResult(string message);
	void LogOutput(string message);

	void RunBenchmark(Benchmark *benchmark);
	void RunBenchmarks();

	vector<Benchmark *> benchmarks;
	ofstream out_file;
	ofstream log_file;
	uint32_t threads = std::thread::hardware_concurrency();

	bool enable_dbshaker_query_split = false;
	bool enable_dbshaker_split_jop = false;
	//! blow is unused yet
	bool merge_back_plan = false;
	bool specify_estimated_card = false;
	bool manual_explain_analyze = false;
	bool whole_plan_manual_explain_analyze = false;
	bool convert_ir_to_duckdb = false;
	bool convert_duckdb_to_ir = false;
	bool convert_ir_to_sql = false;

	uint32_t nruns = 5;
};

} // namespace duckdb
