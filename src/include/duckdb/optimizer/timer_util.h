//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/timer_util.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/printer.hpp"
#include <chrono>
#include <fstream>

namespace duckdb {
timespec tic();

void toc(timespec *start_time, const char *prefix);

std::chrono::high_resolution_clock::time_point chrono_tic();

long chrono_toc(std::chrono::high_resolution_clock::time_point* start_time, const char* prefix, bool print=true);

void appendLineToFile(string filepath, string line);
} // namespace duckdb
