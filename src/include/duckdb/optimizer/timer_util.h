//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/timer_util.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/printer.hpp"

namespace duckdb {
timespec tic();

void toc(timespec *start_time, const char *prefix);
} // namespace duckdb
