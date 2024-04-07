//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/foreign_key_center.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/query_split/top_down.hpp"
#include "duckdb/optimizer/query_split/foreign_key_center.hpp"
#include "duckdb/optimizer/query_split/split_algorithm.hpp"

namespace duckdb {

class SplitAlgorithmFactor {
public:
	SplitAlgorithmFactor() = default;
	~SplitAlgorithmFactor() = default;

	static std::unique_ptr<SplitAlgorithm> CreateSplitter(ClientContext &context, EnumSplitAlgorithm split_algorithm) {
		if (foreign_key_center == split_algorithm || min_sub_query == split_algorithm) {
			return std::unique_ptr<ForeignKeyCenterSplit>(new ForeignKeyCenterSplit(context, split_algorithm));
		} else if (top_down == split_algorithm) {
			return std::unique_ptr<TopDownSplit>(new TopDownSplit(context));
		} else {
			return std::unique_ptr<SplitAlgorithm>(new SplitAlgorithm(context));
		}
	};
};

} // namespace duckdb