#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalColumnDataGet &op) {
	D_ASSERT(op.children.empty());
	D_ASSERT(op.collection);
	auto &scan = Make<PhysicalColumnDataScan>(op.types, PhysicalOperatorType::COLUMN_DATA_SCAN,
	                                          op.estimated_cardinality, std::move(op.collection));
	auto &cds = scan.Cast<PhysicalColumnDataScan>();
	cds.logical_table_index = op.table_index;
	if (op.dynamic_filters) {
		cds.dynamic_filters = op.dynamic_filters;
	}
	return scan;
}

} // namespace duckdb
