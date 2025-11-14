// Example integration code showing how to use CyclicJoinOptimizer in CreatePreparedStatementInternal
// This is a REFERENCE implementation - you'll need to integrate these pieces into client_context.cpp

// 1. Add include at the top of client_context.cpp:
// #include "duckdb/optimizer/query_split/cyclic_join_optimizer.hpp"

// 2. Add this code BEFORE the optimizer.PreOptimize() call (around line 465):

	// Create cyclic join optimizer instance
	CyclicJoinOptimizer cyclic_optimizer;

	// Collect all join conditions from the plan
	if (plan) {
		cyclic_optimizer.CollectJoinConditions(*plan);

		// Detect cycles for debugging/logging
		auto cycles = cyclic_optimizer.DetectCycles();
		#if ENABLE_DEBUG_PRINT
		if (execute_plan && !cycles.empty()) {
			Printer::Print("Detected " + std::to_string(cycles.size()) + " cyclic join pattern(s)");
		}
		#endif
	}

// 3. Add this code in the query splitting loop, AFTER creating sub_plan (around line 647-652):

	// Inside the while loop where you process subqueries:
	while (config.enable_dbshaker_query_split && !config.convert_ir_to_duckdb && execute_plan) {
		// ... existing code ...

		// After this line:
		// auto sub_plan = subquery_preparer.GenerateProjHead(plan, std::move(subqueries.front()[0]),
		//                                                    table_expr_queue, proj_expr, merge_sibling_expr);

		// Add the following:

		// Record which column equalities are guaranteed in this sub_plan
		// The temp table will be created with a new table index
		// You need to track what that index will be - this depends on your implementation
		idx_t temp_table_idx = subquery_preparer.GetNewTableIndex(); // or however you track this

		if (sub_plan) {
			cyclic_optimizer.RecordTempTableEqualities(temp_table_idx, *sub_plan);
		}

		// ... rest of subquery execution ...

		// IMPORTANT: After executing the subquery and creating the temp table,
		// but BEFORE merging back into the main plan, remove redundant filters:

		// Build a set of all temp table indices created so far
		std::unordered_set<idx_t> temp_table_indices;
		temp_table_indices.insert(temp_table_idx); // Add current temp table
		// You may need to track previous temp tables too, depending on your use case

		// Remove redundant filters from the remaining plan
		if (plan) {
			bool filters_removed = cyclic_optimizer.RemoveRedundantFilters(*plan, temp_table_indices);
			#if ENABLE_DEBUG_PRINT
			if (execute_plan && filters_removed) {
				Printer::Print("Removed redundant cyclic join filters from remaining plan");
			}
			#endif
		}

		// ... continue with existing merge logic ...
	}

// 4. ALTERNATIVE APPROACH - Simpler integration if you just want to remove filters after each subquery:

	// After line 647 where sub_plan is created:
	auto sub_plan = subquery_preparer.GenerateProjHead(plan, std::move(subqueries.front()[0]),
	                                                   table_expr_queue, proj_expr, merge_sibling_expr);

	// Track the temp table index (you need to determine this based on your code)
	// Option 1: Get it from subquery_preparer
	idx_t temp_table_idx = subquery_preparer.GetNewTableIndex();

	// Option 2: If you know the temp table will be created with a specific name/index pattern
	// idx_t temp_table_idx = ...; // however you track this

	// Record equalities in the sub_plan
	if (sub_plan) {
		cyclic_optimizer.RecordTempTableEqualities(temp_table_idx, *sub_plan);
	}

	// Later, before merging (before line 671 or wherever you merge):
	std::unordered_set<idx_t> temp_tables = {temp_table_idx};
	cyclic_optimizer.RemoveRedundantFilters(*plan, temp_tables);

// ============================================================================
// COMPLETE EXAMPLE showing the key section with integration:
// ============================================================================

void IntegrationExample(unique_ptr<LogicalOperator> &plan,
                        SubqueryPreparer &subquery_preparer,
                        ClientContext &context,
                        bool execute_plan,
                        subquery_queue &subqueries,
                        table_expr_info &table_expr_queue,
                        std::vector<TableExpr> &proj_expr,
                        bool merge_sibling_expr) {

	// Initialize the cyclic join optimizer
	CyclicJoinOptimizer cyclic_optimizer;

	// Collect join conditions from the initial plan
	if (plan) {
		cyclic_optimizer.CollectJoinConditions(*plan);
	}

	// Track all temp table indices created
	std::unordered_set<idx_t> all_temp_table_indices;

	// In your query splitting loop:
	while (!subqueries.empty() && execute_plan) {
		// Clear old table index and add current subquery's table index
		subquery_preparer.ClearOldTableIndex();
		subquery_preparer.AddOldTableIndex(subqueries.front()[0]);

		// Generate the sub_plan
		auto sub_plan = subquery_preparer.GenerateProjHead(plan, std::move(subqueries.front()[0]),
		                                                   table_expr_queue, proj_expr, merge_sibling_expr);

		subqueries.pop_front();
		table_expr_queue.pop_front();

		// Get the temp table index that will be created for this sub_plan
		// NOTE: This depends on your implementation - you need to know what table index
		// will be assigned to the temp table when it's created
		idx_t temp_table_idx = subquery_preparer.GetNewTableIndex(); // Example - adjust as needed

		// Record which column equalities are guaranteed in this sub_plan
		if (sub_plan) {
			cyclic_optimizer.RecordTempTableEqualities(temp_table_idx, *sub_plan);
			all_temp_table_indices.insert(temp_table_idx);
		}

		// Execute the sub_plan and create temp table
		// ... your execution code here ...

		// CRITICAL: Before merging the temp table back into the remaining plan,
		// remove redundant filters that check equalities already guaranteed in temp tables
		if (plan) {
			bool modified = cyclic_optimizer.RemoveRedundantFilters(*plan, all_temp_table_indices);

			#if ENABLE_DEBUG_PRINT
			if (execute_plan && modified) {
				Printer::Print("Removed redundant cyclic join conditions from remaining plan");
				plan->Print();
			}
			#endif
		}

		// Continue with your existing merge logic
		// plan = subquery_preparer.MergeSubquery(plan, ...);
	}
}

// ============================================================================
// NOTES:
// ============================================================================
//
// 1. You need to track the temp table index correctly. This depends on how your
//    code assigns table indices to temp tables created from sub_plan execution.
//
// 2. The key is to call RemoveRedundantFilters() AFTER executing sub_plan but
//    BEFORE merging it back into the remaining plan.
//
// 3. The optimizer works by:
//    a) Recording which joins were in the sub_plan (these create guaranteed equalities)
//    b) Scanning the remaining plan for filters that check these same equalities
//    c) Removing those redundant filters
//
// 4. You may need to extend SubqueryPreparer to expose GetNewTableIndex() if it
//    doesn't already have such a method.
