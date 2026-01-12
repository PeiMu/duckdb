# Build the plan executor tool for comparing optimizer output across DuckDB versions

add_executable(optimizer_comparison plan_executor.cpp)

# Link against DuckDB library
target_link_libraries(optimizer_comparison duckdb_static)

# Link threads (required by DuckDB)
link_threads(optimizer_comparison "")

# Set output directory to build root for easy access
set_target_properties(optimizer_comparison PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})

# Optional: Install the executable
install(TARGETS optimizer_comparison RUNTIME DESTINATION "${INSTALL_BIN_DIR}")