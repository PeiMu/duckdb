# Build the plan executor tool for comparing optimizer output across DuckDB versions

add_executable(optimizer_comparison plan_executor.cpp)

# Add IR_SQL_Converter include directory
target_include_directories(optimizer_comparison PRIVATE
        ${CMAKE_CURRENT_LIST_DIR}/../../../IR_SQL_Converter/inc)

# Link against DuckDB library and IR_SQL_Converter
target_link_libraries(optimizer_comparison
    duckdb_static
        ${CMAKE_CURRENT_LIST_DIR}/../../../IR_SQL_Converter/lib/libIR_SQL_Converter_static.a)

# Link threads (required by DuckDB)
link_threads(optimizer_comparison "")

# Set output directory to build root for easy access
set_target_properties(optimizer_comparison PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})

# Optional: Install the executable
install(TARGETS optimizer_comparison RUNTIME DESTINATION "${INSTALL_BIN_DIR}")