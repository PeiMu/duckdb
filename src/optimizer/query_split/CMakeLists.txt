add_library_unity(
  duckdb_optimizer_query_split
  OBJECT
  query_split.cpp
        foreign_key_center.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_optimizer_query_split>
    PARENT_SCOPE)
