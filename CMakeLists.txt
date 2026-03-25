cmake_minimum_required(VERSION 3.16)
project(pg_delta_lake_cdc VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Find dependencies
find_package(PostgreSQL REQUIRED)
find_package(Arrow REQUIRED)

# Try to find Parquet
find_package(Parquet)
if (NOT Parquet_FOUND)
    # Arrow includes Parquet if built with Parquet support, often finding Parquet via Arrow is sufficient.
    message(STATUS "Parquet separate package not found, relying on Arrow's internal Parquet support.")
endif()

add_executable(pg_delta_lake_cdc
    src/main.cpp
    src/NetworkReceiver.cpp
    src/ParquetWriter.cpp
)

target_include_directories(pg_delta_lake_cdc PRIVATE 
    src
    ${PostgreSQL_INCLUDE_DIRS}
)

target_link_libraries(pg_delta_lake_cdc PRIVATE
    ${PostgreSQL_LIBRARIES}
    Arrow::arrow_shared
    Threads::Threads
)

if (TARGET Parquet::parquet_shared)
    target_link_libraries(pg_delta_lake_cdc PRIVATE Parquet::parquet_shared)
endif()
