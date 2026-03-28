cmake_minimum_required(VERSION 3.12)

set(TARGET_NAME osmium)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

# libosmium and protozero are header-only.
find_path(LIBOSMIUM_INCLUDE_DIR osmium/version.hpp REQUIRED)
find_path(PROTOZERO_INCLUDE_DIR protozero/version.hpp REQUIRED)

# zlib (required for PBF decompression)
find_package(ZLIB REQUIRED)

# expat (required for XML format support)
find_package(EXPAT REQUIRED)

# Threading (required by libosmium for parallel PBF block decompression)
find_package(Threads REQUIRED)

set(EXTENSION_SOURCES
    src/osmium_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

foreach(EXT_TARGET ${EXTENSION_NAME} ${LOADABLE_EXTENSION_NAME})
    # libosmium requires C++17
    set_target_properties(${EXT_TARGET} PROPERTIES CXX_STANDARD 17)
    target_include_directories(${EXT_TARGET} SYSTEM PRIVATE
        ${LIBOSMIUM_INCLUDE_DIR}
        ${PROTOZERO_INCLUDE_DIR})
    target_link_libraries(${EXT_TARGET} ZLIB::ZLIB EXPAT::EXPAT Threads::Threads)
endforeach()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
