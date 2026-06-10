cmake_minimum_required(VERSION 3.24)
project(McPike)

include(ExternalProject)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# --- BOSS dependency -------------------------------------------------------
# By default, clone and build BOSS from GitHub (mirroring how BOSS itself pulls
# its engines). For local development against an existing BOSS checkout, pass
# -DBOSS_SOURCE_DIR=/path/to/BOSS (built in <dir>/Release) to skip cloning.
set(BOSS_GIT_REPOSITORY "https://github.com/symbol-store/BOSS"
  CACHE STRING "BOSS git repository to fetch")
set(BOSS_GIT_TAG "main" CACHE STRING "BOSS branch/tag/commit to build")
set(BOSS_SOURCE_DIR "" CACHE PATH
  "Optional existing BOSS checkout (built in <dir>/Release); skips cloning")
set(BOSS_DEFAULT_ENGINES "" CACHE STRING
  "Engines to build into BOSS, forwarded to the BOSS sub-build (e.g. \"FITSDKEngine;ArrowComputeEngine\")")
set(BOSS_BUILD_ENGINES "" CACHE STRING
  "Engine repos to clone+build, forwarded to the BOSS sub-build (Namespace/Name:Branch list)")
set(GITHUB_TOKEN "$ENV{GITHUB_TOKEN}" CACHE STRING
  "GitHub token forwarded to BOSS for fetching private engines (optional)")

find_program(MAKE_EXECUTABLE NAMES gmake mingw32-make make NAMES_PER_DIR DOC "GNU Make")

if(BOSS_SOURCE_DIR)
  # Use an existing local BOSS checkout, already built in <dir>/Release.
  set(BOSS_SRC_DIR "${BOSS_SOURCE_DIR}")
  set(BOSS_BUILD_DIR "${BOSS_SOURCE_DIR}/Release")
  set(BOSS_FETCHED FALSE)
else()
  # Clone and build BOSS via ExternalProject, the same way libmicrohttpd and
  # nlohmann-json are vendored below.
  set(BOSS_SRC_DIR "${CMAKE_BINARY_DIR}/boss-src")
  set(BOSS_BUILD_DIR "${CMAKE_BINARY_DIR}/boss-build")
  # CMAKE_ARGS splits on ';', so re-encode the engine lists with LIST_SEPARATOR.
  string(REPLACE ";" "|" BOSS_DEFAULT_ENGINES_ARG "${BOSS_DEFAULT_ENGINES}")
  string(REPLACE ";" "|" BOSS_BUILD_ENGINES_ARG "${BOSS_BUILD_ENGINES}")
  ExternalProject_Add(BOSS
    GIT_REPOSITORY ${BOSS_GIT_REPOSITORY}
    GIT_TAG ${BOSS_GIT_TAG}
    GIT_SHALLOW TRUE
    DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
    SOURCE_DIR ${BOSS_SRC_DIR}
    BINARY_DIR ${BOSS_BUILD_DIR}
    LIST_SEPARATOR "|"
    CMAKE_ARGS
      -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
      -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
      -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
      -DBOSS_DEFAULT_ENGINES=${BOSS_DEFAULT_ENGINES_ARG}
      -DBOSS_BUILD_ENGINES=${BOSS_BUILD_ENGINES_ARG}
      -DGITHUB_TOKEN=${GITHUB_TOKEN}
    INSTALL_COMMAND ""
    LOG_CONFIGURE ON
    LOG_BUILD ON
    LOG_INSTALL ON
    LOG_OUTPUT_ON_FAILURE ON
    BUILD_BYPRODUCTS
      ${BOSS_BUILD_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}BOSS${CMAKE_SHARED_LIBRARY_SUFFIX}
      ${BOSS_BUILD_DIR}/deps/lib/${CMAKE_STATIC_LIBRARY_PREFIX}chibi-scheme${CMAKE_STATIC_LIBRARY_SUFFIX}
  )
  set(BOSS_FETCHED TRUE)
endif()

set(BOSS_LIBRARY
  ${BOSS_BUILD_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}BOSS${CMAKE_SHARED_LIBRARY_SUFFIX})
set(CHIBI_LIBRARY
  ${BOSS_BUILD_DIR}/deps/lib/${CMAKE_STATIC_LIBRARY_PREFIX}chibi-scheme${CMAKE_STATIC_LIBRARY_SUFFIX})

ExternalProject_Add(libmicrohttpd
  URL https://ftp.gnu.org/gnu/libmicrohttpd/libmicrohttpd-0.9.77.tar.gz
  URL_HASH SHA256=9e7023a151120060d2806a6ea4c13ca9933ece4eacfc5c9464d20edddb76b0a0
  DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
  DOWNLOAD_EXTRACT_TIMESTAMP NEW
  CONFIGURE_COMMAND <SOURCE_DIR>/configure
    CC=${CMAKE_C_COMPILER}
    --prefix=${CMAKE_BINARY_DIR}/deps
    --disable-shared
    --enable-static
    --disable-doc
    --disable-examples
    --without-gnutls
  BUILD_COMMAND ${MAKE_EXECUTABLE}
  INSTALL_COMMAND ${MAKE_EXECUTABLE} install
  LOG_CONFIGURE ON
  LOG_BUILD ON
  LOG_INSTALL ON
  LOG_OUTPUT_ON_FAILURE ON
  BUILD_BYPRODUCTS ${CMAKE_BINARY_DIR}/deps/lib/libmicrohttpd.a
)

ExternalProject_Add(nlohmann-json
  URL https://github.com/nlohmann/json/archive/refs/tags/v3.11.3.tar.gz
  URL_HASH SHA256=0d8ef5af7f9794e3263480193c491549b2ba6cc74bb018906202ada498a79406
  DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
  DOWNLOAD_EXTRACT_TIMESTAMP NEW
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ""
  INSTALL_COMMAND ${CMAKE_COMMAND} -E copy
    <SOURCE_DIR>/single_include/nlohmann/json.hpp
    ${CMAKE_BINARY_DIR}/deps/include/nlohmann/json.hpp
  LOG_CONFIGURE ON
  LOG_BUILD ON
  LOG_INSTALL ON
  LOG_OUTPUT_ON_FAILURE ON
  BUILD_BYPRODUCTS ${CMAKE_BINARY_DIR}/deps/include/nlohmann/json.hpp
)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_executable(McPike Source/McPike.cpp)
add_dependencies(McPike libmicrohttpd nlohmann-json)
if(BOSS_FETCHED)
  add_dependencies(McPike BOSS)
endif()

target_include_directories(McPike PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/Source)
target_include_directories(McPike SYSTEM PRIVATE
  ${BOSS_SRC_DIR}/Source
  ${BOSS_BUILD_DIR}/deps/include
  ${CMAKE_BINARY_DIR}/deps/include)

target_link_libraries(McPike
  ${BOSS_LIBRARY}
  ${CHIBI_LIBRARY}
  ${CMAKE_BINARY_DIR}/deps/lib/libmicrohttpd.a
  Threads::Threads)

set_target_properties(McPike PROPERTIES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED ON
  CXX_EXTENSIONS OFF
  BUILD_RPATH ${BOSS_BUILD_DIR})

if(APPLE)
  target_link_libraries(McPike "-Wl,-export_dynamic")
endif()
if(UNIX AND NOT APPLE)
  target_link_libraries(McPike dl rt)
endif()

install(TARGETS McPike RUNTIME DESTINATION bin)
install(DIRECTORY launchd DESTINATION share/McPike)
