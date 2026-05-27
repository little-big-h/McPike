cmake_minimum_required(VERSION 3.24)
project(McPass)

include(ExternalProject)

set(BOSS_SOURCE_DIR "" CACHE PATH "Path to BOSS source tree")
if(NOT BOSS_SOURCE_DIR)
  message(FATAL_ERROR "BOSS_SOURCE_DIR must be set to the BOSS source directory")
endif()
set(BOSS_BINARY_DIR "${BOSS_SOURCE_DIR}/Release" CACHE PATH "Path to BOSS build directory")

find_program(MAKE_EXECUTABLE NAMES gmake mingw32-make make NAMES_PER_DIR DOC "GNU Make")

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
  BUILD_BYPRODUCTS ${CMAKE_BINARY_DIR}/deps/include/nlohmann/json.hpp
)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

find_library(BOSS_LIBRARY
  NAMES BOSS
  HINTS ${BOSS_BINARY_DIR}
  REQUIRED)

add_executable(McPass Source/McPass.cpp)
add_dependencies(McPass libmicrohttpd nlohmann-json)

target_include_directories(McPass PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/Source)
target_include_directories(McPass SYSTEM PRIVATE
  ${BOSS_SOURCE_DIR}/Source
  ${BOSS_BINARY_DIR}/deps/include
  ${CMAKE_BINARY_DIR}/deps/include)

target_link_libraries(McPass
  ${BOSS_LIBRARY}
  ${BOSS_BINARY_DIR}/deps/lib/${CMAKE_STATIC_LIBRARY_PREFIX}chibi-scheme${CMAKE_STATIC_LIBRARY_SUFFIX}
  ${CMAKE_BINARY_DIR}/deps/lib/libmicrohttpd.a
  Threads::Threads)

set_target_properties(McPass PROPERTIES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED ON
  CXX_EXTENSIONS OFF)

if(APPLE)
  target_link_libraries(McPass "-Wl,-export_dynamic")
endif()
if(UNIX AND NOT APPLE)
  target_link_libraries(McPass dl rt)
endif()

install(TARGETS McPass RUNTIME DESTINATION bin)
install(DIRECTORY launchd DESTINATION share/McPass)
