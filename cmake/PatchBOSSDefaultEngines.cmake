# Patches BOSS's CMakeLists.txt so that when McPike forwards a non-symbol-store
# engine via BOSS_BUILD_ENGINES (e.g. "another-org/MyEngine:branch") together
# with the bare name in BOSS_DEFAULT_ENGINES, BOSS does not also auto-append
# "symbol-store/MyEngine" -- the resulting duplicate EngineName would make
# ExternalProject_Add fail with "target MyEngine already exists".
#
# Invoked as: cmake -DBOSS_CMAKELISTS=<path> -P PatchBOSSDefaultEngines.cmake

if(NOT BOSS_CMAKELISTS)
  message(FATAL_ERROR "BOSS_CMAKELISTS not set")
endif()

file(READ "${BOSS_CMAKELISTS}" _content)

set(_needle [[  foreach(_eng IN LISTS BOSS_DEFAULT_ENGINES)
    list(APPEND BOSS_BUILD_ENGINES "symbol-store/${_eng}")
  endforeach()]])

set(_replacement [[  foreach(_eng IN LISTS BOSS_DEFAULT_ENGINES)
    set(_mcpike_already_listed FALSE)
    foreach(_existing IN LISTS BOSS_BUILD_ENGINES)
      if(_existing MATCHES "/${_eng}($|:)")
        set(_mcpike_already_listed TRUE)
        break()
      endif()
    endforeach()
    if(NOT _mcpike_already_listed)
      list(APPEND BOSS_BUILD_ENGINES "symbol-store/${_eng}")
    endif()
  endforeach()]])

string(FIND "${_content}" "${_needle}" _idx)
if(_idx EQUAL -1)
  string(FIND "${_content}" "_mcpike_already_listed" _patched_idx)
  if(_patched_idx EQUAL -1)
    message(FATAL_ERROR
      "PatchBOSSDefaultEngines: needle not found in ${BOSS_CMAKELISTS}. "
      "BOSS upstream may have changed; update the patch script.")
  else()
    message(STATUS "PatchBOSSDefaultEngines: already patched, skipping.")
    return()
  endif()
endif()

string(REPLACE "${_needle}" "${_replacement}" _content "${_content}")
file(WRITE "${BOSS_CMAKELISTS}" "${_content}")
message(STATUS "PatchBOSSDefaultEngines: patched ${BOSS_CMAKELISTS}")
