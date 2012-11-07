# - Finds Condor Classified Ad (Classad) binary distribution.
# The following variables are set:
#   ClassAd_CXX_FLAGS - flags to add to the CXX compiler for Classad support
#   CLASSAD_FOUND - true if the Classad distribution is detected
#
# Supported compilers can be found at http://openmp.org/wp/openmp-compilers/

#    Licensed under the Apache License, Version 2.0 (the "License"); 
#    you may not use this file except in compliance with the License. 
#    You may obtain a copy of the License at 
#  
#        http://www.apache.org/licenses/LICENSE-2.0 
#  
#    Unless required by applicable law or agreed to in writing, software 
#    distributed under the License is distributed on an "AS IS" BASIS, 
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
#    See the License for the specific language governing permissions and 
#    limitations under the License.

include(CheckCSourceCompiles)
include(CheckCXXSourceCompiles)
include(FindPackageHandleStandardArgs)

set(ClassAd_INCLUDE_PATH_DESCRIPTION "top-level directory containing the Condor ClassAd include directories. E.g /opt/classad/include")
set(ClassAd_INCLUDE_DIR_MESSAGE "Set the ClassAd_INCLUDE_DIR cmake cache entry to the ${ClassAd_INCLUDE_PATH_DESCRIPTION}")
set(ClassAd_LIBRARY_PATH_DESCRIPTION "top-level directory containing the Condor ClassAd libraries.")
set(ClassAd_LIBRARY_DIR_MESSAGE "Set the ClassAd_LIBRARY_DIR cmake cache entry to the ${ClassAd_LIBRARY_PATH_DESCRIPTION}")

find_path(ClassAd_INCLUDE_DIR
  NAMES classad_distribution.h
  PATHS
   # Look in other places.
   ${ClassAd_ROOT_DIRECTORIES}
  PATH_SUFFIXES
    classad
    include
  # Help the user find it if we cannot.
  DOC "The ${ClassAd_INCLUDE_DIR_MESSAGE}"
)

message(STATUS "ClassAd_INCLUDE_DIR == " ${ClassAd_INCLUDE_DIR})

# The ClassAd library (should have namespaces enabled).
set (ClassAd_LIBRARY_TO_FIND classad_ns)

# Setting some more prefixes for the library
set (ClassAd_LIB_PREFIX "")
if ( WIN32 )
  set (ClassAd_LIB_PREFIX ${ClassAd_LIB_PREFIX} "lib")
  set ( ClassAd_LIBRARY_TO_FIND ${ClassAd_LIB_PREFIX}${ClassAd_LIBRARY_TO_FIND})
endif()

find_library( ClassAd_LIBRARY
 NAMES ${ClassAd_LIBRARY_TO_FIND}
 PATHS
   ${ClassAd_LIBRARY_DIR}
 PATH_SUFFIXES
   lib
)

get_filename_component(ClassAd_LIBRARY_DIR ${ClassAd_LIBRARY} PATH)
message(STATUS "ClassAd_LIBRARY == " ${ClassAd_LIBRARY})

# sample Classad source code to test
set(ClassAd_CXX_TEST_SOURCE 
"
#include <classad_distribution.h>
classad::ClassAd ad;
classad::ClassAdParser parser;

int
main(int argc, char *argv[])
{
}
")

set(ClassAd_CXX_FLAG_CANDIDATES
  "-DWANT_NAMESPACES"
  "-DWANT_NAMESPACES -DWANT_CLASSAD_NAMESPACE"
)

# check cxx compiler
foreach(FLAG ${ClassAd_CXX_FLAG_CANDIDATES})
  set(SAFE_CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS}")
  set(SAFE_CMAKE_REQUIRED_LIBRARIES "${CMAKE_REQUIRED_LIBRARIES}")
  set(SAFE_CMAKE_REQUIRED_INCLUDES "${CMAKE_REQUIRED_INCLUDES}")
  set(CMAKE_REQUIRED_FLAGS "${FLAG}")
  set(CMAKE_REQUIRED_LIBRARIES "${ClassAd_LIBRARY}")
  set(CMAKE_REQUIRED_INCLUDES "${ClassAd_INCLUDE_DIR}")
  unset(ClassAd_FLAG_DETECTED CACHE)
  message(STATUS "Try Classad CXX flag = [${FLAG}] (library = [${ClassAd_LIBRARY}])")
  check_cxx_source_compiles("${ClassAd_CXX_TEST_SOURCE}" ClassAd_FLAG_DETECTED)
  set(CMAKE_REQUIRED_FLAGS "${SAFE_CMAKE_REQUIRED_FLAGS}")
  set(CMAKE_REQUIRED_LIBRARIES "${SAFE_CMAKE_REQUIRED_LIBRARIES}")
  set(CMAKE_REQUIRED_INCLUDES "${SAFE_CMAKE_REQUIRED_INCLUDES}")
  if(ClassAd_FLAG_DETECTED)
    set(ClassAd_CXX_FLAGS_INTERNAL "${FLAG}")
    break()
  endif(ClassAd_FLAG_DETECTED)
endforeach(FLAG ${ClassAd_CXX_FLAG_CANDIDATES})

set(ClassAd_CXX_FLAGS "${ClassAd_CXX_FLAGS_INTERNAL}"
  CACHE STRING "C++ compiler flags for use of the Condor Classad  library")
message(STATUS "ClassAd_CXX_FLAGS == " ${ClassAd_CXX_FLAGS})
# handle the standard arguments for find_package
find_package_handle_standard_args(ClassAd DEFAULT_MSG 
  ClassAd_LIBRARY ClassAd_INCLUDE_DIR)

mark_as_advanced(
  ClassAd_CXX_FLAGS
  ClassAd_LIBRARY
)
