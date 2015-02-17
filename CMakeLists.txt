# ****************
# * BLAHP daemon * 
# ****************
#
# $Id: $ 
#
# File:           CMakeLists.txt
#
# Author(s):      Francesco Prelz ($Author: $)
#        e-mail: "Francesco.Prelz@mi.infn.it"
#
# Revision history:
#
# 5-Nov-2012  Created 

cmake_minimum_required(VERSION 2.8.11)

project(BLAH)

# CPack info

set(CPACK_RPM_PACKAGE_RELEASE "1")

if (UNIX AND NOT APPLE)
set(CPACK_GENERATOR "STGZ;DEB;RPM")
execute_process(COMMAND uname -i 
    OUTPUT_VARIABLE local_arch OUTPUT_STRIP_TRAILING_WHITESPACE)
set(CPACK_SYSTEM_NAME "${CPACK_RPM_PACKAGE_RELEASE}.${local_arch}")
endif (UNIX AND NOT APPLE)

set(CMAKE_INSTALL_PREFIX "/usr")

set(CPACK_PACKAGE_VENDOR "EMI")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Batch Local ASCII Helper Protocol suite")
set(CPACK_PACKAGE_DESCRIPTION_FILE "${CMAKE_CURRENT_SOURCE_DIR}/blah_description.txt")
file(READ ${CPACK_PACKAGE_DESCRIPTION_FILE} CPACK_PACKAGE_DESCRIPTION)
set(CPACK_PACKAGE_VERSION_MAJOR "1")
set(CPACK_PACKAGE_VERSION_MINOR "21")
set(CPACK_PACKAGE_VERSION_PATCH "0")
set(CPACK_PACKAGE_VERSION 
   "${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}")

add_definitions(-DBLAH_VERSION="${CPACK_PACKAGE_VERSION}")

set(MONTH_LIST "Jan" "Feb" "Mar" "Apr" "May" "Jun" 
               "Jul" "Aug" "Sep" "Oct" "Nov" "Dec")
file(TIMESTAMP "${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists.txt" 
     BLAH_BUILD_MONTH "%m")
math(EXPR BLAH_BUILD_MONTH "${BLAH_BUILD_MONTH}-1")
list(GET MONTH_LIST ${BLAH_BUILD_MONTH} BLAH_BUILD_MONTH_TXT)
file(TIMESTAMP "${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists.txt" 
     BLAH_BUILD_TIMESTAMP "${BLAH_BUILD_MONTH_TXT} %d %Y")
add_definitions(-DBLAH_BUILD_TIMESTAMP="${BLAH_BUILD_TIMESTAMP}")

# For the following setting, see CMAKE manual, page 155
# or http://public.kitware.com/Bug/view.php?id=7000
set(CPACK_SET_DESTDIR ON)
set(CPACK_PACKAGE_RELOCATABLE "false")

# Pre-definition of variables for the UseRPMTools spec template
set(CPACK_RPM_PACKAGE_SUMMARY ${CPACK_PACKAGE_DESCRIPTION_SUMMARY})
set(CPACK_RPM_PACKAGE_NAME ${CMAKE_PROJECT_NAME})
set(CPACK_RPM_PACKAGE_VERSION ${CPACK_PACKAGE_VERSION})
set(CPACK_RPM_PACKAGE_VENDOR ${CPACK_PACKAGE_VENDOR})

# Contruct two spec file templates: one for UseRPMTools and one for 
# the CPack RPM module (that we hope we'll be able to actually use some day)
add_custom_command(OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/project/cpack_spec_template
                   COMMAND ${CMAKE_COMMAND} 
                              -D OUTPUT=${CMAKE_CURRENT_SOURCE_DIR}/project/cpack_spec_template
                              -D CUSTOM=${CMAKE_CURRENT_SOURCE_DIR}/project/cmake_spec_cpack_build
                              -P ${CMAKE_CURRENT_SOURCE_DIR}/create_spec_templates.cmake
                   DEPENDS 
                     ${CMAKE_CURRENT_SOURCE_DIR}/project/cmake_spec_prologue
                     ${CMAKE_CURRENT_SOURCE_DIR}/project/cmake_spec_epilogue
                     ${CMAKE_CURRENT_SOURCE_DIR}/project/cmake_spec_cpack_build)

execute_process(COMMAND ${CMAKE_COMMAND} 
                  -D OUTPUT=${CMAKE_CURRENT_SOURCE_DIR}/project/${CPACK_RPM_PACKAGE_NAME}.spec.in
                  -D CUSTOM=${CMAKE_CURRENT_SOURCE_DIR}/project/cmake_spec_userpmtools_build
                  -P ${CMAKE_CURRENT_SOURCE_DIR}/create_spec_templates.cmake)

add_custom_target(all_spec_files ALL DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/project/cpack_spec_template
    ${CMAKE_CURRENT_SOURCE_DIR}/project/${CPACK_RPM_PACKAGE_NAME}.spec.in
   )


set(CPACK_PACKAGE_CONTACT "blah@mi.infn.it")
set(CPACK_RPM_PACKAGE_LICENSE "Apache Software License")
set(CPACK_RPM_PACKAGE_GROUP "Applications/Internet")
set(CPACK_RPM_USER_BINARY_SPECFILE "${CMAKE_CURRENT_SOURCE_DIR}/project/cpack_spec_template")

set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE 
    ${CMAKE_CURRENT_SOURCE_DIR}/project/glite-ce-blahp.post)
set(CPACK_RPM_PRE_UNINSTALL_SCRIPT_FILE 
    ${CMAKE_CURRENT_SOURCE_DIR}/project/glite-ce-blahp.preun)

set(CPACK_SOURCE_IGNORE_FILES
  "Makefile;CMakeCache.txt;/RPM/;/autom4te.cache/;/CMakeFiles/;/build/;/\\\\.git/;/\\\\.deps/;\\\\.tar\\\\.gz$;\\\\.o$;\\\\.rpm$;\\\\.deb$;${CPACK_SOURCE_IGNORE_FILES}")

add_subdirectory(src build)
add_subdirectory(config)
add_subdirectory(doc)

include(CPack)
include(UseRPMTools.cmake)

if(RPMTools_FOUND)
   file(READ ${CPACK_RPM_POST_INSTALL_SCRIPT_FILE} CPACK_RPM_SPEC_POSTINSTALL)
   file(READ ${CPACK_RPM_PRE_UNINSTALL_SCRIPT_FILE} CPACK_RPM_SPEC_PREUNINSTALL)
   RPMTools_ADD_RPM_TARGETS(${CPACK_RPM_PACKAGE_NAME}
                            "${CMAKE_CURRENT_SOURCE_DIR}/project/${CPACK_RPM_PACKAGE_NAME}.spec.in")
   unset(CPACK_RPM_SPEC_POSTINSTALL)
   unset(CPACK_RPM_SPEC_PREUNINSTALL)
endif(RPMTools_FOUND)

install(FILES LICENSE 
    DESTINATION share/doc/${CMAKE_PROJECT_NAME}-${CPACK_PACKAGE_VERSION})
