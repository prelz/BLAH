file (READ ${CMAKE_CURRENT_SOURCE_DIR}/project/cmake_spec_prologue SP_PROL)
file (READ ${CUSTOM} SP_CUSTOM)
file (READ ${CMAKE_CURRENT_SOURCE_DIR}/project/cmake_spec_epilogue SP_EPIL)

file (WRITE ${OUTPUT} ${SP_PROL} ${SP_CUSTOM} ${SP_EPIL})
