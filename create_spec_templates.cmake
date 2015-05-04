file (READ ${SCRIPLET_DIR}/cmake_spec_prologue SP_PROL)
file (READ ${CUSTOM} SP_CUSTOM)
file (READ ${SCRIPLET_DIR}/cmake_spec_epilogue SP_EPIL)

file (WRITE ${OUTPUT} ${SP_PROL} ${SP_CUSTOM} ${SP_EPIL})
