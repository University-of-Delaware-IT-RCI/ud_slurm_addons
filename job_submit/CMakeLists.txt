CMAKE_MINIMUM_REQUIRED (VERSION 2.6)
PROJECT (job_submit_udhpc C)

#
# For finding packages:
#
INCLUDE(CheckIncludeFiles)
INCLUDE(FindPackageHandleStandardArgs)

IF (NOT SLURM_PREFIX)
  SET (SLURM_PREFIX "/usr/local" CACHE PATH "Directory in which SLURM is installed.")
ENDIF (NOT SLURM_PREFIX)
IF (SLURM_PREFIX)
  LIST(APPEND CMAKE_PREFIX_PATH ${SLURM_PREFIX})
ENDIF (SLURM_PREFIX)

IF (NOT SLURM_SOURCE_DIR)
  SET (SLURM_SOURCE_DIR "${SLURM_PREFIX}/src" CACHE PATH "Directory containing the SLURM source package.")
ENDIF (NOT SLURM_SOURCE_DIR)

IF (NOT DISABLE_RESERVED_PARTITION)
  SET (DISABLE_RESERVED_PARTITION OFF CACHE BOOL "Do not check for proper use of 'reserved' partition.")
ENDIF (NOT DISABLE_RESERVED_PARTITION)

IF (NOT DISABLE_HARDWARE_SPECIFIC_PARTITIONS)
  SET (DISABLE_HARDWARE_SPECIFIC_PARTITIONS OFF CACHE BOOL "Do not auto-set QOS for hardware-specific partitions.")
ENDIF (NOT DISABLE_HARDWARE_SPECIFIC_PARTITIONS)

IF (NOT DISABLE_WORKGROUP_PARTITIONS)
  SET (DISABLE_WORKGROUP_PARTITIONS OFF CACHE BOOL "Do not set workgroup partitions when requested via spank plugin.")
ENDIF (NOT DISABLE_WORKGROUP_PARTITIONS)

IF (NOT DISABLE_GPU_GRES_ADJUSTMENTS)
  SET (DISABLE_GPU_GRES_ADJUSTMENTS OFF CACHE BOOL "Do not enforce GPU-to-CPU binding on jobs requesting GPU GRESes.")
ENDIF (NOT DISABLE_GPU_GRES_ADJUSTMENTS)

IF (NOT DISABLE_PRIORITY_ACCESS_QOS)
  SET (DISABLE_PRIORITY_ACCESS_QOS OFF CACHE BOOL "Do not automatically set priority-access QOS on jobs headed to workgroup partitions.")
ENDIF (NOT DISABLE_PRIORITY_ACCESS_QOS)

SET (SLURM_BUILD_DIR "" CACHE PATH "Directory containing the SLURM build (e.g. config.h).")

FIND_PATH(SLURM_INCLUDE_DIR NAMES slurm/slurm.h)
FIND_LIBRARY(SLURM_LIBRARY NAMES libslurm.so)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(SLURM DEFAULT_MSG SLURM_LIBRARY SLURM_INCLUDE_DIR)
IF (NOT SLURM_FOUND)
  MESSAGE(FATAL_ERROR "SLURM library could not be found")
ELSE (NOT SLURM_FOUND)
  SET (SLURM_LIBRARIES ${SLURM_LIBRARY})
  SET (SLURM_INCLUDE_DIRS ${SLURM_INCLUDE_DIR})
  FIND_PATH(SLURM_MODULES_DIR NAMES lib/slurm DOC "Directory containing SLURM extensions.")
  IF (NOT SLURM_MODULES_DIR)
    MESSAGE(FATAL_ERROR "SLURM extensions directory could not be found")
  ENDIF (NOT SLURM_MODULES_DIR)
ENDIF (NOT SLURM_FOUND)
MARK_AS_ADVANCED (SLURM_LIBRARIES SLURM_INCLUDE_DIRS)

#
# Validate the SLURM_SOURCE_DIR path
#
FIND_PATH(SLURM_XLATOR_H NAMES src/common/slurm_xlator.h PATHS ${SLURM_SOURCE_DIR})
IF (NOT SLURM_XLATOR_H)
  MESSAGE(FATAL_ERROR "${SLURM_SOURCE_DIR} does not appear to be a SLURM source tree")
ENDIF (NOT SLURM_XLATOR_H)
UNSET (SLURM_XLATOR_H CACHE)
IF (NOT SLURM_BUILD_DIR)
  FIND_PATH(SLURM_BUILD_DIR NAMES config.h PATHS ${SLURM_SOURCE_DIR})
  IF (NOT SLURM_BUILD_DIR)
    MESSAGE (FATAL_ERROR "SLURM build directory not found")
  ENDIF (NOT SLURM_BUILD_DIR)
ENDIF (NOT SLURM_BUILD_DIR)

ADD_LIBRARY (job_submit_udhpc MODULE job_submit_udhpc.c)
INCLUDE_DIRECTORIES (BEFORE ${SLURM_INCLUDE_DIRS} ${SLURM_SOURCE_DIR} ${SLURM_BUILD_DIR})
TARGET_LINK_LIBRARIES (job_submit_udhpc -lm)
SET_TARGET_PROPERTIES (job_submit_udhpc PROPERTIES PREFIX "" SUFFIX "" OUTPUT_NAME "job_submit_udhpc.so")
GET_TARGET_PROPERTY(compile_flags job_submit_udhpc COMPILE_FLAGS)
IF (compile_flags STREQUAL "compile_flags-NOTFOUND")
  SET(compile_flags "")
ENDIF ()
IF ( DISABLE_RESERVED_PARTITION )
  SET(compile_flags "${compile_flags} -DDISABLE_RESERVED_PARTITION")
ENDIF ()
IF ( DISABLE_HARDWARE_SPECIFIC_PARTITIONS )
  SET(compile_flags "${compile_flags} -DDISABLE_HARDWARE_SPECIFIC_PARTITIONS")
ENDIF ()
IF ( DISABLE_GPU_GRES_ADJUSTMENTS )
  SET(compile_flags "${compile_flags} -DDISABLE_GPU_GRES_ADJUSTMENTS")
ENDIF ()
IF ( DISABLE_WORKGROUP_PARTITIONS )
  SET(compile_flags "${compile_flags} -DDISABLE_WORKGROUP_PARTITIONS")
ENDIF ()
SET_TARGET_PROPERTIES (job_submit_udhpc PROPERTIES COMPILE_FLAGS "${compile_flags}")
INSTALL (TARGETS job_submit_udhpc DESTINATION ${SLURM_MODULES_DIR}/lib/slurm)

