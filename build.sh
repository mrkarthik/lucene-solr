#!/bin/bash

# Use this script to build and install via Maven
# Global
REPOSITORY_URL="https://artprod.dev.bloomberg.com/artifactory/libs-release"
REPOSITORY_ID="central"
REPOSITORY_USERNAME="bvltuser"
REPOSITORY_PASSWORD="AP4VyaWNaBCo4YyPqA1i8zLwfs6"
VERSION_SUFFIX="bvs"
RC_BUILD_MODE="build"
RC_REBUILD_MODE="rebuild"
RC_NOBUILD_MODE="nobuild"
RELEASE_LOGFILE="/tmp/release.log"
RELEASE_BUILD_LOGFILE="/tmp/build.log"

BUILD=""
RELEASE_VERSION=""
RELEASE_FOLDER=""
RELEASE_RECUT_NUMBER=0
LOGFILE=""

function log () {
  echo "$(date +'%m-%d-%Y %T') ${1}"
}

function pause () {
  read -p "${*}"
}

function executeCommand () {
  local COMMAND="${*}"
  log "Running ${COMMAND}..."
  echo "Running ${COMMAND}..." >> ${LOGFILE} 2>&1
  ${COMMAND} >> ${LOGFILE} 2>&1
  local ERROR_CODE=${?}
  if [[ ${ERROR_CODE} != 0 ]]; then
    log "Error when executing ${COMMAND}"
  fi

  return ${ERROR_CODE}
}

function prepare () {
  log "Preparing for Lucene/Solr Build...."
  if [[ ${BUILD} = ${RC_REBUILD_MODE} ]] ; then
    log "Removing Ant and Ivy Folders before build."
    rm -rf ~/.ant
    rm -rf ~/.ivy2
  fi
  local BASEDIR="${1}"
  local REL_BASE_DIR="/tmp/releases"
  RELEASE_VERSION=$(grep "^version.base=" ${BASEDIR}/lucene/version.properties | cut -d'=' -f2)
  local RC_RELEASE_VERSION=${RELEASE_VERSION}-RC
  local RC_RELEASE_VERSION_LEN=$(echo ${RC_RELEASE_VERSION} | wc -c)
  local LAST_RC_NUM=0
  if [[ -d "${REL_BASE_DIR}" ]]; then
    LAST_RC_NUM=$(ls -lt ${REL_BASE_DIR} | awk '{print $9}'| grep "${RC_RELEASE_VERSION}" | head -1 | cut -c ${RC_RELEASE_VERSION_LEN}-)
  fi
  if [[ ${BUILD} == ${RC_BUILD_MODE} ]] ; then
    RELEASE_RECUT_NUMBER=$(($LAST_RC_NUM + 1))
  else
    RELEASE_RECUT_NUMBER=$LAST_RC_NUM
  fi
  RELEASE_FOLDER="${REL_BASE_DIR}/${RELEASE_VERSION}-RC${RELEASE_RECUT_NUMBER}"

  log "Starting with following variables: "
  log "    Base Directory    : ${BASEDIR}"
  log "    Release Directory : ${RELEASE_FOLDER}"

  if [[ ${BUILD} == ${RC_NOBUILD_MODE} ]] ; then
    if [[ ! -d ${RELEASE_FOLDER} ]] ; then
      log "! ! ! Can't proceed, directory not found: ${RELEASE_FOLDER}"
      log "PS: May be ${RELEASE_VERSION} not yet built!!"
      return 1;
    fi
    else
    if [[ -d ${RELEASE_FOLDER} ]] ; then
      log "! ! ! Can't proceed, directory already exists: ${RELEASE_FOLDER}"
      log "PS: Use different Release Candidate Number"
      return 1;
    fi
  fi
  BVS_OPTIONS="-Ddev.version.suffix=${VERSION_SUFFIX}"
  MAVEN_OPTIONS="-Dm2.repository.id=${REPOSITORY_ID} -Dm2.repository.url=${REPOSITORY_URL} -Dm2.repository.username=${REPOSITORY_USERNAME} -Dm2.repository.password=${REPOSITORY_PASSWORD}"

  return 0;
}

function buildAndTestWitAnt () {
  local BASEDIR="${1}"
  if [[ ${BUILD} == ${RC_NOBUILD_MODE} ]] ; then
    log "Skipping Build and Test...."
    return 0;
  fi
  # Prepare for Release
  log "Build Lucene/Solr...."
  local COMMAND_TO_EXECUTE="ant clean precommit"
  executeCommand ${COMMAND_TO_EXECUTE}
  local ERROR_CODE=${?}
  if [[ ${ERROR_CODE} == 0 ]] ; then
    log "Preparing Lucene for release..."
    cd ${BASEDIR}/lucene
    COMMAND_TO_EXECUTE="ant prepare-release-no-sign ${MAVEN_OPTIONS} ${BVS_OPTIONS}"
    executeCommand ${COMMAND_TO_EXECUTE}
    ERROR_CODE=${?}
    if [[ ${ERROR_CODE} == 0 ]] ; then
      log "Lucene ready for release"
      log "Preparing Solr for release..."
      cd ${BASEDIR}/solr
      COMMAND_TO_EXECUTE="ant prepare-release-no-sign ${MAVEN_OPTIONS} ${BVS_OPTIONS}"
      executeCommand ${COMMAND_TO_EXECUTE}
      ERROR_CODE=${?}
      if [[ ${ERROR_CODE} == 0 ]] ; then
        log "Solr ready for release"
      fi
    fi
  fi

  return ${ERROR_CODE};
}

function buildAndTest () {
  local BASEDIR="${1}"
  if [[ ${BUILD} == ${RC_NOBUILD_MODE} ]] ; then
    log "Skipping Build and Test...."
    return 0;
  fi
  # Prepare for Release
  log "Build and prepare Lucene/Solr...."
  local COMMAND_TO_EXECUTE="python3 -u dev-tools/scripts/buildAndPushRelease.py --push-local ${RELEASE_FOLDER} --rc-num ${RELEASE_RECUT_NUMBER} --root ${BASEDIR}"
  executeCommand ${COMMAND_TO_EXECUTE}
  local ERROR_CODE=${?}
  if [[ ${ERROR_CODE} == 0 ]] ; then
    log "Lucene/Solr package ready."
  fi

  return ${ERROR_CODE};
}

function pushToMaven () {
  local BASEDIR="${1}"
  local RELEASE_REVISION=`cat ${BASEDIR}/rev.txt`
  local RELEASE_REV_FOLDER="${RELEASE_FOLDER}/lucene-solr-${RELEASE_VERSION}-RC${RELEASE_RECUT_NUMBER}-rev${RELEASE_REVISION}"
  # Stage POM for lucene deployment
  cd ${BASEDIR}/lucene
  log "Push lucene to Maven..."
  local COMMAND_TO_EXECUTE="ant clean stage-maven-artifacts ${MAVEN_OPTIONS} ${BVS_OPTIONS} -Dmaven.dist.dir=${RELEASE_REV_FOLDER}/lucene/maven/"
  executeCommand ${COMMAND_TO_EXECUTE}
  local ERROR_CODE=${?}
  if [[ ${ERROR_CODE} == 0 ]] ; then
    log "Lucene pushed to Maven."
    # Stage POM for SOLR deployment
    cd ${BASEDIR}/solr
    log "Push solr to Maven..."
    COMMAND_TO_EXECUTE="ant clean stage-maven-artifacts ${MAVEN_OPTIONS} ${BVS_OPTIONS} -Dmaven.dist.dir=${RELEASE_REV_FOLDER}/solr/maven/"
    executeCommand ${COMMAND_TO_EXECUTE}
    ERROR_CODE=${?}
    if [[ ${ERROR_CODE} == 0 ]] ; then
      log "Solr pushed to Maven."
    fi
  fi

  return ${ERROR_CODE}
}

function logStatus () {
  local ERROR_CODE="${1}"
  local SUBJECT="Lucene/Solr ${RELEASE_VERSION} Build Status ::"
  if [[ ${ERROR_CODE} != 0 ]]; then
    SUBJECT="${SUBJECT} FAILED"
    MAIL_BODY="${SUBJECT}"
  else
    SUBJECT="${SUBJECT} SUCCESS"
    MAIL_BODY="${SUBJECT}"
  fi
  log "${SUBJECT}"

  return 0;
}

function main() {
  # Clean up
  clear
  rm rev.txt &> /dev/null
  rm ${RELEASE_LOGFILE} &> /dev/null
  rm ${RELEASE_BUILD_LOGFILE} &> /dev/null

  # Arguments
  if [[ $# -gt 0 ]] ; then
    log "Starting ${0} with Arguments ${*}..."
  else
    log "Starting ${0}..."
  fi

  if [[ "${1}" == "--help" ]] ; then
      log "Usage: ${0} [build (true or false) or rebuild]"
      exit 1
  fi

  if [[ "${1}" == "ANT" ]] ; then
    local BUILD_TYPE="${1}"
    LOGFILE=${RELEASE_LOGFILE}
    shift
  else
    LOGFILE=${RELEASE_BUILD_LOGFILE}
  fi

  if [[ "${1}" == "false" ]] ; then
      BUILD=${RC_NOBUILD_MODE}
  elif [[ "${1}" == "rebuild" ]] ; then
      BUILD=${RC_REBUILD_MODE}
  else
      BUILD=${RC_BUILD_MODE}
  fi

  log "Starting Lucene/Solr ${BUILD}...."
  local BASEDIR="${PWD}"
  prepare ${BASEDIR}
  local ERROR_CODE=${?}
  if [[ ${ERROR_CODE} == 0 ]] ; then
    if [[ "${BUILD_TYPE}" == "ANT" ]] ; then
      buildAndTestWitAnt ${BASEDIR}
    else
      buildAndTest ${BASEDIR}
    fi
    ERROR_CODE=${?}
    if [[ ${ERROR_CODE} == 0 ]] ; then
      pushToMaven ${BASEDIR}
      ERROR_CODE=${?}
    fi
  fi

  logStatus ${ERROR_CODE}
  return ${ERROR_CODE};
}

main ${*}
log "Status: ${?}"