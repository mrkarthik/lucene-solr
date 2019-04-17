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

BUILD=""
RELEASE_VERSION=""
RELEASE_FOLDER=""

function log () {
  echo "$(date +'%m-%d-%Y %T') ${1}"
}

function pause () {
  read -p "${*}"
}

function executeCommand () {
  local COMMAND="${*}"
  log "Running ${COMMAND}..."
  echo "Running ${COMMAND}..." >> ${RELEASE_LOGFILE} 2>&1
  ${COMMAND} >> ${RELEASE_LOGFILE} 2>&1
  local ERROR_CODE=${?}
  if [[ ${ERROR_CODE} != 0 ]]; then
    log "Error when executing ${COMMAND}"
  fi

  return ${ERROR_CODE}
}

function prepare () {
  log "Preparing for Lucene/Solr Build...."
  rm rev.txt &> /dev/null
  rm /tmp/release.log &> /dev/null
  if [[ ${BUILD} = ${RC_REBUILD_MODE} ]] ; then
    log "Removing Ant and Ivy Folders before build."
    rm -rf ~/.ant
    rm -rf ~/.ivy2
  fi
  local BASEDIR="${1}"
  local REL_BASE_DIR="/tmp/releases"
  RELEASE_VERSION=$(grep "version.base=" ${BASEDIR}/lucene/version.properties | cut -d'=' -f2)
  log "Starting with following variables: "
  log "    Base Directory    : ${BASEDIR}"

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

  return 0;
}

function buildAndTest () {
  local BASEDIR="${1}"
  if [[ ${BUILD} == ${RC_NOBUILD_MODE} ]] ; then
    log "Skipping Build and Test...."
    return 0;
  fi
  # Prepare for Release
  log "Build Lucene/Solr...."
  local COMMAND_TO_EXECUTE="ant clean precommit"
  #executeCommand ${COMMAND_TO_EXECUTE}
  local ERROR_CODE=${?}
  if [[ ${ERROR_CODE} == 0 ]] ; then
    BVS_OPTIONS="-Ddev.version.suffix=${VERSION_SUFFIX}"
    MAVEN_OPTIONS="-Dm2.repository.id=${REPOSITORY_ID} -Dm2.repository.url=${REPOSITORY_URL} -Dm2.repository.username=${REPOSITORY_USERNAME} -Dm2.repository.password=${REPOSITORY_PASSWORD}"
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

function pushToMaven () {
  local BASEDIR="${1}"
  # Stage POM for lucene deployment
  cd ${BASEDIR}/lucene
  log "Push lucene to Maven..."
  #-Dmaven.dist.dir=${RELEASE_FOLDER}/solr/maven/
  local COMMAND_TO_EXECUTE="ant clean stage-maven-artifacts ${MAVEN_OPTIONS} ${BVS_OPTIONS}"
  executeCommand ${COMMAND_TO_EXECUTE}
  local ERROR_CODE=${?}
  if [[ ${ERROR_CODE} == 0 ]] ; then
    log "Lucene pushed to Maven."
    # Stage POM for SOLR deployment
    cd ${BASEDIR}/solr
    log "Push solr to Maven..."
    COMMAND_TO_EXECUTE="ant clean stage-maven-artifacts ${MAVEN_OPTIONS} ${BVS_OPTIONS}"
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
  # Arguments
  if [[ $# -gt 0 ]] ; then
    log "Starting ${0} with ${*}..."
  else
    log "Starting ${0}..."
  fi

  if [[ "${1}" == "--help" ]] ; then
      log "Usage: ${0} [build (true or false) or rebuild]"
      exit 1
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
    buildAndTest ${BASEDIR}
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