#!/bin/sh

# SCRIPT: build.sh
# AUTHOR: wjb
# DATE: 2019.05.22
# REV: 1.0
# PLATFORM: Linux
# PURPOSE: persona build package

basedir=$(cd "$(dirname "$0")"; pwd)

echo ">>>>>>>>>>>>>>>Persona build begin<<<<<<<<<<<"

BINS_DIR=${basedir}/
LIBS_DIR=${basedir}/../lib/
JOBS_DIR=${basedir}/../job/
BUILD_DIR=${basedir}/../target/
MAVEN_DIR=${basedir}/../
# maven package lavaheat-etl.jar 
cd ${MAVEN_DIR}
mvn clean
mvn install

# build Persona package
cd ${BUILD_DIR}
mkdir Persona
cd Persona
mkdir lib
mkdir bin
cp -r ${JOBS_DIR}* ./
cp -r ${LIBS_DIR}* ./lib/
cp -r ${BINS_DIR}* ./bin/
cp ${BUILD_DIR}"lavaheat-etl.jar" ./lib/

# zip Persona
cd ..
zip -r Persona.zip Persona/

echo ">>>>>>>>>>>>>>>Persona build completed<<<<<<<<<<<"
exit 0