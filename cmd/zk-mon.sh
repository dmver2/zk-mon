#!/bin/sh
# $0  http://localhost:8080/commands/mntr http://${HOSTNAME}:8080/commands/mntr
PROGNAME=$(basename "$0")
BASEDIR=$(dirname "$0")

JAVABIN=${JAVA_HOME}/bin/java
JAR_VERSION=1.0-SNAPSHOT

if [ "$1" == "" ]; then
  # shellcheck disable=SC2039
  printf "Usage syntax:\n%s zk-url1 [zk-url2...]\ne.g.:\n%s http://localhost:8080/commands/mntr http://%s:8080/commands/mntr" ${PROGNAME} ${PROGNAME} ${HOSTNAME}
  exit 1
fi
${JAVABIN} -Xms8m -Xmx8m \
  -Dfile.encoding=UTF-8 \
  -jar ${BASEDIR}/../build/libs/zk-mon-${JAR_VERSION}.jar \
  $*
