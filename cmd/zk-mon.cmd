@echo off
setlocal
set JAR_VERSION=1.0-SNAPSHOT
%JAVA_HOME%\bin\java -Xms10m -Xmx10m ^
 -Dfile.encoding=UTF-8 ^
 -Dnetworkaddress.cache.ttl=10 ^
 -Dnetworkaddress.cache.negative.ttl=10 ^
 -Dcom.sun.management.jmxremote.local.only ^
 -Djava.rmi.server.hostname=127.0.0.1 ^
 -Djava.net.preferIPv4Stack=true ^
 -XX:+HeapDumpOnOutOfMemoryError ^
 -jar %~dp0..\build\libs\zk-mon-%JAR_VERSION%.jar ^
 %*
endlocal
