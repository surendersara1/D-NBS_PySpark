@echo off
rem ---------------------------------------------------------------------------
rem  Windows environment for the pyspark30 labs. Run this once per cmd session:
rem      call windows_env.bat
rem  then:  python 00_setup.py   (etc.)
rem
rem  Or make it permanent (new shells only):  setx VAR "value"  for each line.
rem ---------------------------------------------------------------------------

rem 1) Hadoop native shim for Windows (winutils.exe + hadoop.dll).
rem    Without it every write fails with: UnsatisfiedLinkError NativeIO$Windows.access0
set "HADOOP_HOME=C:\Users\suren\hadoop"
set "PATH=%PATH%;%HADOOP_HOME%\bin"

rem 2) Java 23 + Hadoop needs the legacy SecurityManager switch.
rem    Without it: UnsupportedOperationException "getSubject is supported only if..."
set "JAVA_TOOL_OPTIONS=-Djava.security.manager=allow"

rem 3) Pin the Python that Spark workers launch. This machine has TWO Pythons
rem    (3.13 and an old 3.9); without the pin, workers start the wrong one and
rem    jobs hang with: "Timed out while waiting for the Python worker to connect back"
set "PYSPARK_PYTHON=C:\Python3.13.3\python.exe"
set "PYSPARK_DRIVER_PYTHON=C:\Python3.13.3\python.exe"

echo pyspark30 environment set:
echo   HADOOP_HOME=%HADOOP_HOME%
echo   JAVA_TOOL_OPTIONS=%JAVA_TOOL_OPTIONS%
echo   PYSPARK_PYTHON=%PYSPARK_PYTHON%
