@echo off
rem Windows equivalent of run.sh. Needs JDK 17+.
rem   run.cmd          build, run the tests, then the walkthrough + soak
rem   run.cmd test     build and run the tests only
rem   run.cmd demo     build and run the walkthrough + soak only
cd /d %~dp0
if exist out rmdir /s /q out
if exist out-test rmdir /s /q out-test
dir /s /b src\main\*.java > sources.txt
javac --release 17 -d out @sources.txt || exit /b 1
dir /s /b src\test\*.java | findstr /v /i "JUnitTest" > tests.txt
javac --release 17 -cp out -d out-test @tests.txt || exit /b 1
del sources.txt tests.txt
if /i "%1"=="demo" goto demo
java -cp out;out-test com.example.cartrecovery.PipelineTests || exit /b 1
if /i "%1"=="test" goto :eof
echo.
:demo
java -cp out com.example.cartrecovery.verify.Verifier