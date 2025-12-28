@echo off
REM ==============================================
REM Step 1: Run buf generate
REM ==============================================
echo Running buf generate...
buf lint
buf generate
if %ERRORLEVEL% neq 0 (
    echo buf generate failed!
    pause
    exit /b %ERRORLEVEL%
)

REM ==============================================
REM Walk all subfolders under services\
REM and run protoc-go-inject-tag on every *.pb.go
REM ==============================================

for /R backend/interfaces %%f in (*.pb.go) do (
    echo Injecting tags into %%f
    protoc-go-inject-tag -input="%%f"
)

echo ----------------------------------------------
echo All files processed!
@REM pause