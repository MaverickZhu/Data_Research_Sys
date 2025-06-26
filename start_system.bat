@echo off
chcp 65001 >nul
title 消防单位建筑数据关联系统启动工具

echo.
echo ====================================================
echo 🔥 消防单位建筑数据关联系统启动工具
echo ====================================================
echo.

REM 检查Python是否安装
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ 未检测到Python，请先安装Python 3.8或更高版本
    pause
    exit /b 1
)

echo ✅ Python环境检查通过

REM 检查必要文件
if not exist "requirements.txt" (
    echo ❌ 缺少requirements.txt文件
    pause
    exit /b 1
)

if not exist "config\database.yaml" (
    echo ❌ 缺少数据库配置文件 config\database.yaml
    pause
    exit /b 1
)

echo ✅ 配置文件检查通过

REM 提供选项菜单
:menu
echo.
echo 📋 请选择操作：
echo [1] 安装依赖和初始化系统
echo [2] 测试数据库连接
echo [3] 启动系统服务
echo [4] 查看系统状态
echo [0] 退出
echo.
set /p choice=请输入选项 (0-4): 

if "%choice%"=="1" goto install
if "%choice%"=="2" goto test
if "%choice%"=="3" goto start
if "%choice%"=="4" goto status
if "%choice%"=="0" goto exit
echo 无效选项，请重新选择
goto menu

:install
echo.
echo 🔧 正在安装依赖和初始化系统...
python setup.py
if %errorlevel% neq 0 (
    echo ❌ 系统初始化失败
    pause
    goto menu
)
echo ✅ 系统初始化完成
pause
goto menu

:test
echo.
echo 🔍 正在测试数据库连接...
python scripts\test_connection.py
if %errorlevel% neq 0 (
    echo ❌ 数据库连接测试失败
) else (
    echo ✅ 数据库连接测试成功
)
pause
goto menu

:start
echo.
echo 🚀 正在启动系统服务...
echo 系统将在浏览器中打开，如未自动打开请访问: http://localhost:5000
echo 按Ctrl+C停止服务
echo.
python run.py
pause
goto menu

:status
echo.
echo 📊 系统状态检查...
echo.

REM 检查MongoDB连接配置
echo 📡 数据库配置:
findstr "uri:" config\database.yaml
if %errorlevel% neq 0 (
    findstr "host:" config\database.yaml
    findstr "port:" config\database.yaml
    findstr "database:" config\database.yaml
)

echo.
echo 📁 项目文件状态:
if exist "src\main.py" (echo ✅ 主程序文件存在) else (echo ❌ 主程序文件缺失)
if exist "src\database\connection.py" (echo ✅ 数据库连接模块存在) else (echo ❌ 数据库连接模块缺失)
if exist "src\web\app.py" (echo ✅ Web应用模块存在) else (echo ❌ Web应用模块缺失)

echo.
echo 📋 日志目录:
if exist "logs" (
    echo ✅ 日志目录存在
    dir /b logs\*.log 2>nul | find /c /v "" > temp_count.txt
    set /p log_count=<temp_count.txt
    del temp_count.txt
    echo 📝 日志文件数量: %log_count%
) else (
    echo ⚠️  日志目录不存在
)

pause
goto menu

:exit
echo.
echo 👋 再见！
timeout /t 2 >nul
exit /b 0 