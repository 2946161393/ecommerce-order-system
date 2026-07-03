@echo off
REM ============================================================
REM  Demo script for Windows (run from cmd.exe in project root).
REM  Requires: curl (built into Windows 10 1803+).
REM  App must be running on localhost:8080.
REM ============================================================

echo.
echo === 1. Register a user ===
curl -s -X POST http://localhost:8080/api/auth/register ^
  -H "Content-Type: application/json" ^
  -d "{\"username\":\"alice\",\"password\":\"secret123\"}"
echo.

echo.
echo === 2. Login, get a JWT ===
echo (copy the token value from the output below)
curl -s -X POST http://localhost:8080/api/auth/login ^
  -H "Content-Type: application/json" ^
  -d "{\"username\":\"alice\",\"password\":\"secret123\"}"
echo.
echo.
echo Now set it as a variable, e.g.:
echo   set TOKEN=eyJhbGciOi...
echo.

echo === 3. Create an order (needs the token) ===
echo   curl -s -X POST http://localhost:8080/api/orders ^
echo     -H "Authorization: Bearer %%TOKEN%%" ^
echo     -H "Content-Type: application/json" ^
echo     -d "{\"productName\":\"Keyboard\",\"amount\":49.99}"
echo.

echo === 4. List your orders ===
echo   curl -s http://localhost:8080/api/orders -H "Authorization: Bearer %%TOKEN%%"
echo.

echo === 5. Verify the Cassandra status log was written by the consumer ===
echo   docker exec -it eos-cassandra cqlsh -e "SELECT * FROM orderks.order_status_log;"
echo.
