#!/bin/bash
# ============================================================
# start.sh — Simulation Container Startup Script
# Chạy song song mock_backend.py và clickstream_bot.py
#
# Disk Protection:
#   CLICKSTREAM_MAX_EVENTS — bot tự dừng sau N events
#   SIMULATION_MAX_HOURS   — toàn bộ container dừng sau N giờ
# ============================================================
set -e

echo "=== [Simulation] Khởi động RetailFlow Simulation Bots ==="
echo "=== [Simulation] CLICKSTREAM_MAX_EVENTS=${CLICKSTREAM_MAX_EVENTS:-unlimited} ==="
echo "=== [Simulation] SIMULATION_MAX_HOURS=${SIMULATION_MAX_HOURS:-unlimited} ==="

# Tính timeout theo giây nếu SIMULATION_MAX_HOURS được đặt
if [ -n "$SIMULATION_MAX_HOURS" ] && [ "$SIMULATION_MAX_HOURS" != "0" ]; then
    TIMEOUT_SECS=$(echo "$SIMULATION_MAX_HOURS * 3600" | bc | cut -d. -f1)
    echo "[Simulation] Container sẽ tự tắt sau ${SIMULATION_MAX_HOURS}h (${TIMEOUT_SECS}s)."
else
    TIMEOUT_SECS=0
fi

# Chạy mock_backend ở background
echo "[Simulation] Khởi động mock_backend.py..."
python3 scripts/simulation/mock_backend.py &
BACKEND_PID=$!

# Chờ 3 giây để mock_backend kịp khởi tạo kết nối PostgreSQL trước
sleep 3

# Chạy clickstream_bot ở background
echo "[Simulation] Khởi động clickstream_bot.py..."
python3 scripts/simulation/clickstream_bot.py &
BOT_PID=$!

echo "[Simulation] Cả 2 bots đang chạy (mock_backend PID=$BACKEND_PID, bot PID=$BOT_PID)"

# Nếu có timeout: chạy sleep rồi kill cả 2 processes khi hết giờ
if [ "$TIMEOUT_SECS" -gt 0 ] 2>/dev/null; then
    (
        sleep "$TIMEOUT_SECS"
        echo "[Simulation] Đã hết thời gian ${SIMULATION_MAX_HOURS}h. Đang dừng cả 2 bots..."
        kill "$BACKEND_PID" "$BOT_PID" 2>/dev/null || true
    ) &
    TIMER_PID=$!
fi

# Chờ tất cả background processes — thoát khi cả 2 kết thúc
wait $BACKEND_PID $BOT_PID 2>/dev/null || true

# Hủy timer nếu bots kết thúc trước hạn
if [ -n "$TIMER_PID" ]; then
    kill "$TIMER_PID" 2>/dev/null || true
fi

echo "[Simulation] Tất cả bots đã kết thúc."
