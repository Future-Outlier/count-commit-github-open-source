#!/bin/bash

# ==========================================
# 1. 自動偵測系統資源並計算一半作為預設值
# ==========================================
# 取得系統 CPU 核心數
if command -v nproc &> /dev/null; then
    TOTAL_CPU=$(nproc)
elif command -v sysctl &> /dev/null; then
    TOTAL_CPU=$(sysctl -n hw.ncpu)
else
    TOTAL_CPU=4 # 預設 fallback
fi
HALF_CPU=$(( TOTAL_CPU / 2 ))
[[ $HALF_CPU -lt 1 ]] && HALF_CPU=1

# 取得系統實體記憶體並計算一半 (轉換為 MB)
if [ -f /proc/meminfo ]; then
    TOTAL_MEM_KB=$(awk '/MemTotal/ {print $2}' /proc/meminfo)
    HALF_MEM_MB=$(( TOTAL_MEM_KB / 1024 / 2 ))
elif command -v sysctl &> /dev/null; then
    TOTAL_MEM_BYTES=$(sysctl -n hw.memsize)
    HALF_MEM_MB=$(( TOTAL_MEM_BYTES / 1024 / 1024 / 2 ))
else
    HALF_MEM_MB=4096 # 預設 fallback 4GB
fi

# ==========================================
# 2. 設定預設變數
# ==========================================
DEFAULT_DIR="$HOME/project/kafka"
TARGET_CPU=$HALF_CPU
TARGET_MEM="${HALF_MEM_MB}m"
PROJECT_DIR_ARG=""

# ==========================================
# 3. 解析命令列參數
# ==========================================
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -c|--cpus) TARGET_CPU="$2"; shift ;;
        -m|--memory) TARGET_MEM="$2"; shift ;;
        -h|--help)
            echo "用法: $0 [選項] [本機專案目錄路徑]"
            echo "選項:"
            echo "  -c, --cpus <數量>     設定容器使用的 CPU 核心數 (預設: 系統一半, 即 $HALF_CPU)"
            echo "  -m, --memory <大小>   設定容器記憶體大小，支援單位 m, g (預設: 系統一半, 即 ${HALF_MEM_MB}m)"
            echo "  -h, --help            顯示此幫助訊息"
            echo ""
            echo "範例: $0 -c 4 -m 8g ~/my-java-project"
            echo "若未提供路徑，預設使用: $DEFAULT_DIR"
            exit 0
            ;;
        *) PROJECT_DIR_ARG="$1" ;; # 捕捉路徑參數
    esac
    shift
done

# ==========================================
# 4. 路徑與環境檢查
# ==========================================
# 如果沒有提供路徑參數，就使用預設路徑
PROJECT_DIR=$(realpath "${PROJECT_DIR_ARG:-$DEFAULT_DIR}")
GRADLE_USER_HOME="${HOME}/.gradle"
CONTAINER_WORKDIR="/workspace"
# 獨立指定容器內的 Gradle Cache 路徑，避免存取到 /root 導致權限錯誤
CONTAINER_GRADLE_HOME="/gradle-cache"

if [ ! -d "$PROJECT_DIR" ]; then
  echo "錯誤: 找不到指定的專案目錄 '$PROJECT_DIR'"
  exit 1
fi

mkdir -p "$GRADLE_USER_HOME"

# ==========================================
# 5. 啟動 Podman
# ==========================================
echo "🚀 啟動獨立並行測試容器 (使用 Podman Overlay 模式)..."
echo "📂 專案掛載: $PROJECT_DIR -> $CONTAINER_WORKDIR (Copy-on-Write)"
echo "📦 快取掛載: $GRADLE_USER_HOME -> $CONTAINER_GRADLE_HOME (Copy-on-Write)"
echo "⚙️  資源限制: CPU: $TARGET_CPU 核心, 記憶體: $TARGET_MEM"
echo "---------------------------------------------------"

# 關鍵參數說明：
# :O -> 代表 Overlay 掛載，本機目錄唯讀，容器內的修改會寫入獨立的暫存層
# -e GRADLE_USER_HOME -> 告訴 Gradle 使用我們指定的路徑作為快取目錄
# --cpus / --memory -> 限制容器資源
podman run --rm -it \
  --userns=keep-id \
  --cpus="$TARGET_CPU" \
  --memory="$TARGET_MEM" \
  -e GRADLE_USER_HOME="$CONTAINER_GRADLE_HOME" \
  -v "$PROJECT_DIR":"$CONTAINER_WORKDIR":O \
  -v "$GRADLE_USER_HOME":"$CONTAINER_GRADLE_HOME":O \
  -w "$CONTAINER_WORKDIR" \
  docker.io/library/eclipse-temurin:21-jdk \
  bash