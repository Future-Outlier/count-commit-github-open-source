#!/bin/bash

if [ -z "$1" ]; then
  echo "用法: $0 <本機專案目錄路徑>"
  echo "範例: $0 ~/my-java-project"
  exit 1
fi

PROJECT_DIR=$(realpath "$1")
GRADLE_USER_HOME="${HOME}/.gradle"
CONTAINER_WORKDIR="/workspace"
# 獨立指定容器內的 Gradle Cache 路徑，避免存取到 /root 導致權限錯誤
CONTAINER_GRADLE_HOME="/gradle-cache"

if [ ! -d "$PROJECT_DIR" ]; then
  echo "錯誤: 找不到指定的專案目錄 '$PROJECT_DIR'"
  exit 1
fi

mkdir -p "$GRADLE_USER_HOME"

echo "🚀 啟動獨立並行測試容器 (使用 Podman Overlay 模式)..."
echo "📂 專案掛載: $PROJECT_DIR -> $CONTAINER_WORKDIR (Copy-on-Write)"
echo "📦 快取掛載: $GRADLE_USER_HOME -> $CONTAINER_GRADLE_HOME (Copy-on-Write)"
echo "---------------------------------------------------"

# 關鍵參數說明：
# :O -> 代表 Overlay 掛載，本機目錄唯讀，容器內的修改會寫入獨立的暫存層
# -e GRADLE_USER_HOME -> 告訴 Gradle 使用我們指定的路徑作為快取目錄
podman run --rm -it \
  --userns=keep-id \
  -e GRADLE_USER_HOME="$CONTAINER_GRADLE_HOME" \
  -v "$PROJECT_DIR":"$CONTAINER_WORKDIR":O \
  -v "$GRADLE_USER_HOME":"$CONTAINER_GRADLE_HOME":O \
  -w "$CONTAINER_WORKDIR" \
  docker.io/library/eclipse-temurin:21-jdk \
  bash