#!/bin/bash

set -e

if [ "$#" -lt 1 ]; then
    echo "用法: $0 <kafka_source_dir> [key=value,k2=v2...]"
    exit 1
fi

SRC_DIR="$1"
EXTRA_PROPS="$2"

# 定義工作區與主機掛載路徑
WORKSPACE="/tmp/kafka-deploy"
DIST_DIR="$WORKSPACE/dist"
CONFIG_DIR="$WORKSPACE/configs"
DATA_DIR="/tmp/kafka-data"

echo "🧹 正在清理舊的工作區與資料目錄..."
rm -rf "$WORKSPACE" "$DATA_DIR"
mkdir -p "$DIST_DIR" "$CONFIG_DIR"

echo "🚀 [1/5] 從原始碼編譯並打包 Distribution..."
cd "$SRC_DIR"
./gradlew clean releaseTarGz -x test
TAR_FILE=$(find core/build/distributions -name "kafka_*.tgz" | head -n 1)

if [ -z "$TAR_FILE" ]; then
    echo "❌ 找不到編譯後的 tarball，請確認編譯過程是否成功。"
    exit 1
fi

echo "📦 [2/5] 正在解壓縮 Distribution..."
tar -xzf "$TAR_FILE" -C "$DIST_DIR"
KAFKA_DIR_NAME=$(ls "$DIST_DIR" | head -n 1)
KAFKA_HOME="$DIST_DIR/$KAFKA_DIR_NAME"

echo "🔑 [3/5] 產生 KRaft Cluster ID..."
# 使用 podman 並加上 :z 處理掛載權限
CLUSTER_ID=$(podman run --rm -v "$KAFKA_HOME:/opt/kafka:z" eclipse-temurin:21-jre /opt/kafka/bin/kafka-storage.sh random-uuid)
echo "Cluster ID: $CLUSTER_ID"

echo "⚙️ [4/5] 生成配置檔與掛載目錄..."

# 解析外部傳入的 properties
FORMATTED_EXTRA_PROPS=""
if [ -n "$EXTRA_PROPS" ]; then
    FORMATTED_EXTRA_PROPS=$(echo "$EXTRA_PROPS" | tr ',' '\n')
fi

# 準備 Podman Compose 檔案內容
COMPOSE_FILE="$WORKSPACE/docker-compose.yml"
cat << EOF > "$COMPOSE_FILE"
version: '3.8'
networks:
  kafka-net:
    driver: bridge
services:
EOF

VOTERS="1@controller-1:9093,2@controller-2:9093,3@controller-3:9093"

# 1. 配置 3 個 Controller (Node ID: 1~3)
for i in {1..3}; do
    NODE_ID=$i
    PROP_FILE="$CONFIG_DIR/controller-$NODE_ID.properties"
    HOST_DIR="$DATA_DIR/controller-$NODE_ID/log1"
    mkdir -p "$HOST_DIR"

    cat << EOF > "$PROP_FILE"
process.roles=controller
node.id=$NODE_ID
controller.quorum.voters=$VOTERS
listeners=CONTROLLER://:9093
controller.listener.names=CONTROLLER
log.dirs=/tmp/kafka-data/log1
$FORMATTED_EXTRA_PROPS
EOF

    # 注意 volume 的結尾都加上了 :z
    cat << EOF >> "$COMPOSE_FILE"
  controller-$NODE_ID:
    image: eclipse-temurin:21-jre
    container_name: controller-$NODE_ID
    hostname: controller-$NODE_ID
    networks:
      - kafka-net
    command: >
      bash -c "/opt/kafka/bin/kafka-storage.sh format -t $CLUSTER_ID -c /etc/kafka/server.properties --ignore-formatted &&
               exec /opt/kafka/bin/kafka-server-start.sh /etc/kafka/server.properties"
    volumes:
      - "$KAFKA_HOME:/opt/kafka:z"
      - "$PROP_FILE:/etc/kafka/server.properties:z"
      - "$HOST_DIR:/tmp/kafka-data/log1:z"
EOF
done

# 2. 配置 3 個 Broker (Node ID: 4~6)
for i in {4..6}; do
    NODE_ID=$i
    PORT=$((9092 + i - 4)) # Map host ports 9092, 9093, 9094
    PROP_FILE="$CONFIG_DIR/broker-$NODE_ID.properties"

    # 每個 broker 建立 3 個目錄
    HOST_DIR1="$DATA_DIR/broker-$NODE_ID/log1"
    HOST_DIR2="$DATA_DIR/broker-$NODE_ID/log2"
    HOST_DIR3="$DATA_DIR/broker-$NODE_ID/log3"
    mkdir -p "$HOST_DIR1" "$HOST_DIR2" "$HOST_DIR3"

    cat << EOF > "$PROP_FILE"
process.roles=broker
node.id=$NODE_ID
controller.quorum.voters=$VOTERS
listeners=PLAINTEXT://:9092
advertised.listeners=PLAINTEXT://localhost:$PORT
log.dirs=/tmp/kafka-data/log1,/tmp/kafka-data/log2,/tmp/kafka-data/log3
$FORMATTED_EXTRA_PROPS
EOF

    cat << EOF >> "$COMPOSE_FILE"
  broker-$NODE_ID:
    image: eclipse-temurin:21-jre
    container_name: broker-$NODE_ID
    hostname: broker-$NODE_ID
    networks:
      - kafka-net
    ports:
      - "$PORT:9092"
    depends_on:
      - controller-1
      - controller-2
      - controller-3
    command: >
      bash -c "/opt/kafka/bin/kafka-storage.sh format -t $CLUSTER_ID -c /etc/kafka/server.properties --ignore-formatted &&
               exec /opt/kafka/bin/kafka-server-start.sh /etc/kafka/server.properties"
    volumes:
      - "$KAFKA_HOME:/opt/kafka:z"
      - "$PROP_FILE:/etc/kafka/server.properties:z"
      - "$HOST_DIR1:/tmp/kafka-data/log1:z"
      - "$HOST_DIR2:/tmp/kafka-data/log2:z"
      - "$HOST_DIR3:/tmp/kafka-data/log3:z"
EOF
done

echo "🐳 [5/5] 啟動 Kafka KRaft Quorum 與 Broker 叢集 (Podman)..."
cd "$WORKSPACE"
podman compose up -d

echo ""
echo "✅ 部署完成！"
echo "📌 Distribution 位置: $KAFKA_HOME"
echo "📌 設定檔位置: $CONFIG_DIR"
echo "📌 Controller 資料目錄: /tmp/kafka-data/controller-{1..3}"
echo "📌 Broker 資料目錄: /tmp/kafka-data/broker-{4..6}"
echo "🔌 對外 Broker 介面: localhost:9092, localhost:9093, localhost:9094"
echo ""
echo "你可以執行 'podman compose -f $COMPOSE_FILE logs -f' 來觀看日誌。"