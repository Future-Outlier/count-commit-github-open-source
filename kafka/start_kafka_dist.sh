#!/bin/bash

# 預設值
BROKERS=0
CONTROLLERS=0
BROKER_CONFIGS=""
CONTROLLER_CONFIGS=""
DIST_FILE=""

# 1. 參數解析
while [[ $# -gt 0 ]]; do
  case $1 in
    --brokers) BROKERS="$2"; shift 2 ;;
    --controllers) CONTROLLERS="$2"; shift 2 ;;
    --broker-configs) BROKER_CONFIGS="$2"; shift 2 ;;
    --controller-configs) CONTROLLER_CONFIGS="$2"; shift 2 ;;
    --distribution) DIST_FILE="$2"; shift 2 ;;
    *) echo "未知的參數: $1"; exit 1 ;;
  esac
done

# 2. 驗證參數與檔案
if [[ $CONTROLLERS -le 0 || $BROKERS -le 0 ]]; then
  echo "錯誤: --brokers 和 --controllers 的數量必須大於 0。"
  exit 1
fi

if [[ -z "$DIST_FILE" || ! -f "$DIST_FILE" ]]; then
  echo "錯誤: 必須使用 --distribution 指定有效的 Kafka 壓縮檔 (例如 kafka_2.13-3.7.0.tgz)。"
  exit 1
fi

if ! command -v java &> /dev/null; then
    echo "錯誤: 找不到 java 指令，本機啟動需要安裝 Java JRE/JDK。"
    exit 1
fi

# 3. 準備 /tmp 下的工作環境與解壓縮
WORKSPACE="/tmp/kafka_workspace"
KAFKA_HOME="$WORKSPACE/kafka"
CONFIGS_DIR="$WORKSPACE/configs"
DATA_DIR="$WORKSPACE/data"
LOGS_DIR="$WORKSPACE/logs" # 用來存放 nohup 背景執行的輸出日誌

echo "==== 準備 Kafka 工作環境 (於 /tmp) ===="
mkdir -p "$CONFIGS_DIR" "$DATA_DIR" "$LOGS_DIR"
if [ ! -f "$KAFKA_HOME/bin/kafka-server-start.sh" ]; then
  echo "正在解壓縮 $DIST_FILE 至 $KAFKA_HOME ..."
  mkdir -p "$KAFKA_HOME"
  tar -xzf "$DIST_FILE" -C "$KAFKA_HOME" --strip-components=1
fi
echo "工作環境準備完畢: $WORKSPACE"

# 4. 取得 KRaft Cluster ID (使用本機 Java 執行)
echo "正在產生 KRaft Cluster ID..."
CLUSTER_ID=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)
echo "Cluster ID: $CLUSTER_ID"

# 5. 組合 Controller Quorum Voters 字串 (全部指向 localhost 的獨立 Port)
VOTERS=""
for i in $(seq 1 $CONTROLLERS); do
  CTRL_PORT=$((19080 + i))
  VOTERS="${VOTERS}${i}@localhost:${CTRL_PORT},"
done
VOTERS=${VOTERS%,}

# 6. 處理自訂配置函式
append_custom_configs() {
  local configs="$1"
  local target_file="$2"
  if [[ -n "$configs" ]]; then
    IFS=',' read -ra CONF_ARRAY <<< "$configs"
    for conf in "${CONF_ARRAY[@]}"; do
      echo "$conf" >> "$target_file"
    done
  fi
}

# 7. 啟動 Controllers (Ports: 19081, 19082...)
echo "==== 準備啟動 $CONTROLLERS 個 Controllers ===="
for i in $(seq 1 $CONTROLLERS); do
  CONF_FILE="$CONFIGS_DIR/controller-$i.properties"
  CTRL_PORT=$((19080 + i))
  NODE_DATA_DIR="$DATA_DIR/controller-$i"

  cat <<EOF > "$CONF_FILE"
process.roles=controller
node.id=$i
controller.quorum.voters=$VOTERS
controller.listener.names=CONTROLLER
listeners=CONTROLLER://localhost:$CTRL_PORT
log.dirs=$NODE_DATA_DIR
EOF
  append_custom_configs "$CONTROLLER_CONFIGS" "$CONF_FILE"

  # 格式化儲存目錄
  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$CONF_FILE" > /dev/null

  # 在背景啟動並將輸出導向日誌檔
  nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$CONF_FILE" > "$LOGS_DIR/controller-$i.log" 2>&1 &
  echo "已在背景啟動 controller-$i (Port: $CTRL_PORT, 日誌: $LOGS_DIR/controller-$i.log)"
done

# 等待 Controller 初始化
sleep 3

# 8. 啟動 Brokers (Ports: 19091, 19092...)
echo "==== 準備啟動 $BROKERS 個 Brokers ===="
for i in $(seq 1 $BROKERS); do
  node_id=$((CONTROLLERS + i))
  CONF_FILE="$CONFIGS_DIR/broker-$i.properties"
  BROKER_PORT=$((19090 + i))
  NODE_DATA_DIR="$DATA_DIR/broker-$i"

  cat <<EOF > "$CONF_FILE"
process.roles=broker
node.id=$node_id
controller.quorum.voters=$VOTERS
controller.listener.names=CONTROLLER
# 本機啟動不需區分內外網，單一 PLAINTEXT 即可
listeners=PLAINTEXT://localhost:$BROKER_PORT
advertised.listeners=PLAINTEXT://localhost:$BROKER_PORT
log.dirs=$NODE_DATA_DIR
EOF
  append_custom_configs "$BROKER_CONFIGS" "$CONF_FILE"

  # 格式化儲存目錄
  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$CONF_FILE" > /dev/null

  # 在背景啟動並將輸出導向日誌檔
  nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$CONF_FILE" > "$LOGS_DIR/broker-$i.log" 2>&1 &
  echo "已在背景啟動 broker-$i (Node ID: $node_id, Port: $BROKER_PORT, 日誌: $LOGS_DIR/broker-$i.log)"
done

echo "==== 啟動完成！ ===="
echo "所有資料與配置皆已放置於: $WORKSPACE"
echo "--- 本機端連線資訊 ---"
echo "Brokers 端點: localhost:19091, localhost:19092 ..."
echo "Controllers 端點: localhost:19081, localhost:19082 ..."
echo ""
echo "若要查看服務啟動狀況，請檢查: $LOGS_DIR 底下的 log 檔案。"
echo "重開機後 /tmp 將自動清空所有資料與背景行程。"