# --- 設定エリア ---
TOTAL_NODES=4    # プログラムに伝える全体のノード数 (-N)
TOTAL_SHARDS=4    # 全体のシャード数 (-S)

echo "2026-05-15: ネットワークを起動します (N=$TOTAL_NODES, S=$TOTAL_SHARDS)"

# シャードのループ
for ((s=0; s<TOTAL_SHARDS; s++)); do
  # ノードのループ (0, 1, 2, 3)
  for ((n=0; n<TOTAL_NODES; n++)); do
    echo "Starting Shard: $s, Node: $n..."
    go run main.go -n $n -N $TOTAL_NODES -s $s -S $TOTAL_SHARDS &
  done
done

# コントローラーの起動
echo "Starting Controller..."
go run main.go -c -N $TOTAL_NODES -S $TOTAL_SHARDS & 