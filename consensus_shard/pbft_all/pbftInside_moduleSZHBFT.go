// addtional module for new consensus
package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"
)

// simple implementation of pbftHandleModule interface ...
// only for block request and use transaction relay
type SZHBFTPbftExtraHandleMod struct {
	pbftNode *PbftConsensusNode
	// pointer to pbft data

	LocalChain *chain.BlockChain
	// LocalChainのポインタ

	ZoneID uint64
}

// propose request with different types
func (rphm *SZHBFTPbftExtraHandleMod) HandleinPropose() (bool, *message.Request) {
	// 新しいブロックを同時に2種生成（ローカルブロックとグローバルブロック）
	localBlock, globalBlock := rphm.pbftNode.CurChain.GenerateSZBlock(int32(rphm.pbftNode.NodeID))

	// どちらのブロックを合意に乗せるかを決定
	var targetBlock *core.Block = nil
	if localBlock != nil {
		targetBlock = localBlock
		log.Printf("S%dN%d : 【SZHBFT】ローカルブロックをバッチ処理 。TX数：%d \n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID, len(localBlock.Body))

		r := &message.Request{
			RequestType: message.BlockRequest,
			ReqTime:     time.Now(),
		}
		r.Msg.Content = targetBlock.Encode()

		return true, r

	} else if globalBlock != nil { // グローバル処理
		targetBlock = globalBlock
		log.Printf("S%dN%d : 【SZHBFT】グローバルブロックをバッチ処理 。TX数：%d \n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID, len(globalBlock.Body))

		// 宛先をリストアップ
		targetSZs := rphm.pbftNode.CurChain.GetRelatedShards(globalBlock)

		// ⭕ message.go の定義（SenderNode）に完全準拠させて生成
		prepareMsg := message.InterSZonePrepare{
			Block:      globalBlock,
			SeqID:      rphm.pbftNode.sequenceID, // 現在の高度をターゲットにする
			View:       uint64(rphm.pbftNode.view.Load()),
			ReqTime:    time.Now(),
			SenderNode: rphm.pbftNode.RunningNode,
		}

		content, _ := json.Marshal(prepareMsg)
		mergedMsg := message.MergeMessage(message.CInterSZonePrepare, content)

		// 関係する全シャードの全ノードに対してマルチキャスト送信！
		for _, shardID := range targetSZs {
			for _, ip := range rphm.pbftNode.ip_nodeTable[shardID] {
				go networks.TcpDial(mergedMsg, ip)
			}
		}
		log.Printf("S%dN%d : [SZHBFT-Leader] 関連SZへの Inter-SZonePREPARE 一斉送信完了！📡\n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)

		// ⭕ 修正：リーダーに通知するため、false と共にリクエスト構造体を返す！
		r := &message.Request{
			RequestType: message.BlockRequest,
			ReqTime:     time.Now(),
		}
		return false, r
	}
	// 両方空なら何もしない
	return false, nil
}

// the DIY operation in preprepare
func (rphm *SZHBFTPbftExtraHandleMod) HandleinPrePrepare(ppmsg *message.PrePrepare) bool {
	// ⭕ 【安全弁】中身が空っぽの幽霊メッセージ（nil）が届いたら、即座に弾く！
	if ppmsg == nil || ppmsg.RequestMsg == nil {
		log.Println("⚠️ [SZHBFT] 空っぽの幽霊PrePrepareを検知したため、処理をスキップします。")
		return false
	}

	// 安全が確認できたら、通常のブロック検証を行う
	if rphm.pbftNode.CurChain.IsValidBlock(core.DecodeB(ppmsg.RequestMsg.Msg.Content)) != nil {
		rphm.pbftNode.pl.Plog.Printf("S%dN%d : not a valid block\n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)
		return false
	}

	rphm.pbftNode.pl.Plog.Printf("S%dN%d : the pre-prepare message is correct, putting it into the RequestPool. \n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)
	rphm.pbftNode.requestPool[string(ppmsg.Digest)] = ppmsg.RequestMsg

	return true
}

// the operation in prepare, and in pbft + tx relaying, this function does not need to do any.
func (rphm *SZHBFTPbftExtraHandleMod) HandleinPrepare(pmsg *message.Prepare) bool {
	fmt.Println("No operations are performed in Extra handle mod")
	return true
}

// the operation in commit.
func (rphm *SZHBFTPbftExtraHandleMod) HandleinCommit(cmsg *message.Commit) bool {
	r := rphm.pbftNode.requestPool[string(cmsg.Digest)]
	// requestType ...
	block := core.DecodeB(r.Msg.Content)

	// ============================================================
	// ★【新設】SZ内完結（ローカル）か、SZ間（グローバル）かの判定
	// ============================================================
	isLocalSZBlock := true
	for _, tx := range block.Body {
		ssid := rphm.pbftNode.CurChain.Get_PartitionMap(tx.Sender)
		rsid := rphm.pbftNode.CurChain.Get_PartitionMap(tx.Recipient)
		// 送信元と送信先がどちらも自シャード（SZ）ではないものが1つでもあればグローバル
		if ssid != rphm.pbftNode.ShardID || rsid != rphm.pbftNode.ShardID {
			isLocalSZBlock = false
			break
		}
	}

	if isLocalSZBlock {
		// --------------------------------------------------------
		// ①【SZ内完結ルート】EIGツリーPBFTを経てローカル台帳に記録
		// --------------------------------------------------------

		// ブロックの高さをローカル台帳の「現在の高さ + 1」に強制調整する
		localHeight := rphm.LocalChain.CurrentBlock.Header.Number + 1
		block.Header.Number = localHeight
		// 親ブロックのハッシュリンクもローカル台帳の最新ハッシュに繋ぎ直す
		block.Header.ParentBlockHash = rphm.LocalChain.CurrentBlock.Hash
		// ブロック自身のハッシュも再計算して上書き
		block.Hash = block.Header.Hash()

		rphm.pbftNode.pl.Plog.Printf("S%dN%d : [SZHBFT-Local] SZ内完結ブロックをローカル台帳に記録。adjusted height = %d \n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID, block.Header.Number)

		// ローカル専用の台帳（LocalChain）に保存
		rphm.LocalChain.AddBlock(block)

		rphm.pbftNode.pl.Plog.Printf("S%dN%d : [SZHBFT-Local] ローカル台帳への書き込み完了。現在のローカル最高高度 = %d\n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID, rphm.LocalChain.CurrentBlock.Header.Number)

		// SZ内完結なので、他シャードへの「リレー」は一切行わずにここで正常終了
		return true
	}
	// --------------------------------------------------------
	// ②【SZ間ルート】これまで通りのグローバル台帳への記録 ＆ リレー送信
	// --------------------------------------------------------
	rphm.pbftNode.pl.Plog.Printf("S%dN%d : [SZHBFT-Global] SZ間（他シャード跨ぎ）ブロックをグローバル台帳に記録します... \n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)
	rphm.pbftNode.CurChain.AddBlock(block)
	rphm.pbftNode.CurChain.PrintBlockChain()
	// =============================================================

	// now try to relay txs to other shards (for main nodes)
	if rphm.pbftNode.NodeID == uint64(rphm.pbftNode.view.Load()) {
		rphm.pbftNode.pl.Plog.Printf("S%dN%d : main node is trying to send relay txs at height = %d \n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID, block.Header.Number)
		// generate relay pool and collect txs excuted
		rphm.pbftNode.CurChain.Txpool.RelayPool = make(map[uint64][]*core.Transaction)
		interShardTxs := make([]*core.Transaction, 0)
		relay1Txs := make([]*core.Transaction, 0)
		relay2Txs := make([]*core.Transaction, 0)
		for _, tx := range block.Body {
			ssid := rphm.pbftNode.CurChain.Get_PartitionMap(tx.Sender)
			rsid := rphm.pbftNode.CurChain.Get_PartitionMap(tx.Recipient)
			if !tx.Relayed && ssid != rphm.pbftNode.ShardID {
				log.Panic("incorrect tx")
			}
			if tx.Relayed && rsid != rphm.pbftNode.ShardID {
				log.Panic("incorrect tx")
			}
			if rsid != rphm.pbftNode.ShardID {
				relay1Txs = append(relay1Txs, tx)
				tx.Relayed = true
				rphm.pbftNode.CurChain.Txpool.AddRelayTx(tx, rsid)
			} else {
				if tx.Relayed {
					relay2Txs = append(relay2Txs, tx)
				} else {
					interShardTxs = append(interShardTxs, tx)
				}
			}
		}

		// send relay txs
		if params.RelayWithMerkleProof == 1 {
			rphm.pbftNode.RelayWithProofSend(block)
		} else {
			rphm.pbftNode.RelayMsgSend()
		}

		// send txs excuted in this block to the listener
		// add more message to measure more metrics
		bim := message.BlockInfoMsg{
			BlockBodyLength: len(block.Body),
			InnerShardTxs:   interShardTxs,
			Epoch:           0,

			Relay1Txs: relay1Txs,
			Relay2Txs: relay2Txs,

			SenderShardID: rphm.pbftNode.ShardID,
			ProposeTime:   r.ReqTime,
			CommitTime:    time.Now(),
		}
		bByte, err := json.Marshal(bim)
		if err != nil {
			log.Panic()
		}
		msg_send := message.MergeMessage(message.CBlockInfo, bByte)
		go networks.TcpDial(msg_send, rphm.pbftNode.ip_nodeTable[params.SupervisorShard][0])
		rphm.pbftNode.pl.Plog.Printf("S%dN%d : sended excuted txs\n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)
		rphm.pbftNode.CurChain.Txpool.GetLocked()
		metricName := []string{
			"Block Height",
			"EpochID of this block",
			"TxPool Size",
			"# of all Txs in this block",
			"# of Relay1 Txs in this block",
			"# of Relay2 Txs in this block",
			"TimeStamp - Propose (unixMill)",
			"TimeStamp - Commit (unixMill)",

			"SUM of confirm latency (ms, All Txs)",
			"SUM of confirm latency (ms, Relay1 Txs) (Duration: Relay1 proposed -> Relay1 Commit)",
			"SUM of confirm latency (ms, Relay2 Txs) (Duration: Relay1 proposed -> Relay2 Commit)",
		}
		metricVal := []string{
			strconv.Itoa(int(block.Header.Number)),
			strconv.Itoa(bim.Epoch),
			strconv.Itoa(len(rphm.pbftNode.CurChain.Txpool.TxQueue)),
			strconv.Itoa(len(block.Body)),
			strconv.Itoa(len(relay1Txs)),
			strconv.Itoa(len(relay2Txs)),
			strconv.FormatInt(bim.ProposeTime.UnixMilli(), 10),
			strconv.FormatInt(bim.CommitTime.UnixMilli(), 10),

			strconv.FormatInt(computeTCL(block.Body, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(relay1Txs, bim.CommitTime), 10),
			strconv.FormatInt(computeTCL(relay2Txs, bim.CommitTime), 10),
		}
		rphm.pbftNode.writeCSVline(metricName, metricVal)
		rphm.pbftNode.CurChain.Txpool.GetUnlocked()
	}
	return true
}

func (rphm *SZHBFTPbftExtraHandleMod) HandleReqestforOldSeq(*message.RequestOldMessage) bool {
	fmt.Println("No operations are performed in Extra handle mod")
	return true
}

// the operation for sequential requests
func (rphm *SZHBFTPbftExtraHandleMod) HandleforSequentialRequest(som *message.SendOldMessage) bool {
	if int(som.SeqEndHeight-som.SeqStartHeight+1) != len(som.OldRequest) {
		rphm.pbftNode.pl.Plog.Printf("S%dN%d : the SendOldMessage message is not enough\n", rphm.pbftNode.ShardID, rphm.pbftNode.NodeID)
	} else { // add the block into the node pbft blockchain
		for height := som.SeqStartHeight; height <= som.SeqEndHeight; height++ {
			r := som.OldRequest[height-som.SeqStartHeight]
			if r.RequestType == message.BlockRequest {
				b := core.DecodeB(r.Msg.Content)
				rphm.pbftNode.CurChain.AddBlock(b)
			}
		}
		rphm.pbftNode.sequenceID = som.SeqEndHeight + 1
		rphm.pbftNode.CurChain.PrintBlockChain()
	}
	return true
}
