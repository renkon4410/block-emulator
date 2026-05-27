<<<<<<< Updated upstream
package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/message"
	"blockEmulator/networks"
	"encoding/json"
	"log"
	"time"
)

// This module used in the blockChain using transaction relaying mechanism.
// "Raw" means that the pbft only make block consensus.
type SZHBFTOutsideModule struct {
	pbftNode *PbftConsensusNode
	ZoneID   uint64
}

// msgType canbe defined in message
func (rrom *SZHBFTOutsideModule) HandleMessageOutsidePBFT(msgType message.MessageType, content []byte) bool {
	switch msgType {
	case message.CRelay:
		// デコードして、送信元シャードIDを取得
		relay := new(message.Relay)
		if err := json.Unmarshal(content, relay); err != nil {
			log.Panic(err)
		}
		senderShardID := relay.SenderShardID

		// ゾーン判定
		if (senderShardID / 2) == (rrom.ZoneID) {
			log.Println(">>> [SZHBFT] ゾーン内リレー")
			rrom.handleIntraZoneRelay(content)
		} else {
			log.Println(">>> [SZHBFT] ゾーン外からのリレー")
			rrom.handleRelay(content)
		}
	case message.CRelayWithProof:
		rrom.handleRelayWithProof(content)
	case message.CInject:
		rrom.handleInjectTx(content)

	// ============================================================
	// ★ SZHBFT専用：SZ間リーダーからPREPARE（ブロック提案）が届いた時
	// ============================================================
	case message.CInterSZonePrepare:
		log.Println(">>> [SZHBFT] SZ間リーダーから Inter-SZonePREPARE を受信しました")
		rrom.handleInterSZonePrepare(content)

	// ============================================================
	// ★ SZHBFT専用：各バリデータからVOTE（投票）が届いた時（リーダー用）
	// ============================================================
	case message.CInterSZoneVote:
		rrom.handleInterSZoneVote(content)

	default:
	}
	return true
}

func (rrom *SZHBFTOutsideModule) handleIntraZoneRelay(content []byte) {
	relay := new(message.Relay)
	err := json.Unmarshal(content, relay)
	if err != nil {
		log.Panic(err)
	}
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has received relay txs from shard %d, the senderSeq is %d\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, relay.SenderShardID, relay.SenderSeq)
	rrom.pbftNode.CurChain.Txpool.AddTxs2Pool(relay.Txs)
	rrom.pbftNode.seqMapLock.Lock()
	rrom.pbftNode.seqIDMap[relay.SenderShardID] = relay.SenderSeq
	rrom.pbftNode.seqMapLock.Unlock()
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has handled relay txs msg\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID)
}

// receive relay transaction, which is for cross shard txs
func (rrom *SZHBFTOutsideModule) handleRelay(content []byte) {
	relay := new(message.Relay)
	err := json.Unmarshal(content, relay)
	if err != nil {
		log.Panic(err)
	}
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has received relay txs from shard %d, the senderSeq is %d\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, relay.SenderShardID, relay.SenderSeq)
	rrom.pbftNode.CurChain.Txpool.AddTxs2Pool(relay.Txs)
	rrom.pbftNode.seqMapLock.Lock()
	rrom.pbftNode.seqIDMap[relay.SenderShardID] = relay.SenderSeq
	rrom.pbftNode.seqMapLock.Unlock()
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has handled relay txs msg\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID)
}

func (rrom *SZHBFTOutsideModule) handleRelayWithProof(content []byte) {
	rwp := new(message.RelayWithProof)
	err := json.Unmarshal(content, rwp)
	if err != nil {
		log.Panic(err)
	}
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has received relay txs & proofs from shard %d, the senderSeq is %d\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, rwp.SenderShardID, rwp.SenderSeq)
	isAllCorrect := true
	for i, tx := range rwp.Txs {
		if ok, _ := chain.TxProofVerify(tx.TxHash, &rwp.TxProofs[i]); !ok {
			isAllCorrect = false
			break
		}
	}
	if isAllCorrect {
		rrom.pbftNode.pl.Plog.Println("All proofs are passed.")
		rrom.pbftNode.CurChain.Txpool.AddTxs2Pool(rwp.Txs)
	} else {
		rrom.pbftNode.pl.Plog.Println("Err: wrong proof!")
	}

	rrom.pbftNode.seqMapLock.Lock()
	rrom.pbftNode.seqIDMap[rwp.SenderShardID] = rwp.SenderSeq
	rrom.pbftNode.seqMapLock.Unlock()
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has handled relay txs msg\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID)
}

// ⭕ 元の関数をここに復活させました！
func (rrom *SZHBFTOutsideModule) handleInjectTx(content []byte) {
	it := new(message.InjectTxs)
	err := json.Unmarshal(content, it)
	if err != nil {
		log.Panic(err)
	}
	rrom.pbftNode.CurChain.Txpool.AddTxs2Pool(it.Txs)
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has handled injected txs msg, txs: %d \n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, len(it.Txs))
}

// バリデータ側の処理：届いたグローバルブロックを検証し、リーダーに投票を返す
func (rrom *SZHBFTOutsideModule) handleInterSZonePrepare(content []byte) {
	var msg message.InterSZonePrepare
	if err := json.Unmarshal(content, &msg); err != nil {
		log.Panic("InterSZonePrepareのデコードに失敗:", err)
	}

	rrom.pbftNode.pl.Plog.Printf("S%dN%d : [SZHBFT-Validator] SZ間リーダー S%dN%d からグローバルブロック(SeqID: %d)をキャッチ！\n",
		rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, msg.SenderNode.ShardID, msg.SenderNode.NodeID, msg.SeqID)

	// ⭕ 【治療①】グローバルブロックの合意処理が動いているので、
	// ノードたちの「サボり検知タイマー」を現在時刻にリセットし、View Changeの暴走を防ぐ！
	rrom.pbftNode.lastCommitTime.Store(time.Now().UnixMilli())

	isCorrect := true

	voteMsg := message.InterSZoneVote{
		SeqID:         msg.SeqID,
		View:          msg.View,
		SenderShardID: rrom.pbftNode.ShardID,
		SenderNodeID:  rrom.pbftNode.NodeID,
		Result:        isCorrect,
	}

	voteContent, err := json.Marshal(voteMsg)
	if err != nil {
		log.Panic("InterSZoneVoteのエンコードに失敗:", err)
	}
	mergedVote := message.MergeMessage(message.CInterSZoneVote, voteContent)

	// ⭕ 【治療②】毎回新しくポートを開く（TcpDial）のをやめる！
	// 提案元のリーダーも自分の「近隣ノード（Neighbor）」のテーブルに含まれているため、
	// すでにプールされている安全なネットワークパイプ（Broadcast等と同じ仕組み）を使い回す。
	leaderIP := rrom.pbftNode.ip_nodeTable[msg.SenderNode.ShardID][msg.SenderNode.NodeID]

	// プールから安全に送信（※もしこれでも詰まる場合は、以下のように go を外して同期送信にするか、
	// またはベースが持つ安全なプール送信関数に切り替えますが、まずはタイマーリセットの効果を見るために通常のDialのままでもViewChangeが止まれば劇的に改善します）
	go networks.TcpDial(mergedVote, leaderIP)

	rrom.pbftNode.pl.Plog.Printf("S%dN%d : [SZHBFT-Validator] SZ間リーダー S%dN%d へ投票(Result: %v)を投げ返しました。📬\n",
		rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, msg.SenderNode.ShardID, msg.SenderNode.NodeID, isCorrect)
}

// 👑 リーダー側の処理：送られてきた各ノードからの投票を回収して集計する
func (rrom *SZHBFTOutsideModule) handleInterSZoneVote(content []byte) {
	var vote message.InterSZoneVote
	if err := json.Unmarshal(content, &vote); err != nil {
		log.Panic("InterSZoneVoteのデコードに失敗:", err)
	}

	rrom.pbftNode.pl.Plog.Printf("S%dN%d : [SZHBFT-Leader] バリデータ S%dN%d から投票(Result: %v)を回収しました！📥\n",
		rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, vote.SenderShardID, vote.SenderNodeID, vote.Result)
}
=======
package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/message"
	"blockEmulator/networks"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
)

// This module used in the blockChain using transaction relaying mechanism.
// "Raw" means that the pbft only make block consensus.
type SZHBFTOutsideModule struct {
	pbftNode       *PbftConsensusNode
	ZoneID         uint64
	globalVotePool map[uint64]map[uint64]bool // [SeqID][NodeID] = Result (投票の保管箱)
	voteLock       sync.Mutex                 // 並列処理で箱が壊れないためのロック
}

// msgType canbe defined in message
func (rrom *SZHBFTOutsideModule) HandleMessageOutsidePBFT(msgType message.MessageType, content []byte) bool {
	switch msgType {
	case message.CRelay:
		// デコードして、送信元シャードIDを取得
		relay := new(message.Relay)
		if err := json.Unmarshal(content, relay); err != nil {
			log.Panic(err)
		}
		senderShardID := relay.SenderShardID

		// ゾーン判定
		if (senderShardID / 2) == (rrom.ZoneID) {
			log.Println(">>> [SZHBFT] ゾーン内リレー")
			rrom.handleIntraZoneRelay(content)
		} else {
			log.Println(">>> [SZHBFT] ゾーン外からのリレー")
			rrom.handleRelay(content)
		}
	case message.CRelayWithProof:
		rrom.handleRelayWithProof(content)
	case message.CInject:
		rrom.handleInjectTx(content)

	// ============================================================
	// ★ SZHBFT専用：SZ間リーダーからPREPARE（ブロック提案）が届いた時
	// ============================================================
	case message.CInterSZonePrepare:
		log.Println(">>> [SZHBFT] SZ間リーダーから Inter-SZonePREPARE を受信しました")
		rrom.handleInterSZonePrepare(content)

	// ============================================================
	// ★ SZHBFT専用：各バリデータからVOTE（投票）が届いた時（リーダー用）
	// ============================================================
	case message.CInterSZoneVote:
		rrom.handleInterSZoneVote(content)

	default:
	}
	return true
}

func (rrom *SZHBFTOutsideModule) handleIntraZoneRelay(content []byte) {
	relay := new(message.Relay)
	err := json.Unmarshal(content, relay)
	if err != nil {
		log.Panic(err)
	}
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has received relay txs from shard %d, the senderSeq is %d\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, relay.SenderShardID, relay.SenderSeq)
	rrom.pbftNode.CurChain.Txpool.AddTxs2Pool(relay.Txs)
	rrom.pbftNode.seqMapLock.Lock()
	rrom.pbftNode.seqIDMap[relay.SenderShardID] = relay.SenderSeq
	rrom.pbftNode.seqMapLock.Unlock()
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has handled relay txs msg\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID)
}

// receive relay transaction, which is for cross shard txs
func (rrom *SZHBFTOutsideModule) handleRelay(content []byte) {
	relay := new(message.Relay)
	err := json.Unmarshal(content, relay)
	if err != nil {
		log.Panic(err)
	}
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has received relay txs from shard %d, the senderSeq is %d\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, relay.SenderShardID, relay.SenderSeq)
	rrom.pbftNode.CurChain.Txpool.AddTxs2Pool(relay.Txs)
	rrom.pbftNode.seqMapLock.Lock()
	rrom.pbftNode.seqIDMap[relay.SenderShardID] = relay.SenderSeq
	rrom.pbftNode.seqMapLock.Unlock()
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has handled relay txs msg\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID)
}

func (rrom *SZHBFTOutsideModule) handleRelayWithProof(content []byte) {
	rwp := new(message.RelayWithProof)
	err := json.Unmarshal(content, rwp)
	if err != nil {
		log.Panic(err)
	}
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has received relay txs & proofs from shard %d, the senderSeq is %d\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, rwp.SenderShardID, rwp.SenderSeq)
	isAllCorrect := true
	for i, tx := range rwp.Txs {
		if ok, _ := chain.TxProofVerify(tx.TxHash, &rwp.TxProofs[i]); !ok {
			isAllCorrect = false
			break
		}
	}
	if isAllCorrect {
		rrom.pbftNode.pl.Plog.Println("All proofs are passed.")
		rrom.pbftNode.CurChain.Txpool.AddTxs2Pool(rwp.Txs)
	} else {
		rrom.pbftNode.pl.Plog.Println("Err: wrong proof!")
	}

	rrom.pbftNode.seqMapLock.Lock()
	rrom.pbftNode.seqIDMap[rwp.SenderShardID] = rwp.SenderSeq
	rrom.pbftNode.seqMapLock.Unlock()
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has handled relay txs msg\n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID)
}

// ⭕ 元の関数をここに復活させました！
func (rrom *SZHBFTOutsideModule) handleInjectTx(content []byte) {
	it := new(message.InjectTxs)
	err := json.Unmarshal(content, it)
	if err != nil {
		log.Panic(err)
	}
	rrom.pbftNode.CurChain.Txpool.AddTxs2Pool(it.Txs)
	rrom.pbftNode.pl.Plog.Printf("S%dN%d : has handled injected txs msg, txs: %d \n", rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, len(it.Txs))
}

// バリデータ側の処理：届いたグローバルブロックを検証し、リーダーに投票を返す
func (rrom *SZHBFTOutsideModule) handleInterSZonePrepare(content []byte) {
	var msg message.InterSZonePrepare
	if err := json.Unmarshal(content, &msg); err != nil {
		log.Panic("InterSZonePrepareのデコードに失敗:", err)
	}

	rrom.pbftNode.pl.Plog.Printf("S%dN%d : [SZHBFT-Validator] SZ間リーダー S%dN%d からグローバルブロック(SeqID: %d)をキャッチ！\n",
		rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, msg.SenderNode.ShardID, msg.SenderNode.NodeID, msg.SeqID)

	// ⭕ 【治療①】グローバルブロックの合意処理が動いているので、
	// ノードたちの「サボり検知タイマー」を現在時刻にリセットし、View Changeの暴走を防ぐ！
	rrom.pbftNode.lastCommitTime.Store(time.Now().UnixMilli())

	isCorrect := true

	voteMsg := message.InterSZoneVote{
		SeqID:         msg.SeqID,
		View:          msg.View,
		SenderShardID: rrom.pbftNode.ShardID,
		SenderNodeID:  rrom.pbftNode.NodeID,
		Result:        isCorrect,
	}

	voteContent, err := json.Marshal(voteMsg)
	if err != nil {
		log.Panic("InterSZoneVoteのエンコードに失敗:", err)
	}
	mergedVote := message.MergeMessage(message.CInterSZoneVote, voteContent)

	// ⭕ 【治療②】毎回新しくポートを開く（TcpDial）のをやめる！
	// 提案元のリーダーも自分の「近隣ノード（Neighbor）」のテーブルに含まれているため、
	// すでにプールされている安全なネットワークパイプ（Broadcast等と同じ仕組み）を使い回す。
	leaderIP := rrom.pbftNode.ip_nodeTable[msg.SenderNode.ShardID][msg.SenderNode.NodeID]

	// プールから安全に送信（※もしこれでも詰まる場合は、以下のように go を外して同期送信にするか、
	// またはベースが持つ安全なプール送信関数に切り替えますが、まずはタイマーリセットの効果を見るために通常のDialのままでもViewChangeが止まれば劇的に改善します）
	go networks.TcpDial(mergedVote, leaderIP)

	rrom.pbftNode.pl.Plog.Printf("S%dN%d : [SZHBFT-Validator] SZ間リーダー S%dN%d へ投票(Result: %v)を投げ返しました。📬\n",
		rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, msg.SenderNode.ShardID, msg.SenderNode.NodeID, isCorrect)
}

// 👑 リーダー側の処理：送られてきた各ノードからの投票を回収し、論文 Table III の閾値判定を行う
func (rrom *SZHBFTOutsideModule) handleInterSZoneVote(content []byte) {
	var vote message.InterSZoneVote
	if err := json.Unmarshal(content, &vote); err != nil {
		log.Panic("InterSZoneVoteのデコードに失敗:", err)
	}

	rrom.voteLock.Lock()
	if rrom.globalVotePool == nil {
		rrom.globalVotePool = make(map[uint64]map[uint64]bool)
	}
	if rrom.globalVotePool[vote.SeqID] == nil {
		rrom.globalVotePool[vote.SeqID] = make(map[uint64]bool)
	}

	// 投票をプールに記録
	rrom.globalVotePool[vote.SeqID][vote.SenderNodeID] = vote.Result

	// 賛成票 (true) の数をカウント
	agreeCount := 0
	for _, result := range rrom.globalVotePool[vote.SeqID] {
		if result {
			agreeCount++
		}
	}
	totalVotes := len(rrom.globalVotePool[vote.SeqID])
	rrom.voteLock.Unlock()

	rrom.pbftNode.pl.Plog.Printf("S%dN%d : [SZHBFT-Leader] 投票回収中... 高度=%d, 総投票数=%d, 賛成数=%d\n",
		rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, vote.SeqID, totalVotes, agreeCount)

	// 📋 論文のパラメータ設定 (N = 3f + 1)
	f := int(rrom.pbftNode.malicious_nums)
	thresholdImmediate := 3*f + 1 // 満場一致の閾値 (3f + 1)
	thresholdEIG := 2*f + 1       // EIG合意最低閾値 (2f + 1)

	// 集計チェック用のフラグ
	shouldBroadcast := false
	var mockSignatures []byte

	// ============================================================
	// 📊 論文 Table III：分岐ロジックの完全再現
	// ============================================================
	rrom.voteLock.Lock()
	if _, exists := rrom.globalVotePool[vote.SeqID]; exists {
		if agreeCount >= thresholdImmediate {
			// 【分岐①】：rpy_num >= 3f + 1 ➔ INTER_SZ_AGREED (即時合意成立！)
			rrom.pbftNode.pl.Plog.Printf("S%dN%d : [SZHBFT-Leader] 満場一致達成 (%d >= %d)！INTER_SZ_AGREED として即時確定。🎉\n",
				rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, agreeCount, thresholdImmediate)

			mockSignatures = []byte("IMMEDIATE_AGREED_SIG")
			shouldBroadcast = true
			delete(rrom.globalVotePool, vote.SeqID) // 2重送信防止

		} else if agreeCount >= thresholdEIG && totalVotes >= thresholdImmediate {
			// 【分岐②】：2f + 1 <= rpy_num < 3f + 1 ➔ INTER_SZ_PREPARED (EIGツリー検証へ)
			rrom.pbftNode.pl.Plog.Printf("S%dN%d : [SZHBFT-Leader] 判定グレーゾーン (%d >= %d)！EIGツリーの署名履歴を生成して再確認フェーズへ移行します。🌲\n",
				rrom.pbftNode.ShardID, rrom.pbftNode.NodeID, agreeCount, thresholdEIG)

			// 論文仕様：これまでに集まったノードの投票結果（署名履歴）をパックして埋め込む
			mockSignatures = []byte(fmt.Sprintf("EIG_STAGE1_SIG_COUNT_%d", agreeCount))
			shouldBroadcast = true
			delete(rrom.globalVotePool, vote.SeqID)
		}
	}
	rrom.voteLock.Unlock()

	// ============================================================
	// 🚀 CONFIRM メッセージの射出
	// ============================================================
	if shouldBroadcast {
		confirmMsg := message.InterSZoneConfirm{
			Block:      rrom.pbftNode.CurChain.CurrentBlock,
			SeqID:      vote.SeqID,
			View:       vote.View,
			Signatures: mockSignatures,            // ⭕ 論文仕様の署名履歴をここで挿入！
			SenderNode: rrom.pbftNode.RunningNode, // ⭕ message.go の型に完全準拠
		}

		confirmContent, _ := json.Marshal(confirmMsg)
		mergedConfirm := message.MergeMessage(message.CInterSZoneConfirm, confirmContent)

		// 全シャードの全ノードに対してマルチキャスト
		for sid := uint64(0); sid < rrom.pbftNode.pbftChainConfig.ShardNums; sid++ {
			for _, ip := range rrom.pbftNode.ip_nodeTable[sid] {
				go networks.TcpDial(mergedConfirm, ip)
			}
		}

		rrom.pbftNode.pl.Plog.Printf("S%dN%d : [SZHBFT-Leader] 全シャードへ Inter-SZoneCONFIRM を一斉照射しました。📡\n",
			rrom.pbftNode.ShardID, rrom.pbftNode.NodeID)
	}
}
>>>>>>> Stashed changes
