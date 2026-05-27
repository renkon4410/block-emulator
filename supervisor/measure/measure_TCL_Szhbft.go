package measure

import (
	"blockEmulator/message"
	"strconv"
	"time"
)

// to test average Transaction_Confirm_Latency (TCL)  in this system
type TestModule_TCL_Szhbft struct {
	epochID int

	totTxLatencyEpoch     []float64 // record the Transaction_Confirm_Latency in each epoch, only for excuted txs (normal txs & relay2 txs)
	szhbft1CommitLatency   []int64
	szhbft2CommitLatency   []int64
	ctxCommitLatency      []int64
	normalTxCommitLatency []int64

	szhbft1CommitTS map[string]time.Time

	normalTxNum []int
	szhbft1TxNum []int
	szhbft2TxNum []int
	txNum       []float64 // record the txNumber in each epoch
}

func NewTestModule_TCL_Szhbft() *TestModule_TCL_Szhbft {
	return &TestModule_TCL_Szhbft{
		epochID:           -1,
		totTxLatencyEpoch: make([]float64, 0),

		szhbft1CommitLatency:   make([]int64, 0),
		szhbft2CommitLatency:   make([]int64, 0),
		normalTxCommitLatency: make([]int64, 0),
		ctxCommitLatency:      make([]int64, 0),

		szhbft1CommitTS: make(map[string]time.Time),

		normalTxNum: make([]int, 0),
		szhbft1TxNum: make([]int, 0),
		szhbft2TxNum: make([]int, 0),
		txNum:       make([]float64, 0),
	}
}

func (tml *TestModule_TCL_Szhbft) OutputMetricName() string {
	return "Transaction_Confirm_Latency"
}

// modified latency
func (tml *TestModule_TCL_Szhbft) UpdateMeasureRecord(b *message.BlockInfoMsg) {
	if b.BlockBodyLength == 0 { // empty block
		return
	}

	epochid := b.Epoch
	mTime := b.CommitTime

	// extend
	for tml.epochID < epochid {
		tml.txNum = append(tml.txNum, 0)
		tml.totTxLatencyEpoch = append(tml.totTxLatencyEpoch, 0)

		tml.szhbft1CommitLatency = append(tml.szhbft1CommitLatency, 0)
		tml.szhbft2CommitLatency = append(tml.szhbft2CommitLatency, 0)
		tml.normalTxCommitLatency = append(tml.normalTxCommitLatency, 0)
		tml.ctxCommitLatency = append(tml.ctxCommitLatency, 0)

		tml.szhbft1TxNum = append(tml.szhbft1TxNum, 0)
		tml.szhbft2TxNum = append(tml.szhbft2TxNum, 0)
		tml.normalTxNum = append(tml.normalTxNum, 0)

		tml.epochID++
	}

	tml.normalTxNum[epochid] += len(b.InnerShardTxs)
	tml.szhbft1TxNum[epochid] += len(b.Relay1Txs)
	tml.szhbft2TxNum[epochid] += len(b.Relay2Txs)
	tml.txNum[epochid] += float64(len(b.InnerShardTxs)) + float64(len(b.Relay1Txs)+len(b.Relay2Txs))/2

	// szhbft1 tx
	for _, s1tx := range b.Relay1Txs {
		tml.szhbft1CommitTS[string(s1tx.TxHash)] = mTime
		tml.szhbft1CommitLatency[epochid] += int64(mTime.Sub(s1tx.Time).Milliseconds())
	}

	// szhbft2 tx
	for _, s2tx := range b.Relay2Txs {
		tml.totTxLatencyEpoch[epochid] += mTime.Sub(s2tx.Time).Seconds()

		if s1CommitTime, ok := tml.szhbft1CommitTS[string(s2tx.TxHash)]; ok {
			tml.szhbft2CommitLatency[epochid] += int64(mTime.Sub(s1CommitTime).Milliseconds())
			tml.ctxCommitLatency[epochid] += int64(mTime.Sub(s2tx.Time).Milliseconds())
		}
	}

	// normal tx
	for _, ntx := range b.InnerShardTxs {
		tml.totTxLatencyEpoch[epochid] += mTime.Sub(ntx.Time).Seconds()

		tml.normalTxCommitLatency[epochid] += int64(mTime.Sub(ntx.Time).Milliseconds())
	}
}

func (tml *TestModule_TCL_Szhbft) HandleExtraMessage([]byte) {}

func (tml *TestModule_TCL_Szhbft) OutputRecord() (perEpochLatency []float64, totLatency float64) {
	tml.writeToCSV()

	// calculate the simple result
	perEpochLatency = make([]float64, 0)
	latencySum := 0.0
	totTxNum := 0.0
	for eid, totLatency := range tml.totTxLatencyEpoch {
		perEpochLatency = append(perEpochLatency, totLatency/tml.txNum[eid])
		latencySum += totLatency
		totTxNum += tml.txNum[eid]
	}
	totLatency = latencySum / totTxNum
	return
}

func (tml *TestModule_TCL_Szhbft) writeToCSV() {
	fileName := tml.OutputMetricName()
	measureName := []string{
		"EpochID",
		"Total tx # in this epoch",
		"Normal tx # in this epoch",
		"Szhbft1 tx # in this epoch",
		"Szhbft2 tx # in this epoch",
		"Sum of Szhbft1 TCL (ms) (Duration: Szhbft1 Tx Propose -> Szhbft1 Tx Commit)",
		"Sum of Szhbft2 TCL (ms) (Duration: Szhbft2 Tx Propose -> Szhbft2 Tx Commit)",
		"Sum of innerShardTx TCL (ms)",
		"Sum of CTX TCL (ms) (Duration: Szhbft1 Tx Propose -> Szhbft2 Tx Commit)",
		"Sum of All Tx TCL (sec.)"}
	measureVals := make([][]string, 0)

	for eid, totTxInE := range tml.txNum {
		csvLine := []string{
			strconv.Itoa(eid),
			strconv.FormatFloat(totTxInE, 'f', '8', 64),
			strconv.Itoa(tml.normalTxNum[eid]),
			strconv.Itoa(tml.szhbft1TxNum[eid]),
			strconv.Itoa(tml.szhbft2TxNum[eid]),
			strconv.FormatInt(tml.szhbft1CommitLatency[eid], 10),
			strconv.FormatInt(tml.szhbft2CommitLatency[eid], 10),
			strconv.FormatInt(tml.normalTxCommitLatency[eid], 10),
			strconv.FormatInt(tml.ctxCommitLatency[eid], 10),
			strconv.FormatFloat(tml.totTxLatencyEpoch[eid], 'f', '8', 64),
		}
		measureVals = append(measureVals, csvLine)
	}
	WriteMetricsToCSV(fileName, measureName, measureVals)
}
