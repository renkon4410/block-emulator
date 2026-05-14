package measure

import (
	"blockEmulator/message"
	"strconv"
)

// to test cross-transaction rate
type TestCrossTxRate_Szhbft struct {
	epochID int

	normalTxNum []int
	szhbft1TxNum []int
	szhbft2TxNum []int

	totTxNum      []float64
	totCrossTxNum []float64
}

func NewTestCrossTxRate_Szhbft() *TestCrossTxRate_Szhbft {
	return &TestCrossTxRate_Szhbft{
		epochID:       -1,
		totTxNum:      make([]float64, 0),
		totCrossTxNum: make([]float64, 0),

		normalTxNum: make([]int, 0),
		szhbft1TxNum: make([]int, 0),
		szhbft2TxNum: make([]int, 0),
	}
}

func (tctr *TestCrossTxRate_Szhbft) OutputMetricName() string {
	return "CrossTransaction_ratio"
}

func (tctr *TestCrossTxRate_Szhbft) UpdateMeasureRecord(b *message.BlockInfoMsg) {
	if b.BlockBodyLength == 0 { // empty block
		return
	}

	epochid := b.Epoch
	r1TxNum := len(b.Relay1Txs)
	r2TxNum := len(b.Relay2Txs)

	// extend
	for tctr.epochID < epochid {
		tctr.totTxNum = append(tctr.totTxNum, 0)
		tctr.totCrossTxNum = append(tctr.totCrossTxNum, 0)

		tctr.szhbft1TxNum = append(tctr.szhbft1TxNum, 0)
		tctr.szhbft2TxNum = append(tctr.szhbft2TxNum, 0)
		tctr.normalTxNum = append(tctr.normalTxNum, 0)

		tctr.epochID++
	}

	tctr.normalTxNum[epochid] += len(b.InnerShardTxs)
	tctr.szhbft1TxNum[epochid] += r1TxNum
	tctr.szhbft2TxNum[epochid] += r2TxNum

	tctr.totCrossTxNum[epochid] += float64(r1TxNum+r2TxNum) / 2
	tctr.totTxNum[epochid] += float64(r1TxNum+r2TxNum)/2 + float64(len(b.InnerShardTxs))
}

func (tctr *TestCrossTxRate_Szhbft) HandleExtraMessage([]byte) {}

func (tctr *TestCrossTxRate_Szhbft) OutputRecord() (perEpochCTXratio []float64, totCTXratio float64) {
	tctr.writeToCSV()

	// calculate the simple result
	perEpochCTXratio = make([]float64, 0)
	allEpoch_totTxNum := 0.0
	allEpoch_ctxNum := 0.0
	for eid, totTxN := range tctr.totTxNum {
		perEpochCTXratio = append(perEpochCTXratio, tctr.totCrossTxNum[eid]/totTxN)
		allEpoch_totTxNum += totTxN
		allEpoch_ctxNum += tctr.totCrossTxNum[eid]
	}
	perEpochCTXratio = append(perEpochCTXratio, allEpoch_totTxNum)
	perEpochCTXratio = append(perEpochCTXratio, allEpoch_ctxNum)

	return perEpochCTXratio, allEpoch_ctxNum / allEpoch_totTxNum
}

func (tctr *TestCrossTxRate_Szhbft) writeToCSV() {
	fileName := tctr.OutputMetricName()
	measureName := []string{"EpochID", "Total tx # in this epoch", "CTX # in this epoch", "Normal tx # in this epoch", "Szhbft1 tx # in this epoch", "Szhbft2 tx # in this epoch", "CTX ratio of this epoch"}
	measureVals := make([][]string, 0)

	for eid, totTxInE := range tctr.totTxNum {
		csvLine := []string{
			strconv.Itoa(eid),
			strconv.FormatFloat(totTxInE, 'f', '8', 64),
			strconv.FormatFloat(tctr.totCrossTxNum[eid], 'f', '8', 64),
			strconv.Itoa(tctr.normalTxNum[eid]),
			strconv.Itoa(tctr.szhbft1TxNum[eid]),
			strconv.Itoa(tctr.szhbft2TxNum[eid]),
			strconv.FormatFloat(tctr.totCrossTxNum[eid]/totTxInE, 'f', '8', 64),
		}
		measureVals = append(measureVals, csvLine)
	}
	WriteMetricsToCSV(fileName, measureName, measureVals)
}
