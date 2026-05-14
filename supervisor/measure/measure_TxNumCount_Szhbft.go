package measure

import (
	"blockEmulator/message"
	"strconv"
)

// to test Tx number
type TestTxNumCount_Szhbft struct {
	epochID int
	txNum   []float64

	normalTxNum []int
	szhbft1TxNum []int
	szhbft2TxNum []int
}

func NewTestTxNumCount_Szhbft() *TestTxNumCount_Szhbft {
	return &TestTxNumCount_Szhbft{
		epochID: -1,
		txNum:   make([]float64, 0),

		normalTxNum: make([]int, 0),
		szhbft1TxNum: make([]int, 0),
		szhbft2TxNum: make([]int, 0),
	}
}

func (ttnc *TestTxNumCount_Szhbft) OutputMetricName() string {
	return "Tx_number"
}

func (ttnc *TestTxNumCount_Szhbft) UpdateMeasureRecord(b *message.BlockInfoMsg) {
	if b.BlockBodyLength == 0 { // empty block
		return
	}
	epochid := b.Epoch
	r1TxNum := len(b.Relay1Txs)
	r2TxNum := len(b.Relay2Txs)
	// extend
	for ttnc.epochID < epochid {
		ttnc.txNum = append(ttnc.txNum, 0)
		ttnc.szhbft1TxNum = append(ttnc.szhbft1TxNum, 0)
		ttnc.szhbft2TxNum = append(ttnc.szhbft2TxNum, 0)
		ttnc.normalTxNum = append(ttnc.normalTxNum, 0)

		ttnc.epochID++
	}

	ttnc.normalTxNum[epochid] += len(b.InnerShardTxs)
	ttnc.szhbft1TxNum[epochid] += r1TxNum
	ttnc.szhbft2TxNum[epochid] += r2TxNum
	ttnc.txNum[epochid] += float64(len(b.InnerShardTxs)) + float64(len(b.Relay1Txs)+len(b.Relay2Txs))/2
}

func (ttnc *TestTxNumCount_Szhbft) HandleExtraMessage([]byte) {}

func (ttnc *TestTxNumCount_Szhbft) OutputRecord() (perEpochCTXs []float64, totTxNum float64) {
	ttnc.writeToCSV()

	// calculate the simple result
	perEpochCTXs = make([]float64, 0)
	totTxNum = 0.0
	for _, tn := range ttnc.txNum {
		perEpochCTXs = append(perEpochCTXs, tn)
		totTxNum += tn
	}
	return perEpochCTXs, totTxNum
}

func (ttnc *TestTxNumCount_Szhbft) writeToCSV() {
	fileName := ttnc.OutputMetricName()
	measureName := []string{"EpochID", "Total tx # in this epoch", "Normal tx # in this epoch", "Szhbft1 tx # in this epoch", "Szhbft2 tx # in this epoch"}
	measureVals := make([][]string, 0)

	for eid, totTxInE := range ttnc.txNum {
		csvLine := []string{
			strconv.Itoa(eid),
			strconv.FormatFloat(totTxInE, 'f', '8', 64),
			strconv.Itoa(ttnc.normalTxNum[eid]),
			strconv.Itoa(ttnc.szhbft1TxNum[eid]),
			strconv.Itoa(ttnc.szhbft2TxNum[eid]),
		}
		measureVals = append(measureVals, csvLine)
	}
	WriteMetricsToCSV(fileName, measureName, measureVals)
}
