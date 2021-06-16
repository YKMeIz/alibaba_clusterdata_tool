package timewindow

import (
	"github.com/YKMeIz/alibaba_clusterdata_tool/internal"
	"github.com/YKMeIz/regression"
	"log"
	"os"
	"strconv"
	"strings"
)

func (tw *timeWindow) fillFilterMachinesData(text string) {
	tw.wg.Add(1)
	go func() {
		strs := strings.Split(text, ",")

		if strs[0] == "time" {
			limit := 60
			arr := strs[lowerBound+1:]
			var timeline []int
			for i := 0; i < len(arr); i += limit {
				batch := arr[i:min(i+limit, len(arr))]
				timeline = append(timeline, internal.Stoi(batch[0]))
			}
			tw.dfsTimeline = timeline
			tw.wg.Done()
			return

		}

		if _, ok := tw.machines[strs[0]]; !ok {
			tw.wg.Done()
			return
		}

		log.Println("process", strs[0], "...")

		//c := missingCount(append(strs[1:14161],strs[16038:]...))
		tw.lock.Lock()
		tw.dfs[strs[0]] = convertTo60secondTineWindow(strs[lowerBound+1:])
		tw.lock.Unlock()
		tw.wg.Done()
	}()
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func convertTo60secondTineWindow(arr[]string) []regression.DataFeature {
	var (
		feats []regression.DataFeature
	)

	limit := 60

	for i := 0; i < len(arr); i += limit {
		batch := arr[i:min(i+limit, len(arr))]
		feats = append(feats, regression.Feature(internal.Stof64(batch)))
	}

	return feats
}



func Convert(src, dst string, filter int) {
	tw := timeWindow{
		filter:   filter,
		machines: make(map[string][]int),
		dfs: make(map[string][]regression.DataFeature),
	}
	internal.FileScan(src, tw.loopThrough, true)
	tw.wg.Wait()

	filteredMachines  := make(map[string][]int)

	for k, v :=range tw.machines {
		//internal.WriteEntity(f, k, v)
		if v[1] < 60 {
			filteredMachines[k] = []int{}
		}
	}

	tw.machines = filteredMachines

	internal.FileScan(src, tw.fillFilterMachinesData, true)
	tw.wg.Wait()

	dst = strings.TrimSuffix(dst, ".csv")
	minCSV := dst + "_min.csv"
	maxCSV := dst + "_max.csv"
	meanCSV := dst + "_mean.csv"
	sdCSV := dst + "_sd.csv"

	minData := make(map[string][]float64)
	maxData := make(map[string][]float64)
	meanData := make(map[string][]float64)
	sdData := make(map[string][]float64)


	for k, v := range tw.dfs {
		var vMin, vMax, vMean, vSD []float64

		for _, f := range v {
			vMin = append(vMin, f.Min)
			vMax = append(vMax, f.Max)
			vMean = append(vMean, f.Mean)
			vSD = append(vSD, f.SD)
				}

		minData[k] = vMin
		maxData[k] = vMax
		meanData[k] = vMean
		sdData[k] = vSD
	}

	tw.writeCSV(minCSV, minData)
	tw.writeCSV(maxCSV, maxData)
	tw.writeCSV(meanCSV, meanData)
	tw.writeCSV(sdCSV, sdData)

}

func (tw *timeWindow) writeCSV(file string, data map[string][]float64) {
	f, err := os.Create(file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	internal.WriteEntity(f, "time", tw.dfsTimeline)

	for k, v := range data {
		var strs []string
		for i := 0; i < len(v); i++ {
			strs = append(strs, strconv.FormatFloat(v[i], 'f', -1, 64))
		}
		_, err := f.WriteString(strings.Join(append([]string{k}, strs...), ",") + "\n")
		if err != nil {
			log.Fatal(err)
		}
		err = f.Sync()
		if err != nil {
			log.Fatal(err)
		}
	}
}
