package timewindow

import (
	"github.com/YKMeIz/alibaba_clusterdata_tool/internal"
	"github.com/YKMeIz/regression"
	"log"
	"os"
	"strings"
	"sync"
)

const lowerBound = 16070

type timeWindow struct {
	wg sync.WaitGroup

	filter int
	lock sync.Mutex
	// machine_id, position, missing_count
	machines map[string][]int

	dfs map[string][]regression.DataFeature
	dfsTimeline []int
}

func (tw *timeWindow) loopThrough(text string) {
	tw.wg.Add(1)
	go func() {
		strs := strings.Split(text, ",")
		if strs[0] == "time" {
			tw.wg.Done()
			return
		}

		log.Println("process", strs[0], "...")

		mean := 0
		meanCount := 0
		for _, v := range strs[1:] {
			if v != "-1" {
				mean += internal.Stoi(v)
				meanCount++
			}
		}
		mean = mean / meanCount
		if mean < tw.filter {
			tw.wg.Done()
			return
		}

		//c := missingCount(append(strs[1:14161],strs[16038:]...))
		c := missingCount(strs[lowerBound:])
		tw.lock.Lock()
		tw.machines[strs[0]] = c
		tw.lock.Unlock()
		tw.wg.Done()
	}()
}

func missingCount(data []string) []int {
	var (
		m []string
		mCount int
		position int
	)

	for i := 0; i < len(data); i++ {
		if data[i] == "-1" {
			m = append(m, "-1")
			continue
		}
		if len(m) == 0 {
			continue
		}
		if mCount < len(m) {
			mCount = len(m)
			position = i - len(m)
		}
		m = []string{}
	}

	return []int{position, mCount}
}

func Find(src, dst string, filter int) {
	tw := timeWindow{
		filter:   filter,
		machines: make(map[string][]int),
	}
	internal.FileScan(src, tw.loopThrough, true)
	tw.wg.Wait()

	f, err := os.Create(dst)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	for k, v :=range tw.machines {
		internal.WriteEntity(f, k, v)
	}
}
