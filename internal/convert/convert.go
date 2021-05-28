package convert

import (
	"fmt"
	"github.com/YKMeIz/alibaba_clusterdata_tool/internal"
	"github.com/cheggaaa/pb/v3"
	"log"
	"os"
	"strconv"
	"strings"
)

type machineUsageData struct {
	bar *pb.ProgressBar

	timeline      []int
	timeLastValue int
	machines      map[string]map[int]int
}

func (mud *machineUsageData) convert(text string) {
	strs := strings.Split(text, ",")

	// time
	t := internal.Stoi(strs[1])
	// ram
	r := internal.Stoi(strs[3])
	// machine
	m, ok := mud.machines[strs[0]]
	if !ok {
		m = make(map[int]int)
	}
	m[t] = r
	mud.machines[strs[0]] = m

	if t > mud.timeLastValue {
		mud.timeLastValue = t
	}

	mud.bar.Increment()
}

func (mud *machineUsageData) writeData(file string) {
	mud.bar.Finish()
	mud.bar = &pb.ProgressBar{}

	dataLength := len(mud.machines)
	mud.bar = pb.Full.Start(len(mud.machines))

	f, err := os.Create(file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	fmt.Println("start writing data to:", file)

	for i := 0; i <= mud.timeLastValue; i += 10 {
		mud.timeline = append(mud.timeline, i)
	}

	internal.WriteEntity(f, "time", mud.timeline)

	id := 0
	for {
		name := "m_" + strconv.Itoa(id)
		m, ok := mud.machines[name]
		if !ok {
			id++
			continue
		}

		var data []int

		for i := 0; i <= mud.timeLastValue; i += 10 {
			v, ok := m[i]
			if !ok {
				data = append(data, -1)
				continue
			}
			data = append(data, v)
		}

		internal.WriteEntity(f, name, data)
		mud.bar.Increment()
		id++
		dataLength--
		if dataLength <= 0 {
			break
		}
	}
}

func Run(src, dst string) {
	d := machineUsageData{
		machines: make(map[string]map[int]int),
	}

	fmt.Println("initialize ...")
	d.bar = pb.Full.Start(internal.GetLineCount(src))

	internal.FileScan(src, d.convert, false)
	d.writeData(dst)
	d.bar.Finish()
}
