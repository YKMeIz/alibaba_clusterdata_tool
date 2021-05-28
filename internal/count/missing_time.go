package count

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

	timeline      map[int]map[string]string
	timeLastValue int
	machines      map[string]string
}

func (mud *machineUsageData) convert(text string) {
	strs := strings.Split(text, ",")

	// time
	t := internal.Stoi(strs[1])
	// timeline
	if strs[3] != "" {
		v, ok := mud.timeline[t]
		if !ok {
			v = make(map[string]string)
		}
		v[strs[0]] = ""
		mud.timeline[t] = v
	}
	// machine
	mud.machines[strs[0]] = ""

	if t > mud.timeLastValue {
		mud.timeLastValue = t
	}

	mud.bar.Increment()
}

func (mud *machineUsageData) writeData(file string) {
	mud.bar.Finish()
	mud.bar = &pb.ProgressBar{}

	machinesCount := len(mud.machines)
	mud.bar = pb.Full.Start(mud.timeLastValue/10 + 1)

	f, err := os.Create(file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	fmt.Println("start writing data to:", file)

	for i := 0; i <= mud.timeLastValue; i += 10 {
		v, ok := mud.timeline[i]
		if !ok {
			internal.WriteEntity(f, strconv.Itoa(i), []int{machinesCount})
		} else {
			internal.WriteEntity(f, strconv.Itoa(i), []int{machinesCount - len(v)})
		}
		mud.bar.Increment()
	}

}

func MissingByTime(src, dst string) {
	d := machineUsageData{
		timeline: make(map[int]map[string]string),
		machines: make(map[string]string),
	}

	fmt.Println("initialize ...")
	d.bar = pb.Full.Start(internal.GetLineCount(src))

	internal.FileScan(src, d.convert, false)
	d.writeData(dst)
	d.bar.Finish()
}
