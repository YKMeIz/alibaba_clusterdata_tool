package stddev

import (
	"fmt"
	"github.com/YKMeIz/alibaba_clusterdata_tool/internal"
	"github.com/YKMeIz/regression"
	"github.com/cheggaaa/pb/v3"
	"log"
	"os"
	"strings"
)

type machineUsageData struct {
	bar *pb.ProgressBar

	machines map[string][]int
}

func (mud *machineUsageData) convert(text string) {
	strs := strings.Split(text, ",")

	mud.machines[strs[0]] = append(mud.machines[strs[0]], internal.Stoi(strs[3]))

	mud.bar.Increment()
}

func (mud *machineUsageData) writeData(file string) {
	mud.bar.Finish()
	mud.bar = &pb.ProgressBar{}

	mud.bar = pb.Full.Start(len(mud.machines))

	f, err := os.Create(file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	fmt.Println("start writing data to:", file)

	for k, v := range mud.machines {
		sd := getStdDev(v)
		// filter memory usage record below 50%
		if sd == -1 {
			mud.bar.Increment()
			continue
		}

		_, err := f.WriteString(k + "," + fmt.Sprintf("%f", sd) + "\n")
		if err != nil {
			log.Fatal(err)
		}
		err = f.Sync()
		if err != nil {
			log.Fatal(err)
		}

		mud.bar.Increment()
	}
}

func getStdDev(data []int) float64 {
	ds := regression.DataSet{}
	for _, v := range data {
		// filter memory usage record below 50%
		if v < 50 {
			return -1
		}
		ds.Add(regression.DataPoint{
			Y: float64(v),
		})
	}
	return ds.SD()
}

func Run(src, dst string) {
	d := machineUsageData{
		machines: make(map[string][]int),
	}

	fmt.Println("initialize ...")
	d.bar = pb.Full.Start(internal.GetLineCount(src))

	internal.FileScan(src, d.convert, false)
	d.writeData(dst)
	d.bar.Finish()
}
