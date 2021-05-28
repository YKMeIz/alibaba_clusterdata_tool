package main

import (
	"github.com/YKMeIz/alibaba_clusterdata_tool/internal/count"
)

func main() {
	r := "../machine_usage.csv"
	w := "../machine_ram_missing.csv"
	//convert.Run(r, w)
	count.MissingByTime(r, w)

}
