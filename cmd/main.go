package main

import (
	"github.com/YKMeIz/alibaba_clusterdata_tool/internal/db"
)

func main() {
	r := "../machine_usage.csv"
	//w := "../machine_ram_missing.csv"
	//convert.Run(r, w)
	//count.MissingByTime(r, w)
	db.Open("../machine_usage.db").Convert(r)
	//timewindow.Find("../machine_ram_usage.csv", "../time_window_after16070.csv", 80)
	//timewindow.Convert("../machine_ram_usage.csv", "../time_window_after16070.csv", 80)
	//stddev.Run(r, "../machine_sd.csv")
}
