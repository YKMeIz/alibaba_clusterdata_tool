package db

import (
	"fmt"
	"github.com/YKMeIz/alibaba_clusterdata_tool/internal"
	"github.com/cheggaaa/pb/v3"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"strings"
)

type DB struct {
	*gorm.DB

	bar *pb.ProgressBar

	data []MachineUsage
}

func Open(path string) *DB {
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}
	// Migrate the schema
	db.AutoMigrate(&MachineUsage{})

	return &DB{DB: db}
}

func (d *DB) convertEachLine(text string) {
	strs := strings.Split(text, ",")

	d.data = append(d.data, MachineUsage{
		MachineID: strs[0],
		Time:      internal.Stoi(strs[1]),
		CPU:       internal.Stoi(strs[2]),
		Ram:       internal.Stoi(strs[3]),
	})

	d.bar.Increment()
}

func (d *DB) Convert(file string) {
	fmt.Println("initialize ...")
	d.bar = pb.Full.Start(internal.GetLineCount(file))
	fmt.Println("generate database rows ...")
	internal.FileScan(file, d.convertEachLine, false)
	d.bar.Finish()
	fmt.Println("write database ...")
	// panic: runtime error: index out of range [2469348269] with length 1073741824
	d.DB.CreateInBatches(d.data, len(d.data))
}