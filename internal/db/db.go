package db

import (
	"fmt"
	"github.com/YKMeIz/alibaba_clusterdata_tool/internal"
	"github.com/cheggaaa/pb/v3"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"strings"
)

type DB struct {
	*gorm.DB

	bar *pb.ProgressBar

	data []MachineUsage
}

func Open(path string) *DB {
	db, err := gorm.Open(mysql.New(mysql.Config{
		DSN:                       "gorm:gorm@tcp(sef-bigdata-00:3306)/gorm?charset=utf8&parseTime=True&loc=Local", // data source name
		DefaultStringSize:         256,                                                                             // default size for string fields
		DisableDatetimePrecision:  true,                                                                            // disable datetime precision, which not supported before MySQL 5.6
		DontSupportRenameIndex:    true,                                                                            // drop & create when rename index, rename index not supported before MySQL 5.7, MariaDB
		DontSupportRenameColumn:   true,                                                                            // `change` when rename column, rename column not supported before MySQL 8, MariaDB
		SkipInitializeWithVersion: false,                                                                           // auto configure based on currently MySQL version
	}), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	//db, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
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
	// d.DB.CreateInBatches(d.data, len(d.data))
	d.bar = pb.Full.Start(len(d.data))
	for _, v := range d.data {
		if res := d.DB.Create(&v); res.Error != nil {
			log.Fatalln(res.Error)
		}
		d.bar.Increment()
	}
	d.bar.Finish()
}
