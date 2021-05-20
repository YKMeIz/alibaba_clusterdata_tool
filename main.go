package main

import (
	"bufio"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
)

func normalize() {
	file, err := os.Open("../machine_usage.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	f, err := os.Create("../machine_usage_1.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var (
		time     map[int]struct{}
		machines map[string]map[int]int
	)

	time = make(map[int]struct{})
	machines = make(map[string]map[int]int)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		strs := strings.Split(scanner.Text(), ",")

		// record timeline info
		time[atoi(strs[1])] = struct{}{}

		// record machines info
		m, ok := machines[strs[0]]
		if !ok {
			m = make(map[int]int)
		}
		m[atoi(strs[1])] = atoi(strs[3])
		machines[strs[0]] = m
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	var (
		timeArray     []int
		machinesOrder []string
	)

	for k, _ := range time {
		timeArray = append(timeArray, k)
	}

	sort.Ints(timeArray)
	_, err = f.WriteString("time")
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < len(timeArray); i++ {
		_, err = f.WriteString("," + strconv.Itoa(timeArray[i]))
		if err != nil {
			log.Fatal(err)
		}
	}
	_, err = f.WriteString("\n")
	if err != nil {
		log.Fatal(err)
	}

	err = f.Sync()
	if err != nil {
		log.Fatal(err)
	}

	for k, _ := range machines {
		machinesOrder = append(machinesOrder, k)
	}
	sort.Slice(machinesOrder, func(i, j int) bool {
		a := atoi(machinesOrder[i][2:])
		b := atoi(machinesOrder[j][2:])
		return a < b
	})

	for i := 0; i < len(machinesOrder); i++ {
		log.Println("write", machinesOrder[i], "...")
		// Write machine id
		_, err = f.WriteString(machinesOrder[i])
		if err != nil {
			log.Fatal(err)
		}

		var missingRecords []int

		for timeItem := 0; timeItem < len(timeArray); timeItem++ {
			v, ok := machines[machinesOrder[i]][timeItem]
			// Write empty value
			if !ok && (len(missingRecords) > 0) {
				//_, err = f.WriteString(",")
				//if err != nil {
				//	log.Fatal(err)
				//}
				missingRecords = append(missingRecords, -1)
				continue
			}
			missingRecords = append(missingRecords, v)
			if len(missingRecords) > 2 {
				fillNaN(missingRecords)
				for i := 0; i < len(missingRecords); i++ {
					// Write value
					_, err = f.WriteString("," + strconv.Itoa(missingRecords[i]))
					if err != nil {
						log.Fatal(err)
					}
				}
			}
			// Write value
			_, err = f.WriteString("," + strconv.Itoa(v))
			if err != nil {
				log.Fatal(err)
			}
		}
		// new line
		_, err = f.WriteString("\n")
		if err != nil {
			log.Fatal(err)
		}

		err = f.Sync()
		if err != nil {
			log.Fatal(err)
		}
	}

	err = f.Sync()
	if err != nil {
		log.Fatal(err)
	}

}

func atoi(s string) int {
	t, err := strconv.Atoi(s)
	if err != nil {
		log.Fatal(err)
	}
	return t
}

func fillNaN(s []int) []int {
	b := s[0]
	a := (s[len(s)-1] - b) / (len(s)-1)
	// y = a * x + b
	for i := 1; i < len(s) - 1; i++ {
		s[i] = a * i + b
	}
	return s
}

type notimeRamData struct {
	time, ram int
}

func removeTimeFrame() {
	file, err := os.Open("../machine_usage.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	f, err := os.Create("../machine_usage_notime.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	machines := make(map[string][]notimeRamData)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		strs := strings.Split(scanner.Text(), ",")

		// record machines info
		m, _ := machines[strs[0]]
		m = append(m, notimeRamData{
			time: atoi(strs[1]),
			ram:  atoi(strs[3]),
		})
		machines[strs[0]] = m
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	var (
		machinesOrder []string
	)

	for k, _ := range machines {
		machinesOrder = append(machinesOrder, k)
	}
	sort.Slice(machinesOrder, func(i, j int) bool {
		a := atoi(machinesOrder[i][2:])
		b := atoi(machinesOrder[j][2:])
		return a < b
	})

	for i := 0; i < len(machinesOrder); i++ {
		log.Println("write", machinesOrder[i], "...")
		// Write machine id
		_, err = f.WriteString(machinesOrder[i])
		if err != nil {
			log.Fatal(err)
		}

		data := machines[machinesOrder[i]]
		sort.Slice(data, func(i, j int) bool {
			return data[i].time < data[i].ram
		})

		for idx := 0; idx < len(data); idx++ {
			// Write value
			_, err = f.WriteString("," + strconv.Itoa(data[idx].ram))
			if err != nil {
				log.Fatal(err)
			}
		}

		// new line
		_, err = f.WriteString("\n")
		if err != nil {
			log.Fatal(err)
		}

		err = f.Sync()
		if err != nil {
			log.Fatal(err)
		}
	}

	err = f.Sync()
	if err != nil {
		log.Fatal(err)
	}

}

func main() {
	removeTimeFrame()
}