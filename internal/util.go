package internal

import (
	"bytes"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

func Stoi(s string) int {
	t, err := strconv.Atoi(s)
	if err != nil {
		log.Fatal(err)
	}
	return t
}

func FillNaN(s []int) []int {
	b := s[0]
	a := (s[len(s)-1] - b) / (len(s) - 1)
	// y = a * x + b
	for i := 1; i < len(s)-1; i++ {
		s[i] = a*i + b
	}
	return s
}

func LineCounter(r io.Reader) (int, error) {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

func WriteEntity(file *os.File, title string, values []int) {
	var strs []string
	for i := 0; i < len(values); i++ {
		strs = append(strs, strconv.Itoa(values[i]))
	}
	_, err := file.WriteString(strings.Join(append([]string{title}, strs...), ",") + "\n")
	if err != nil {
		log.Fatal(err)
	}
	err = file.Sync()
	if err != nil {
		log.Fatal(err)
	}
}
