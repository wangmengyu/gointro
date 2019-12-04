package main

import (
	"bufio"
	"fmt"
	"gointro.com/pipeline"
	"os"
)

func main() {

	//create a file
	const filename = "large.in"
	const n = 100000000
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	p := pipeline.RandomSource(n)
	//write random number into file, use bufio.NewWriter to speed up
	writer := bufio.NewWriter(file)
	pipeline.WriteSink(writer, p)
	writer.Flush() // must use flush , when use bufio.newWriter

	//read file, out put to channel
	file, err = os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	//read from file, use bufio.NewReader to speed up reader
	p = pipeline.ReaderSource(bufio.NewReader(file))

	i := 0
	for v := range p {
		if i > 100 {
			break
		}
		fmt.Println(v)
		i++
	}

}

func mergeDemo() {
	//建立数据源管道
	p1 := pipeline.InMemSort(pipeline.ArraySource(3, 2, 6, 8, 4, 13, 2, 3, 4, 55, 32, 31))
	p2 := pipeline.InMemSort(pipeline.ArraySource(8, 2, 5, 12, 15, 56, 12, 34, 111, 231, 32))
	p := pipeline.Merge(p1, p2)
	for v := range p {
		fmt.Println(v)
	}

}
