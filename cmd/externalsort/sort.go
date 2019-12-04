package main

import (
	"bufio"
	"fmt"
	"gointro.com/pipeline"
	"os"
	"strconv"
)

/**
完整外部排序
*/
func main() {

	/*
		p := createPipeline("large.in", 800000000, 4)
		writeToFile(p, "large.out")
		printFile("large.out")
	*/

	p := createNetworkPipeline("large.in", 800000000, 4)
	writeToFile(p, "large.out")
	printFile("large.out")

}

/**
  print data from file
*/
func printFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	p := pipeline.ReaderSource(file, -1)
	i := 0
	for v := range p {
		if i > 100 {
			break
		}
		fmt.Println(v)
		i++
	}

}

/**
write data from pipeline to file
*/
func writeToFile(p <-chan int, filename string) {
	//create a file to output data
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	//create a buf writer
	writer := bufio.NewWriter(file)
	defer writer.Flush()

	//write data into new file
	pipeline.WriteSink(writer, p)

}

/**
  read from file ,put data into channel
*/
func createPipeline(filename string, fileSize, chunkCount int) <-chan int {
	sortResults := make([]<-chan int, 0)
	chunkSize := fileSize / chunkCount
	pipeline.Init()
	for i := 0; i < chunkCount; i++ {
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}
		//set pointer of current start pos
		file.Seek(int64(i*chunkSize), 0)

		//create pipeline for read file
		source := pipeline.ReaderSource(bufio.NewReader(file), chunkSize)
		res := pipeline.InMemSort(source)
		//collect sorted pipeline
		sortResults = append(sortResults, res)
	}

	return pipeline.MergeN(sortResults...)

}

/**
  network version:
  read from file ,put data into channel
*/
func createNetworkPipeline(filename string, fileSize, chunkCount int) <-chan int {

	chunkSize := fileSize / chunkCount
	pipeline.Init()
	sortAddr := make([]string, 0)
	for i := 0; i < chunkCount; i++ {
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}
		//set pointer of current start pos
		file.Seek(int64(i*chunkSize), 0)

		//create pipeline for read file
		source := pipeline.ReaderSource(bufio.NewReader(file), chunkSize)

		//sink data from memory to tcp conn
		addr := ":" + strconv.Itoa(7000+i)
		pipeline.NetworkSink(addr, pipeline.InMemSort(source))
		sortAddr = append(sortAddr, addr)
	}

	//collect data from network conn into sortResults fo merge
	sortResults := make([]<-chan int, 0)
	for _, addr := range sortAddr {
		sortResults = append(sortResults, pipeline.NetworkSource(addr))
	}
	return pipeline.MergeN(sortResults...)

}
