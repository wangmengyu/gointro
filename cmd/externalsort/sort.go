package main

import (
	"bufio"
	"fmt"
	"gointro.com/pipeline"
	"os"
)

/**
完整外部排序
*/
func main() {

	p := createPipeline("small.in", 512, 4)
	writeToFile(p, "small.out")
	printFile("small.out")

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
	for v := range p {
		fmt.Println(v)
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
		fmt.Println("after source:", len(res))
		//collect sorted pipeline
		sortResults = append(sortResults, res)
	}

	return pipeline.MergeN(sortResults...)

}
