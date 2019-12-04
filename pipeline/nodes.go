package pipeline

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"time"
)

/**
  get data source channel
*/
func ArraySource(a ...int) <-chan int {
	//make a new out channel
	out := make(chan int)
	go func() {
		for _, v := range a {
			out <- v
		}
		close(out)
	}()
	return out
}

/**
read data from io.Reader
*/
func ReaderSource(reader io.Reader, chunkSize int) <-chan int {
	out := make(chan int, 1024)
	go func() {
		buffer := make([]byte, 8)
		bytesRead := 0
		for {
			n, err := reader.Read(buffer)
			bytesRead += n
			if n > 0 {
				//convert byte to int
				v := int(binary.BigEndian.Uint64(buffer))
				out <- v
			}
			if err != nil || (chunkSize != -1 && bytesRead > chunkSize) {
				//over chunkSize or already done
				break
			}
		}

		close(out)
	}()
	return out

}

/**
  write data from in channel to buffer
*/
func WriteSink(writer io.Writer, in <-chan int) {
	for v := range in {
		buffer := make([]byte, 8)
		binary.BigEndian.PutUint64(buffer, uint64(v))
		writer.Write(buffer)
	}

}

/**
  generate random number into out channel
*/
func RandomSource(count int) <-chan int {
	out := make(chan int)
	go func() {
		for i := 0; i < count; i++ {
			out <- rand.Int()
		}
		close(out)
	}()
	return out
}

var startTime time.Time

func Init() {
	startTime = time.Now()
}

/**
  get data from channel,
  sort data in memory,
  put into out channel
*/
func InMemSort(in <-chan int) <-chan int {
	//create new out channel
	out := make(chan int, 1024)
	//begin a goroutine to sort data
	go func() {
		//Read into memory
		a := make([]int, 0)
		for i := range in {
			a = append(a, i)
		}
		//log time for read into memo
		fmt.Println("Read done:", time.Now().Sub(startTime))
		//sort
		sort.Ints(a)
		fmt.Println("InMemSort done:", time.Now().Sub(startTime))

		//range data from memory and put into out channel
		for _, v := range a {
			//fmt.Println(v)
			out <- v
		}
		close(out)

	}()
	return out

}

/**
  merge data from two channel, push data to out channel
*/
func Merge(in1, in2 <-chan int) <-chan int {
	out := make(chan int, 1024)
	//start a goroutine
	go func() {
		v1, ok1 := <-in1
		v2, ok2 := <-in2
		for {
			if !ok1 && !ok2 {
				//fmt.Println("no data")
				break
			}
			// get v1 or v2 from two channel, only one
			if !ok2 { // only in1 has value
				out <- v1
				v1, ok1 = <-in1
			} else if !ok1 { //only in2 has value
				out <- v2
				v2, ok2 = <-in2
			} else { //both in1 and in2 has value . select smaller one to out put
				if v1 < v2 {
					out <- v1
					v1, ok1 = <-in1
				} else {
					out <- v2
					v2, ok2 = <-in2
				}
			}

		}
		close(out)
		fmt.Println("Merge done:", time.Now().Sub(startTime))
	}()

	return out
}

// merge multiple channel into one channel
func MergeN(inputs ...<-chan int) <-chan int {

	if len(inputs) == 1 {
		return inputs[0]
	}
	mid := len(inputs) / 2

	return Merge(MergeN(inputs[:mid]...), MergeN(inputs[mid:]...))
}
