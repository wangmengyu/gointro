package pipeline

import "sort"

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
  get data from channel,
  sort data in memory,
  put into out channel
*/
func InMemSort(in <-chan int) <-chan int {
	//create new out channel
	out := make(chan int)
	//begin a goroutine to sort data
	go func() {
		//get data from channel,put into memory
		a := make([]int, 0)
		for i := range in {
			a = append(a, i)
		}
		//sort
		sort.Ints(a)

		//range data from memory and put into out channel
		for _, v := range a {
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
	out := make(chan int)
	//start a goroutine
	go func() {

		v1, ok1 := <-in1
		v2, ok2 := <-in2
		for {
			if !ok1 && !ok2 {
				//no one has value
				break
			}

			if !ok2 {
				//only ok1 has value
				out <- v1
				v1, ok1 = <-in1

			} else if !ok1 {
				//only ok2 has value
				out <- v2
				v2, ok2 = <-in2

			} else {
				//both has value
				if v1 < v2 {
					out <- v1
					v1, ok1 = <-in1
					out <- v2
					v2, ok2 = <-in2
				} else {
					out <- v2
					v2, ok2 = <-in2
					out <- v1
					v1, ok1 = <-in1
				}
			}
		}
		close(out)
	}()

	return out
}
