package pipeline

import "sort"

/**
  建立 数据源收集器
*/
func ArraySource(a ...int) <-chan int {
	//建立一个接收器
	out := make(chan int)
	go func() {
		for _, v := range a {
			out <- v
		}
		//在协程处理完后，关闭管道
		close(out)
	}()
	return out
}

func InMemSort(in <-chan int) <-chan int {
	//新建一个输出管道
	out := make(chan int)
	//开协程对数据进行处理
	go func() {
		//从管道读取数据到内存
		a := make([]int, 0)
		for i := range in {
			a = append(a, i)
		}
		//排序
		sort.Ints(a)

		//输出给输出管道
		for _, v := range a {
			out <- v
		}
		close(out)

	}()

	//返回管道
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
