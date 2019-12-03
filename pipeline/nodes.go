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
