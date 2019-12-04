package main

import (
	"fmt"
	"gointro.com/pipeline"
)

func main() {
	//建立数据源管道
	p := pipeline.InMemSort(pipeline.ArraySource(1, 3, 5, 7, 9))
	p2 := pipeline.InMemSort(pipeline.ArraySource(2, 4, 6, 8, 0))
	p3 := pipeline.Merge(p, p2)

	//这里会 等待 管道有数据了就开始循环取出数据做处理,此处range 内部自己实现了等待机制
	for i := range p3 {
		fmt.Println(i)
	}

}
