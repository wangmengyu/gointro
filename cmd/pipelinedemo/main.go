package main

import (
	"fmt"
	"gointro.com/pipeline"
)

func main() {
	//建立数据源管道
	p := pipeline.Merge(pipeline.InMemSort(pipeline.ArraySource(3, 2, 6, 7, 4)),
		pipeline.InMemSort(pipeline.ArraySource(7, 4, 0, 3, 2, 13, 8)))
	//这里会 等待 管道有数据了就开始循环取出数据做处理,此处range 内部自己实现了等待机制
	for i := range p {
		fmt.Println(i)
	}

}
