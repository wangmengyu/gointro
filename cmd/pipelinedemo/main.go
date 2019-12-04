package main

import (
	"fmt"
	"gointro.com/pipeline"
)

func main() {
	//建立数据源管道
	p1 := pipeline.InMemSort(pipeline.ArraySource(3, 2, 6, 8, 4, 13, 2, 3, 4, 55, 32, 31))
	p2 := pipeline.InMemSort(pipeline.ArraySource(8, 2, 5, 12, 15, 56, 12, 34, 111, 231, 32))
	p := pipeline.Merge(p1, p2)
	for v := range p {
		fmt.Println(v)
	}

}
