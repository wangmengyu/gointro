package main

import (
	"fmt"
)

func main() {

	//创建一个通道，存储字符串类型
	c := make(chan string)
	for i := 0; i < 5000; i++ {
		//开启goroutine, 不断的往通道里塞字符串
		go printHelloWorld(i, c)
	}
	for {
		//不断从通道获取数据并且输出
		msg := <-c
		fmt.Println(msg)
	}
}

func printHelloWorld(i int, c chan string) {
	for {
		c <- fmt.Sprintf("Hello from goroutine:%d", i)
	}

}
