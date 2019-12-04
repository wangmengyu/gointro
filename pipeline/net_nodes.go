package pipeline

import (
	"bufio"
	"net"
)

/**
   create tcp conn,
  write data from a channel to conn
*/
func NetworkSink(addr string, in <-chan int) {

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	//defer listener.Close()
	go func() {
		defer listener.Close()
		//create tcp conn
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		//close conn
		defer conn.Close()

		//create buff writer
		writer := bufio.NewWriter(conn)
		defer writer.Flush()

		//write data from channel into tcp conn
		WriteSink(writer, in)

	}()

}

/**
 read data from tcp conn
out put into a channel
*/
func NetworkSource(addr string) <-chan int {
	out := make(chan int)
	go func() {
		//create conn for tcp link
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			panic(err)
		}
		defer conn.Close()

		//read data from tcp conn, and put data into out channel
		r := ReaderSource(conn, -1)
		for v := range r {
			out <- v
		}
		close(out)
	}()
	return out

}
