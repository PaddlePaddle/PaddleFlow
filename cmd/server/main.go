package main

import (
	_ "go.uber.org/automaxprocs"
	"log"
	"paddleflow/cmd/server/app"
)

func main() {
	server := app.Server{}
	server.Init()
	err := server.Run()
	if err != nil {
		log.Fatalf("server run failed. error:%s", err.Error())
	}
}
