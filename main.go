package main

import (
	"log"

	"blevehub/hub"
)

func main() {
	myhub, err := hub.NewHub("./indexes", "testhub", 5)
	if err != nil {
		log.Println(err)
	}
	myhub.IndexBatch(nil)
}
