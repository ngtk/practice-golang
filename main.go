package main

import (
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
)

var (
	redisClient *redis.Client
)

func main() {
	createRedisClient()
	book := map[string]interface{}{
		"id":    12,
		"title": "Land of Lisp",
	}
	setBook(book)
}

func createRedisClient() {
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	pong, err := redisClient.Ping().Result()
	fmt.Println(pong, err)
}

func bookKey(bookId int) string {
	return fmt.Sprint("book:%d", bookId)
}

func setBook(book map[string]interface{}) {
	bookId, ok := book["id"].(int)
	if !ok {
		return
	}

	err := redisClient.Set(bookKey(bookId), marshal(book), 0).Err()
	if err != nil {
		panic(err)
	}
	fmt.Print("Book Saved.")
}

// Helpers
func marshal(object interface{}) []byte {
	b, _ := json.Marshal(object)
	return b
}
