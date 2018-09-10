package main

import (
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"log"
)

var (
	redisClient *redis.Client
)

func main() {
	createRedisClient()
	book := Book{12, "Land of Lisp"}
	err := setBook(book)
	if err != nil {
		log.Fatal("Book set error.")
	}
	fmt.Println("Book set success.")

	book2, err := getBook(12)
	if err != nil {
		log.Fatal("Book get error.")
	}
	fmt.Println("Book get success.")

	err = delBook(book2.Id)
	if err != nil {
		log.Fatal("Book del error.")
	}
	fmt.Println("Book del success.")
}

// Struct
type Book struct {
	Id    int64
	Title string
}

// Operations
func createRedisClient() {
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	pong, err := redisClient.Ping().Result()
	fmt.Println(pong, err)
}

func bookKey(bookId int64) string {
	return fmt.Sprint("book:%d", bookId)
}

func setBook(book Book) error {
	b, err := json.Marshal(book)
	if err != nil {
		return err
	}
	err = redisClient.Set(bookKey(book.Id), b, 0).Err()
	if err != nil {
		return err
	}
	return nil
}

func getBook(id int64) (Book, error) {
	b, err := redisClient.Get(bookKey(id)).Bytes()
	if err != nil {
		return Book{}, err
	}
	var book Book
	if err = json.Unmarshal(b, &book); err != nil {
		return Book{}, err
	}
	return book, nil
}

func delBook(id int64) error {
	err := redisClient.Del(bookKey(id)).Err()
	if err != nil {
		return err
	}
	return nil
}
