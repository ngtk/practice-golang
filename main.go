package main

import (
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"time"
)

var (
	redisClient *redis.Client
)

func main() {
	createRedisClient()
	book := Book{12, "Land of Lisp", time.Now()}
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

	_ = setBook(Book{13, "Land of Lisp", time.Now()})
	_ = setBook(Book{14, "Land of Lisp", time.Now()})
	_ = setBook(Book{15, "Land of Lisp", time.Now()})

	fmt.Println(getBooks([]int64{13, 14, 16}))
	zaddBook(book)
	fmt.Println(zrevrangeBook())
}

// Struct
type Book struct {
	Id        int64
	Title     string
	UpdatedAt time.Time
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

func getBooks(ids []int64) ([]Book, error) {
	keys := []string{}
	for _, id := range ids {
		keys = append(keys, bookKey(id))
	}

	rows, err := redisClient.MGet(keys...).Result()
	if err != nil {
		return []Book{}, err
	}

	var books []Book
	for _, row := range rows {
		var book Book
		b, ok := row.(string)
		if ok {
			if err = json.Unmarshal([]byte(b), &book); err != nil {
				return []Book{}, err
			}
		} else {
			book = Book{-1, "", time.Now()}
		}
		books = append(books, book)
	}
	return books, nil
}

func bookSortedSetUpdatedAtKey() string {
	return "book:sorted_set:updated_at"
}

func zaddBook(book Book) error {
	b, err := json.Marshal(book)
	if err != nil {
		return err
	}
	err = redisClient.ZAdd(bookSortedSetUpdatedAtKey(), redis.Z{
		Score:  float64(book.UpdatedAt.Unix()),
		Member: b,
	}).Err()
	return err
}

func zrevrangeBook() ([]Book, error) {
	rows, err := redisClient.ZRevRange(bookSortedSetUpdatedAtKey(), 0, -1).Result()
	if err != nil {
		return []Book{}, err
	}

	var books []Book
	for _, row := range rows {
		var book Book
		if err = json.Unmarshal([]byte(row), &book); err != nil {
			return []Book{}, err
		}
		books = append(books, book)
	}
	return books, nil
}
