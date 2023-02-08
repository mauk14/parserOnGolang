package main

import (
	"context"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	client, err := clientMongoDb(os.Getenv("MONGODB_URI"))
	if err != nil {
		log.Fatal(err)
		return
	}
	db := client.Database("Library")

	for i := 1; i <= 5; i++ {
		URL := fmt.Sprintf("https://openlibrary.org/trending/daily?page=%v", i)
		res, err := http.Get(URL)
		if err != nil {
			log.Fatal(err)
			return
		}
		defer res.Body.Close()

		if res.StatusCode != 200 {
			log.Fatalf("status code error: %d %s", res.StatusCode, res.Status)
			return
		}

		doc, err := goquery.NewDocumentFromReader(res.Body)
		if err != nil {
			log.Fatal(err)
			return
		}
		linkAll := doc.Find(".list-books").Find(".searchResultItem")
		linkAll.Each(func(j int, s *goquery.Selection) {
			var wg sync.WaitGroup
			wg.Add(5)
			var title string
			var id int64
			var sizeInt int
			var yearInt int
			var author string

			go func() {
				defer wg.Done()
				title = s.Find("h3").Text()
				title = strings.TrimSpace(title)
			}()

			go func() {
				defer wg.Done()
				author = s.Find(".bookauthor").Find("a").Text()
				author = strings.TrimSpace(author)
			}()

			go func() {
				defer wg.Done()
				year := s.Find(".publishedYear").Text()
				year = strings.TrimSpace(year)
				year = strings.Replace(year, "First published in ", "", 1)
				yearInt, err = strconv.Atoi(year)
				if err != nil {
					log.Fatal(err)
					return
				}
			}()

			go func() {
				defer wg.Done()
				id, err = GetLastId(db)
				if err != nil {
					log.Fatal(err)
					return
				}
				id++
			}()

			go func() {
				defer wg.Done()
				size := strings.TrimSpace(s.Find(".resultPublisher").Find("a").Text())
				size = strings.Replace(size, " edition", "", 1)
				size = strings.Replace(size, "s", "", 1)
				sizeInt, err = strconv.Atoi(size)
				if err != nil {
					log.Fatal(err)
					return
				}
			}()

			wg.Wait()

			coll := db.Collection("books")

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			book := Book{
				ID:        id,
				Title:     title,
				Author:    author,
				Year:      int32(yearInt),
				Size:      sizeInt,
				Genres:    []string{"fantasy"},
				Version:   uuid.New(),
				CreatedAt: time.Now(),
			}

			_, err = coll.InsertOne(ctx, book)
		})
	}
}

func clientMongoDb(uri string) (*mongo.Client, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	defer cancel()

	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return nil, err
	}

	return client, nil
}

func GetLastId(database *mongo.Database) (int64, error) {
	coll := database.Collection("books")
	filter := bson.D{}
	opts := options.FindOne().SetSort(bson.D{{"id", -1}})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var result Book
	err := coll.FindOne(ctx, filter, opts).Decode(&result)
	if err != nil {
		return 0, err
	}
	return result.ID, nil

}

type Book struct {
	ID        int64     `json:"id"`
	CreatedAt time.Time `json:"-"`
	Title     string    `json:"title"`
	Author    string    `json:"author"`
	Year      int32     `json:"year,omitempty"`
	Size      int       `json:"-"`
	Genres    []string  `json:"genres,omitempty"`
	Version   uuid.UUID `json:"version"`
}
