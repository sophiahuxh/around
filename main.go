package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"reflect"
	"strconv"

	"github.com/auth0/go-jwt-middleware"
	jwt "github.com/dgrijalva/jwt-go"
	"github.com/gorilla/mux"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/storage"
	"github.com/pborman/uuid"
	"golang.org/x/net/context"
	elastic "gopkg.in/olivere/elastic.v3"
)

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Post struct {
	User     string   `json:"user"`
	Message  string   `json:"message"`
	Location Location `json:"location"`
	Url      string   `json:"url"`
}

const (
	DISTANCE    = "200km"
	INDEX       = "around"
	TYPE        = "post"
	BUCKET_NAME = "post-images-around201805"
	PROJECT_ID  = "around201805"
	BT_INSTANCE = "around201805-post"
	ES_URL      = "http://35.202.51.192:9200"
)

var mySigningKey = []byte("unknown")

func main() {
	// Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(INDEX).Do()
	if err != nil {
		panic(err)
	}
	if !exists {
		// Create a new index.
		mapping := `{
				"mappings":{
					"post":{
						"properties":{
							"location":{
								"type":"geo_point"
							}
						}
					}
				}
			}`
		_, err := client.CreateIndex(INDEX).Body(mapping).Do()
		if err != nil {
			// Handle error
			panic(err)
		}
	}

	fmt.Println("started-service")

	// r := mux.NewRouter()

	// var jwtMiddleware = jwtmiddleware.New(jwtmiddleware.Options{
	// 	ValidationKeyGetter: func(token *jwt.Token) (interface{}, error) {
	// 		return mySigningKey, nil
	// 	},
	// 	SigningMethod: jwt.SigningMethodHS256,
	// })

	r := mux.NewRouter()

	var jwtMiddleware = jwtmiddleware.New(jwtmiddleware.Options{
		ValidationKeyGetter: func(token *jwt.Token) (interface{}, error) {
			return mySigningKey, nil
		},
		SigningMethod: jwt.SigningMethodHS256,
	})

	r.Handle("/post", jwtMiddleware.Handler(http.HandlerFunc(handlerPost))).Methods("POST")
	r.Handle("/search", jwtMiddleware.Handler(http.HandlerFunc(handlerSearch))).Methods("GET")
	r.Handle("/login", http.HandlerFunc(loginHandler)).Methods("POST")
	r.Handle("/signup", http.HandlerFunc(signupHandler)).Methods("POST")

	http.Handle("/", r)

	// r.Handle("/post", jwtMiddleware.Handler(http.HandlerFunc(handlerPost))).Methods("POST")
	// r.Handle("/search", jwtMiddleware.Handler(http.HandlerFunc(handlerSearch))).Methods("GET")
	// r.Handle("/login", http.HandlerFunc(loginHandler)).Methods("POST")
	// r.Handle("/signup", http.HandlerFunc(signupHandler)).Methods("POST")

	// r.Handle("/", r)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// {
// 	"user": "john",
// 	"message":"test",
// 	"location": {
// 			"lat": 37
// 			"lon": -120
// 	}
// }

func handlerPost(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one post request")

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

	// user := r.Context().Value("user")
	// claims := user.(*jwt.Token).Claims
	// username := claims.(jwt.MapClaims)["username"]

	user := r.Context().Value("user")
	claims := user.(*jwt.Token).Claims
	username := claims.(jwt.MapClaims)["username"]

	//32 左移20位 2^20 = 1024 * 1024 = 1M 一共32M
	r.ParseMultipartForm(32 << 20)

	fmt.Printf("Received one post request %s\n", r.FormValue("message"))

	lat, _ := strconv.ParseFloat(r.FormValue("lat"), 64)
	lon, _ := strconv.ParseFloat(r.FormValue("lon"), 64)

	p := &Post{
		User:    username.(string),
		Message: r.FormValue("message"),
		Location: Location{
			Lat: lat,
			Lon: lon,
		},
	}
	// decoder := json.NewDecoder(r.Body)

	// var p Post
	// if err := decoder.Decode(&p); err != nil {
	// 	panic(err)
	// }
	//Fprintf F = File. write to file. the first parameter is a io writer.

	id := uuid.New()

	file, _, err := r.FormFile("image")
	if err != nil {
		http.Error(w, "Image is not available", http.StatusInternalServerError)
		fmt.Printf("Image is not available %v.\n", err)
		return
	}
	defer file.Close()

	//go 运行时得到可以访问GCS的一个context
	ctx := context.Background()

	_, attrs, err := saveToGCS(ctx, file, BUCKET_NAME, id)
	if err != nil {
		http.Error(w, "GCS is not setup", http.StatusInternalServerError)
		fmt.Printf("GCS is not setup %v\n", err)
		panic(err)
	}

	p.Url = attrs.MediaLink
	// Save to ES
	saveToES(p, &id)

	// fmt.Fprintf(w, "Post successful: %v\n", p.Message)

	saveToBigTable(p, id)

}

func handlerSearch(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one request fro search")

	lat, _ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
	lon, _ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)

	ran := DISTANCE
	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}

	fmt.Printf("Search received %f, %f, %s", lat, lon, ran)

	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))

	if err != nil {
		panic(err)
	}

	q := elastic.NewGeoDistanceQuery("location")
	q = q.Distance(ran).Lat(lat).Lon(lon)

	searchResult, err := client.Search().
		Index(INDEX).
		Query(q).
		Pretty(true).
		Do()

	if err != nil {
		panic(err)
		return
	}

	fmt.Println("Query took %d milliseconds\n", searchResult.TookInMillis)
	fmt.Println("Found a total of %d posts\n", searchResult.TotalHits)

	var typ Post
	var ps []Post

	for _, item := range searchResult.Each(reflect.TypeOf(typ)) { // instance of
		p := item.(Post) // p=(Post)item
		fmt.Printf("Post by %s, %s at lat %v and lon %v\n", p.User,
			p.Message, p.Location.Lat, p.Location.Lon)
		ps = append(ps, p)
	}

	js, err := json.Marshal(ps)
	if err != nil {
		panic(err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write(js)
	// // Return a fake post
	// // &Post 返回的是object的地址。因为要用json.Marshal 主要是不希望copy整个Object。
	// // 因为copy address消耗比较小
	// p := &Post{
	// 	User:    "1111",
	// 	Message: "fake post",
	// 	Location: Location{
	// 		Lat: lat,
	// 		Lon: lon,
	// 	},
	// }

	// js, err := json.Marshal(p)
	// if err != nil {
	// 	panic(err)
	// }

	// w.Header().Set("Content-Type", "application/json")
	// w.Write(js)
	// fmt.Fprintf(w, "Search received: %f %f", lat, lon)
}

func saveToGCS(ctx context.Context, r io.Reader, bucketName, name string) (*storage.ObjectHandle, *storage.ObjectAttrs, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, nil, err
	}

	bucket := client.Bucket(bucketName)
	if _, err := bucket.Attrs(ctx); err != nil {
		return nil, nil, err
	}

	obj := bucket.Object(name)
	wc := obj.NewWriter(ctx)
	if _, err = io.Copy(wc, r); err != nil {
		return nil, nil, err
	}

	if err := wc.Close(); err != nil {
		return nil, nil, err
	}

	if err := obj.ACL().Set(ctx, storage.AllUsers, storage.RoleReader); err != nil {
		return nil, nil, err
	}

	attrs, err := obj.Attrs(ctx)

	fmt.Printf("Post uuid: %s\n", name)
	fmt.Printf("Post is saved to GCS: %s\n", attrs.MediaLink)
	return obj, attrs, err
}

func saveToES(p *Post, id *string) {
	es_client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))

	if err != nil {
		panic(err)
	}

	_, err1 := es_client.Index().
		Index(INDEX).
		Type(TYPE).
		Id(*id).
		BodyJson(p).
		Refresh(true).
		Do()

	if err1 != nil {
		panic(err1)
	}

	fmt.Printf("Post is saved to Elasticsearch INDEX -  %s: %s\n", INDEX, p.Message)
}

func saveToBigTable(p *Post, id string) {
	ctx := context.Background()
	// you must update project name here
	bt_client, err := bigtable.NewClient(ctx, PROJECT_ID, BT_INSTANCE)
	if err != nil {
		panic(err)
		return
	}

	tbl := bt_client.Open("post")
	mut := bigtable.NewMutation()
	t := bigtable.Now()

	mut.Set("post", "user", t, []byte(p.User))
	mut.Set("post", "message", t, []byte(p.Message))
	mut.Set("location", "lat", t, []byte(strconv.FormatFloat(p.Location.Lat, 'f', -1, 64)))
	mut.Set("location", "lon", t, []byte(strconv.FormatFloat(p.Location.Lon, 'f', -1, 64)))

	err = tbl.Apply(ctx, id, mut)
	if err != nil {
		panic(err)
		return
	}
	fmt.Printf("Post is saved to BigTable: %s\n", p.Message)

}
