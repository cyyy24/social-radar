package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"reflect"
	"strconv"

	"cloud.google.com/go/bigtable"

	"cloud.google.com/go/storage"
	"github.com/olivere/elastic"
	"github.com/pborman/uuid"
	"google.golang.org/api/option"
)

const (
	POST_INDEX = "post" // database name (for storing posts into Elastic Search).
	POST_TYPE  = "post" // database table name.

	DISTANCE        = "200km"                         // search range.
	ES_URL          = "http://35.196.164.154:9200"    // url & ports of Google Compute Engine VM instance and Elastic Search listening port.
	BUCKET_NAME     = "socialradar-post-images"       // bucket (folder) name of GCS (Google Cloud Storage).
	CREDENTIAL_FILE = "SocialRadar-576b9b3c0db7.json" // ServiceAccount key file.

	ENABLE_BIGTABLE = true               // Set this to "true" if want to use Google BigTable.
	PROJECT_ID      = "socialradar"      // for saveToBigTable func.
	BT_INSTANCE     = "socialradar-post" // BigTable instance id.
)

// Location struct representing what data location contains.
type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

// Post struct representing what data a user's post contains.
type Post struct {
	// `json:"user"` is for json parsing. Otherwise, by default it's 'User' (Same applies to fields below).
	User     string   `json:"user"`
	Message  string   `json:"message"`
	Location Location `json:"location"`
	Url      string   `json:"url"`
}

// ------------------ MAIN FUNCTION ------------------
func main() {
	fmt.Println("started-service")
	createIndexIfNotExist()

	http.HandleFunc("/post", handlerPost)
	http.HandleFunc("/search", handlerSearch) // a GET request.
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// ---------------------------------------------------

/**
 *  Handler functions to handle HTTP requests (GET, POST):
 */
// Function that handles a POST request.
func handlerPost(w http.ResponseWriter, r *http.Request) {
	// Parse from body of request to get a json object.
	fmt.Println("Received one post request")

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")

	lat, _ := strconv.ParseFloat(r.FormValue("lat"), 64)
	lon, _ := strconv.ParseFloat(r.FormValue("lon"), 64)

	p := &Post{
		User:    r.FormValue("user"),
		Message: r.FormValue("message"),
		Location: Location{
			Lat: lat,
			Lon: lon,
		},
	}

	id := uuid.New()
	file, _, err := r.FormFile("image")
	if err != nil {
		http.Error(w, "Image is not available", http.StatusBadRequest)
		fmt.Printf("Image is not available %v.\n", err)
		return
	}

	attrs, err := saveToGCS(file, BUCKET_NAME, id)
	if err != nil {
		http.Error(w, "Failed to save image to GCS", http.StatusInternalServerError)
		fmt.Printf("Failed to save image to GCS %v.\n", err)
		return
	}
	p.Url = attrs.MediaLink

	err = saveToES(p, id)
	if err != nil {
		http.Error(w, "Failed to save post to ElasticSearch", http.StatusInternalServerError)
		fmt.Printf("Failed to save post to ElasticSearch %v.\n", err)
		return
	}
	fmt.Printf("Saved one post to ElasticSearch: %s", p.Message)

	if ENABLE_BIGTABLE {
		saveToBigTable(p, id)
	}
}

// Function that handles a GET request (search for nearby posts).
func handlerSearch(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one request for search")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")

	lat, _ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
	lon, _ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)
	// range is optional
	ran := DISTANCE
	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}

	posts, err := readFromES(lat, lon, ran)
	if err != nil {
		http.Error(w, "Failed to read post from ElasticSearch", http.StatusInternalServerError)
		fmt.Printf("Failed to read post from ElasticSearch %v.\n", err)
		return
	}

	js, err := json.Marshal(posts)
	if err != nil {
		http.Error(w, "Failed to parse posts into JSON format", http.StatusInternalServerError)
		fmt.Printf("Failed to parse posts into JSON format %v.\n", err)
		return
	}

	w.Write(js)
}

/**
 *  Helper functions:
 */
// Function that creates a table in ES for storing our data if table not already exists.
func createIndexIfNotExist() {
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
	}

	exists, err := client.IndexExists(POST_INDEX).Do(context.Background())
	if err != nil {
		panic(err)
	}

	if !exists {
		mapping := `{
            "mappings": {
                "post": {
                    "properties": {
                        "location": {
                            "type": "geo_point"
                        }
                    }
                }
            }
        }`
		_, err = client.CreateIndex(POST_INDEX).Body(mapping).Do(context.Background())
		if err != nil {
			panic(err)
		}
	}
}

// Function that helps save a post to Google BigTable for later transmitting data to BigQuery for offline analysis.
func saveToBigTable(p *Post, id string) {
	ctx := context.Background()
	bt_client, err := bigtable.NewClient(ctx, PROJECT_ID, BT_INSTANCE, option.WithCredentialsFile(CREDENTIAL_FILE))
	if err != nil {
		panic(err)
		return
	}

	tbl := bt_client.Open("post")
	mut := bigtable.NewMutation()
	t := bigtable.Now()
	mut.Set("post", "user", t, []byte(p.User)) // cast to byte[] b/c BigTable stores data in byte array form.
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

// Function that helps save a post to ElasticSearch on GCE (Google Compute Engine).
func saveToES(post *Post, id string) error {
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		return err
	}

	_, err = client.Index().
		Index(POST_INDEX).
		Type(POST_TYPE).
		Id(id).
		BodyJson(post).
		Refresh("wait_for").
		Do(context.Background())
	if err != nil {
		return err
	}

	fmt.Printf("Post is saved to index: %s\n", post.Message)
	return nil
}

// Function that helps read the returned result from Elastic Search.
func readFromES(lat, lon float64, ran string) ([]Post, error) {
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		return nil, err
	}

	query := elastic.NewGeoDistanceQuery("location")
	query = query.Distance(ran).Lat(lat).Lon(lon)

	searchResult, err := client.Search().
		Index(POST_INDEX).
		Query(query).
		Pretty(true).
		Do(context.Background())
	if err != nil {
		return nil, err
	}

	// searchResult is of type SearchResult and returns hits, suggestions,
	// and all kinds of other information from Elasticsearch.
	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)

	// Each is a convenience function that iterates over hits in a search result.
	// It makes sure you don't need to check for nil values in the response.
	// However, it ignores errors in serialization. If you want full control
	// over iterating the hits, see below.
	var ptyp Post
	var posts []Post
	for _, item := range searchResult.Each(reflect.TypeOf(ptyp)) {
		if p, ok := item.(Post); ok {
			posts = append(posts, p)
		}
	}

	return posts, nil
}

// Function that helps save the image of a post to GCS (Google Cloud Storage).
// It will return the MediaLink (url) of the saved image.
func saveToGCS(r io.Reader, bucketName, objectName string) (*storage.ObjectAttrs, error) {
	ctx := context.Background()

	//client, err := storage.NewClient(ctx）

	// Creates a client (with option to pass credential file).
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(CREDENTIAL_FILE))
	if err != nil {
		return nil, err
	}

	bucket := client.Bucket(bucketName)
	if _, err := bucket.Attrs(ctx); err != nil {
		return nil, err
	}

	object := bucket.Object(objectName)
	wc := object.NewWriter(ctx)
	if _, err = io.Copy(wc, r); err != nil {
		return nil, err
	}
	if err := wc.Close(); err != nil {
		return nil, err
	}

	// Grant access to the bucket to everyone on the Internet (for downloading images).
	if err = object.ACL().Set(ctx, storage.AllUsers, storage.RoleReader); err != nil {
		return nil, err
	}

	attrs, err := object.Attrs(ctx)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Image is saved to GCS: %s\n", attrs.MediaLink)
	return attrs, nil
}
