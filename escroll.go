package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
)

// Results wraps up top level search results from Elasticsearch
type Results struct {
	ScrollID string `json:"_scroll_id"`
	Hits     struct {
		Hits []Source `json:"hits"`
	}
}

// Source contains
type Source struct {
	Source Event `json:"_source"`
}

// Event contains a map of arbritrary JSON
type Event map[string]interface{}

// RequestBodySearch containts parts of the search body
type RequestBodySearch struct {
	Host, Query string
	Body        []byte
}

// Search performs the first search, returning the scroll ID and first batch of results
func Search(search RequestBodySearch) string {
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%+v/%+v", search.Host, search.Query), bytes.NewBuffer(search.Body))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Body.Close()
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Fatal(err)
	}
	return string(b)
}

func CheckParams(params RequestBodySearch) bool {
	// Not going to allow delete by query, because, there be dragons
	if strings.Contains(params.Query, "delete_by_query") == true {
		fmt.Println("Error: escroll wasn't made to do delete queries ;)")
		return false
	}

	// No point running if scroll isn't included in the scroll
	if strings.Contains(params.Query, "_search?scroll=") != true {
		fmt.Println("Error: The query contains invalid search scroll parameters. eg: /_search?scroll=30s")
		return false
	}

	// Test connection to host
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%+v", params.Host), nil)
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return false
	}
	// Ensure its returning 200
	defer res.Body.Close()
	if res.StatusCode != 200 {
		fmt.Printf("Error: Can't talk to elasticsearch. Status Code: %d\n", res.StatusCode)
		return false
	}
	// Could do some checking on ES versions here

	return true
}

func main() {
	// Import the flags
	file := flag.String("f", "", "Path to the search body data file")
	data := flag.String("d", "", "The body data input direclty to the command, same as curl")
	host := flag.String("h", "localhost:9200", "Elasticsearch server and port")
	query := flag.String("q", "/_search?scroll=0s", "Index path and query string")
	flag.Parse()

	// Populate the esSearch struct with data from flags
	esSearch := RequestBodySearch{Host: fmt.Sprintf("%s", *host), Query: fmt.Sprintf("%s", *query)}

	if CheckParams(esSearch) != true {
		os.Exit(1)
	}

	// Determine whether to use file or data for POST body
	if len(*file) > 0 {
		// If file path is greater null then read in the query JSON file
		c, fileErr := ioutil.ReadFile(*file)
		if fileErr != nil {
			fmt.Println(fileErr)
			return
		}
		esSearch.Body = c
	} else {
		// Else read in the data flag as the query body, like -d in curl
		esSearch.Body = []byte(*data)
	}

	// Perform the initial search to establish scroll
	first := Search(esSearch)
	fmt.Printf("%s\n", first)

}
