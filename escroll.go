package main

import (
	"bytes"
	"encoding/json"
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

// Source is a struct of an event from Elasticsearch
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

// FileOrData reads in from file or data flags and returns bytes for POST body
func FileOrData(file, data string) []byte {
	// Determine whether to use file or data for POST body
	if len(file) > 0 {
		// If file path is greater null then read in the query JSON file
		c, fileErr := ioutil.ReadFile(file)
		if fileErr != nil {
			fmt.Println(fileErr)
			os.Exit(1)
		}
		return c
	} else {
		// Else read in the data flag as the query body, like -d in curl
		return []byte(data)
	}
}

// CheckParams checks the flag parameters for errors
func CheckParams(params RequestBodySearch) bool {

	// Not going to allow delete by query, because, there be dragons
	if strings.Contains(params.Query, "delete_by_query") == true {
		fmt.Println("Error: escroll wasn't made to do delete queries.")
		return false
	}

	// No point running if scroll isn't included in the scroll
	if strings.Contains(params.Query, "_search?scroll=") != true {
		fmt.Printf("Error: The query %s contains invalid search scroll parameters. Should be like: /_search?scroll=30s\n", params.Query)
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

// Search performs the first search, returning the scroll ID and first batch of results
func Search(search RequestBodySearch) []byte {
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
	return b
}

func main() {
	// Import the flags
	file := flag.String("f", "", "Path to the search body data file")
	data := flag.String("d", "", "The body data input direclty to the command, same as curl")
	host := flag.String("h", "localhost:9200", "Elasticsearch server and port")
	query := flag.String("q", "/_search?scroll=0s", "Index path and query string")
	pretty := flag.Bool("p", false, "Turn on pretty JSON output")
	flag.Parse()

	// Populate the esSearch with data from flags
	esSearch := RequestBodySearch{Host: fmt.Sprintf("%s", *host), Query: fmt.Sprintf("%s", *query)}

	// Returns the JSON bytes for the search POST
	esSearch.Body = FileOrData(*file, *data)

	// Checks the flag data for errors
	if CheckParams(esSearch) != true {
		os.Exit(1)
	}

	// Perform the initial search to establish scroll
	first := Search(esSearch)
	var respJSON Results

	// Unmarshall the results
	if err := json.Unmarshal(first, &respJSON); err != nil {
		log.Println(err)
		return
	}

	// Iterate through the mapped JSON and marshall as pretty or lines JSON
	for _, v := range respJSON.Hits.Hits {
		if *pretty != false {
			o, _ := json.Marshal(v.Source)
			fmt.Printf("%s\n", o)
		} else {
			o, _ := json.MarshalIndent(v.Source, "", "    ")
			fmt.Printf("%s\n", o)
		}
	}

	//scrollID, _ := json.Marshal(respJSON)
	fmt.Printf("Scroll ID: %s\n", respJSON.ScrollID)

}
