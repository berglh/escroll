package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/wsxiaoys/terminal/color"
)

// Results wraps up top level search results from Elasticsearch
type Results struct {
	ScrollID string `json:"_scroll_id"`
	Hits     struct {
		Total int      `json:"total"`
		Hits  []Source `json:"hits"`
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

// RequestSize captures the size of the scroll search, if specified
type RequestSize struct {
	Size string `json:"size"`
}

// FileOrData reads in from file or data flags and returns bytes for POST body
func FileOrData(file, data string) []byte {
	// Determine whether to use file or data for POST body
	if len(file) > 0 {
		// If file path is greater null then read in the query JSON file
		c, err := ioutil.ReadFile(file)
		if err != nil {
			Log(Error, err.Error())
		}
		return c
	}
	// Else read in the data flag as the query body, like -d in curl
	return []byte(data)
}

// CheckParams checks the flag parameters for errors
func CheckParams(params RequestBodySearch) {

	// Not going to allow delete by query, because, there be dragons
	if strings.Contains(params.Query, "delete_by_query") == true {
		Log(Error, "Delete_by_query is blocked by escroll")
	}

	// No point running if scroll isn't included in the scroll search
	if strings.Contains(params.Query, "_search?scroll=") != true {
		Log(Error, fmt.Sprintf("The query %s contains invalid search scroll parameters. Should be like: /_search?scroll=30s", params.Query))
	}

	// Test connection to host
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%+v", params.Host), nil)
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		Log(Error, err.Error())
	}
	// Ensure its returning 200
	defer res.Body.Close()
	if res.StatusCode != 200 {
		Log(Error, fmt.Sprintf("Can't talk to elasticsearch. Status Code: %d", res.StatusCode))
	}
	// Could do some checking on ES versions here
}

// Search performs the first search, returning the scroll ID and first batch of results
func Search(search RequestBodySearch) []byte {
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%+v/%+v", search.Host, search.Query), bytes.NewBuffer(search.Body))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		Log(Error, err.Error())
	}
	defer res.Body.Close()
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		Log(Error, err.Error())
	}
	return b
}

// Scroll performs the paginated scroll searches
func Scroll(search RequestBodySearch, scrollid []byte) []byte {
	//fmt.Printf("QUERY: http://%+v/%+v\n\n", search.Host, search.Query)
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%+v/%+v", search.Host, search.Query), bytes.NewBuffer(scrollid))
	//req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		Log(Error, err.Error())
	}
	defer res.Body.Close()
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		Log(Error, err.Error())
	}
	return b
}

func main() {
	// Start of main function
	fmt.Fprintln(os.Stderr, color.Sprintf("@g%s escroll b0.2.1", logTimestamp()))

	// Import the parameters from flags
	file := flag.String("f", "", "Path to the search body data file")
	data := flag.String("d", "", "The body data input direclty to the command, same as curl")
	host := flag.String("h", "localhost:9200", "Elasticsearch server and port")
	query := flag.String("q", "/_search?scroll=0s", "Index path and query string")
	pretty := flag.Bool("p", false, "Turn on pretty JSON output")
	Log(Info, "Processing flags and parameters")
	flag.Parse()

	// Declare variables
	var requestSize RequestSize // Struct to capture requested scroll size
	var respJSON Results        // Struct to capture response in JSON
	var scrollSize int          // Int to capture actual scroll size
	var scrollTotal int         // Int to capture total number of scrolls

	// Populate the esSearch with data from flags
	esSearch := RequestBodySearch{Host: fmt.Sprintf("%s", *host), Query: fmt.Sprintf("%s", *query)}

	// Returns the JSON bytes used for the initial search POST
	esSearch.Body = FileOrData(*file, *data)

	// Checks the flag data for errors
	CheckParams(esSearch)

	// Unmarshall the request query JSON to determine the scroll size
	if err := json.Unmarshal(esSearch.Body, &requestSize); err != nil {
		// If unable to Unmarshal, it's probably not going to work
		Log(Error, fmt.Sprintf("Could not unmarhsal the search body: %s", err))
	} else {
		// Capture the hit size of scroll search
		if requestSize.Size == "" {
			Log(Warn, "Scroll size not defined in search body, assuming default of 10")
			scrollSize = 10
		} else {
			scrollSize, err = strconv.Atoi(requestSize.Size)
			if err != nil {
				Log(Error, "Unable to convert size of query into int")
			} else {
				Log(Info, fmt.Sprintf("Size of scroll: %d", scrollSize))
			}
		}
	}

	// Capture the start time of scoll searching
	startTime := int(time.Now().UnixNano())

	// Perform the initial search to establish scroll
	first := Search(esSearch)

	// Unmarshall the results
	if err := json.Unmarshal(first, &respJSON); err != nil {
		Log(Error, err.Error())
		return
	}

	// Capture hit total as int and numbt of scrolls required
	total := respJSON.Hits.Total
	if total%scrollSize == 0 {
		scrollTotal = total / scrollSize
	} else {
		scrollTotal = (total / scrollSize) + 1
	}
	Log(Info, fmt.Sprintf("Total Hits: %d Total Scrolls: %d", total, scrollTotal))

	// Iterate through the mapped JSON and marshall as pretty or lines JSON
	if len(respJSON.Hits.Hits) > 4 {
		for _, v := range respJSON.Hits.Hits {
			if *pretty == false {
				o, _ := json.Marshal(v.Source)
				fmt.Printf("%s\n", o)
			} else {
				o, _ := json.MarshalIndent(v.Source, "", "    ")
				fmt.Printf("%s\n", o)
			}
		}
	} else {
		Log(Error, "No Elasticsearch Hits Found")
	}

	scrollID := []byte(respJSON.ScrollID) // Capture the scroll ID to perform scrolling
	scroll := []byte("{seed}")            // Variable to store the scroll search JSON output
	scrollNum := 1                        // The scroll number starts at one due to the initial search

	// Munge the query string of the search to hit the scroll API
	reg := regexp.MustCompile(`.*\/([^\/]+)`)
	esSearch.Query = reg.ReplaceAllString(esSearch.Query, "${1}&filter_path=hits.hits._source")
	esSearch.Query = strings.Replace(esSearch.Query, "_search", "_search/scroll", 1)

	// Scroll Search Loop
	for len(scroll) > 4 {

		// Perform the scroll search
		scroll = Scroll(esSearch, scrollID)

		// Track some metrics on the scrolling to form estimates
		scrollTime := int(time.Now().UnixNano())
		scrollNum++
		estimateTimeMs := ((scrollTime - startTime) / 1000000 / scrollNum) * (scrollTotal - scrollNum)
		eSec := (estimateTimeMs / 1000) % 60
		eMin := (estimateTimeMs / 1000 / 60) % 60
		eHrs := (estimateTimeMs / 1000 / 60) / 60

		// Unmarshal the scroll search results
		if err := json.Unmarshal(scroll, &respJSON); err != nil {
			Log(Error, err.Error())
			return
		}

		// Iterate through the mapped JSON and marshall as pretty or lines JSON
		for _, v := range respJSON.Hits.Hits {
			if *pretty == false {
				o, _ := json.Marshal(v.Source)
				fmt.Printf("%s\n", o)
			} else {
				o, _ := json.MarshalIndent(v.Source, "", "    ")
				fmt.Printf("%s\n", o)
			}
		}

		// Output escroll progress
		fmt.Fprintf(os.Stderr, fmt.Sprintf("\r%s Scrolls: [ %d / %d ]  Estimated Time: [ %02d:%02d:%02d ]", logTimestamp(), scrollNum, scrollTotal, eHrs, eMin, eSec))
	}

	// Successful end of escroll
	Log(OK, "Scroll search finished")

}
