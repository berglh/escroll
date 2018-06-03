package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/wsxiaoys/terminal/color"
)

const (
	esrcollVersion = "v0.3.0" // escroll version number
)

// Results wraps up top level search results from Elasticsearch
// Comment: This potentially a terrible way to do this, ommits query/shard/score stats in hits
type Results struct {
	ScrollID string `json:"_scroll_id"`
	Hits     struct {
		Total int     `json:"total"`
		Hits  []Event `json:"hits"`
	}
}

// Event contains a map of arbritrary JSON in the ES Hits.Hits object
type Event map[string]interface{}

// PrimarySearchRequest is a struct to collect all the elements for scroll search requests
type PrimarySearchRequest struct {
	URI        *url.URL
	SearchBody []byte
	ScrollBody []byte
}

// ScrollRequestBody is the body used for conducting the scroll searches
type ScrollRequestBody struct {
	Scroll   string `json:"scroll"`
	ScrollID string `json:"scroll_id"`
}

// RequestSize captures the size of the scroll search, if specified
type RequestSize struct {
	Size string `json:"size"`
}

// ParseData reads in the data flag like curl
func ParseData(data string) []byte {
	// Determine if it's a file by the @prefix like curl
	if strings.HasPrefix(data, "@") == true {
		file := strings.TrimPrefix(data, "@")
		c, err := ioutil.ReadFile(file)
		if err != nil {
			Log("Error", err.Error())
		}
		return c
	}
	// Otherwise treat it as raw data
	return []byte(data)
}

// SecDurationFormat takes in seconds and formats it in hours, minutes and seconds
func SecDurationFormat(duration int) (int, int, int) {
	return (duration / 1000 / 60) / 60, (duration / 1000 / 60) % 60, (duration / 1000) % 60
}

// CheckParams checks the flag parameters for errors
func CheckParams(params PrimarySearchRequest) {

	// Not going to allow delete by query, because, there be dragons
	if strings.Contains(params.URI.RawQuery, "delete_by_query") == true {
		Log("Error", "Delete_by_query is blocked by escroll")
	}

	// Check the query contains the required parameters for a valid scroll search
	if params.URI.RawQuery == "" {
		Log("Error", "No query parameters for scroll search")
	}
	if strings.Contains(params.URI.Path, "_search") != true {
		Log("Error", fmt.Sprintf("The path \"%s\" is missing the search method parameter. Required format: \"/index/_search\"", params.URI.Path))
	}
	scrollWait, ok := params.URI.Query()["scroll"]
	if !ok || len(scrollWait) < 1 {
		Log("Error", fmt.Sprintf("The query \"%s\" is missing scroll parameters. Required format: \"/index/_search?scroll=30s\"", params.URI.RawQuery))
	}
	// Can't do calculations if hits.total is unavailable when using the filter_path feature
	if strings.Contains(params.URI.RawQuery, "filter_path") == true && strings.Contains(params.URI.RawQuery, "hits.total") != true {
		Log("Error", fmt.Sprintf("This query \"%s\" that contains filter_path is missing \"filter_path=hits.total\" which prevents escroll tracking the number of scrolls correctly", params.URI.RawQuery))
	}

	// Test connection to host
	req, err := http.NewRequest("GET", fmt.Sprintf("%+v://%+v", params.URI.Scheme, params.URI.Host), nil)
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		Log("Error", err.Error())
	}
	// Ensure its returning 200
	defer res.Body.Close()
	if res.StatusCode != 200 {
		Log("Error", fmt.Sprintf("Can't talk to elasticsearch node. Status Code: %d", res.StatusCode))
	}
	// Could do some checking on ES versions here
}

// SearchRequest does the actual http client request against Elasticsearch
func SearchRequest(req *http.Request) []byte {
	client := &http.Client{}
	res, err := client.Do(req)
	// If the response isn't empty, defer close body
	if res != nil {
		defer res.Body.Close()
	}
	// Exit if there is an error
	if err != nil {
		Log("Error", err.Error())
	}
	// An error in elasticsearch is still a valid response, check the status code
	if res.StatusCode != 200 {
		oBytes, _ := ioutil.ReadAll(res.Body)
		res.Body.Close()
		Log("Error", fmt.Sprintf("Scroll Search Error: %d, %+v", res.StatusCode, string(oBytes)))
	}
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		Log("Error", err.Error())
	}
	return b
}

// ScrollSearch prepares the http request for the inital scroll search
func ScrollSearch(search PrimarySearchRequest) []byte {
	var req *http.Request
	var err error
	// Build the http request using the search request struct and query body
	req, err = http.NewRequest("GET", fmt.Sprintf("%+v", search.URI.String()), bytes.NewBuffer(search.SearchBody))
	if err != nil {
		Log("Error", err.Error())
	}
	// All payloads must be explicitly defined as of ESv6
	req.Header.Add("Content-Type", "application/json")
	b := SearchRequest(req)
	return b
}

// NextScroll prepares the http request for the paginated scrolls
func NextScroll(search PrimarySearchRequest) []byte {
	var req *http.Request
	var err error
	// Build the http request using the search request struct and scroll body
	req, err = http.NewRequest("GET", fmt.Sprintf("%+v", search.URI.String()), bytes.NewBuffer(search.ScrollBody))
	if err != nil {
		Log("Error", err.Error())
	}
	// All payloads must be explicitly defined as of ESv6
	req.Header.Add("Content-Type", "application/json")
	b := SearchRequest(req)
	return b
}

func main() {
	// Start of main function

	// Import the parameters from flags
	scrolluri := flag.String("url", "", "Elasticsearch scroll search URI\n\ti.e. \"http://localhost:9200/_search/scroll=30s\"")
	data := flag.String("d", "", "The query body to send in the POST request.\n\tEmulates the -d/data-ascii flag in curl. Prefixing the string with @ will in the valid file as ASCII encoded. ie.\"@filname.json\"")
	pretty := flag.Bool("p", false, "Switch to turn on pretty JSON output")
	version := flag.Bool("v", false, "Print version information")
	flag.Parse()

	// Parse in the scroll URL using the net/url package
	if scrolluri != nil {
		if strings.Contains(*scrolluri, "http") != true {
			*scrolluri = "http://" + *scrolluri
		}
		_, err := url.Parse(*scrolluri)
		if err != nil {
			Log("Error", fmt.Sprintf("Malformed URL: %s", err))
		}
	} else {
		Log("Error", "No URL parameter supplied")
	}

	if *version == true {
		fmt.Fprintf(os.Stderr, "escroll %s\n", esrcollVersion)
		os.Exit(0)
	} else {
		fmt.Fprintln(os.Stderr, color.Sprintf("@g%s escroll %s", logTimestamp(), esrcollVersion))
		Log("Info", "Processing flags and parameters")
	}

	// Declare variables
	var requestSize RequestSize       // Struct to capture requested scroll size
	var esSearch PrimarySearchRequest // Search request paramaters
	respJSON := &Results{}            // Pointer to capture the JSON response
	var scrollSize int                // Int to capture actual scroll size
	var scrollTotal int               // Int to capture total number of scrolls
	scrollNum := 1                    // Seed scroll number variable to track scroll iterations

	// Populate the esSearch with data from flags
	esSearch.URI, _ = url.Parse(*scrolluri)
	esSearch.SearchBody = ParseData(*data)

	// Checks the flag data for errors
	CheckParams(esSearch)

	// Unmarshall the request query JSON to determine the scroll size
	if err := json.Unmarshal(esSearch.SearchBody, &requestSize); err != nil {
		// If unable to Unmarshal, it's probably not going to work as a query
		Log("Error", fmt.Sprintf("Could not unmarhsal the search body: %s", err))
	} else {
		// Capture the hit size of scroll search
		if requestSize.Size == "" {
			Log("Warn", "Scroll size not defined in search body, assuming default of 10")
			scrollSize = 10
		} else {
			scrollSize, err = strconv.Atoi(requestSize.Size)
			if err != nil {
				Log("Error", "Unable to convert size of query into int")
			} else {
				Log("Info", fmt.Sprintf("Size of scroll: %d", scrollSize))
			}
		}
	}

	// Capture the start time of scoll searching
	startTime := int(time.Now().UnixNano())

	// Perform the initial search to establish scroll
	searchResults := ScrollSearch(esSearch)

	// Unmarshall the results
	if err := json.Unmarshal(searchResults, &respJSON); err != nil {
		Log("Error", err.Error())
		return
	}

	// Capture hit total as int and number of scrolls required
	total := respJSON.Hits.Total
	if total%scrollSize == 0 {
		scrollTotal = total / scrollSize
	} else {
		scrollTotal = (total / scrollSize) + 1
	}
	Log("Info", fmt.Sprintf("Total Hits: %d Total Scrolls: %d", total, scrollTotal))

	// Iterate through the mapped JSON and marshall as pretty or lines JSON
	if len(respJSON.Hits.Hits) > 4 {
		for _, v := range respJSON.Hits.Hits {
			if *pretty == false {
				o, _ := json.Marshal(v)
				fmt.Printf("%s\n", o)
			} else {
				o, _ := json.MarshalIndent(v, "", "    ")
				fmt.Printf("%s\n", o)
			}
		}
	} else {
		Log("Error", "No Elasticsearch Hits Found")
	}

	// Build the scroll request body from the response of the first search
	// This replaces the legacy approach where only the scrollID comprises the payload body in v0.2.4
	scrollBody, err := json.Marshal(ScrollRequestBody{ScrollID: respJSON.ScrollID, Scroll: strings.Join(esSearch.URI.Query()["scroll"], ", ")})
	if err != nil {
		Log("Error", err.Error())
		return
	}
	// Assign the body to the search struct element
	esSearch.ScrollBody = []byte(scrollBody)

	// Modification of the body query search to be syntactically correct for the scroll API
	// This block has replaced the old regex method and references the query parameters directly after v0.2.4
	esSearch.URI.Path = "/_search/scroll"
	q, _ := url.ParseQuery(esSearch.URI.RawQuery)
	q.Del("scroll")
	q.Del("_source")
	esSearch.URI.RawQuery = q.Encode()
	//Log("Info", fmt.Sprintf("ScrollURL: %+v\nScrollQuery: %+v\nScrollPath: %+v\n", esSearch.URI, esSearch.URI.RawQuery, esSearch.URI.Path))

	// Scroll Search Loop
	for len(respJSON.Hits.Hits) > 4 {

		// Clear respJSON to avoid returning old results
		respJSON = nil

		// Perform the scroll search
		scroll := NextScroll(esSearch)

		// Track some metrics on the scrolling to form estimates
		scrollTime := int(time.Now().UnixNano())
		scrollNum++

		// Get some useful values to display estimated time
		eHr, eMin, eSec := SecDurationFormat(((scrollTime - startTime) / 1000000 / scrollNum) * (scrollTotal - scrollNum))

		// Unmarshal the scroll search results
		if err := json.Unmarshal(scroll, &respJSON); err != nil {
			Log("Error", err.Error())
			return
		}

		// Iterate through the mapped JSON and marshall as pretty or lines JSON
		if len(respJSON.Hits.Hits) > 4 {
			for _, v := range respJSON.Hits.Hits {
				if *pretty == false {
					o, _ := json.Marshal(v)
					fmt.Printf("%s\n", o)
				} else {
					o, _ := json.MarshalIndent(v, "", "    ")
					fmt.Printf("%s\n", o)
				}
			}

			// Output escroll progress
			fmt.Fprintf(os.Stderr, fmt.Sprintf("\r%s Scrolls: [ %d / %d ]  Estimated Time: [ %02d:%02d:%02d ]", logTimestamp(), scrollNum, scrollTotal, eHr, eMin, eSec))
		}

	}

	scrollTime := int(time.Now().UnixNano())
	tHr, tMin, tSec := SecDurationFormat((scrollTime - startTime) / 1000000)

	fmt.Fprintf(os.Stderr, fmt.Sprintf("\n%s Completed %d scrolls in: %02d:%02d:%02d", logTimestamp(), scrollTotal, tHr, tMin, tSec))

	// Successful end of escroll
	Log("NlnOK", "Scroll search finished")

}
