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

const (
	esrcollVersion = "v0.2.3" // escroll version number
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

// RequestBodySearch is a struct
type RequestBodySearch struct {
	Host, Query string
	Body        []byte
}

// RequestSize captures the size of the scroll search, if specified
type RequestSize struct {
	Size string `json:"size"`
}

// Data reads in the data flag like curl
func Data(data string) []byte {
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

// CheckParams checks the flag parameters for errors
func CheckParams(params RequestBodySearch) {

	// Not going to allow delete by query, because, there be dragons
	if strings.Contains(params.Query, "delete_by_query") == true {
		Log("Error", "Delete_by_query is blocked by escroll")
	}

	// No point running if scroll isn't included in the scroll search
	if strings.Contains(params.Query, "_search?scroll=") != true {
		Log("Error", fmt.Sprintf("The query %s contains invalid search scroll parameters. Should be like: /_search?scroll=30s", params.Query))
	}

	// Test connection to host
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%+v", params.Host), nil)
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

// Search performs the first search, returning the scroll ID and first batch of results
func Search(search RequestBodySearch) []byte {
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%+v/%+v", search.Host, search.Query), bytes.NewBuffer(search.Body))
	req.Header.Set("Content-Type", "application/json")
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
		Log("Error", fmt.Sprintf("Primary Search Failed: %d, %+v", res.StatusCode, string(oBytes)))
	}
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		Log("Error", err.Error())
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
		Log("Error", fmt.Sprintf("Scroll Search Failed: %d, %+v", res.StatusCode, string(oBytes)))
	}
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		Log("Error", err.Error())
	}
	return b
}

func main() {
	// Start of main function
	fmt.Fprintln(os.Stderr, color.Sprintf("@g%s escroll %s", logTimestamp(), esrcollVersion))

	// Import the parameters from flags
	server := flag.String("s", "localhost:9200", "Elasticsearch server and port")
	query := flag.String("q", "/_search?scroll=0s", "Index path and query string")
	data := flag.String("d", "", "The query body to send in the POST request.\n\tEmulates the -d/data-ascii flag in curl. Prefixing the string with @ will in the valid file as ASCII encoded. ie.\"@filname.json\"")
	pretty := flag.Bool("p", false, "Switch to turn on pretty JSON output")
	Log("Info", "Processing flags and parameters")
	flag.Parse()

	// Declare variables
	var requestSize RequestSize // Struct to capture requested scroll size
	respJSON := &Results{}      // Pointer to capture the JSON response
	var scrollSize int          // Int to capture actual scroll size
	var scrollTotal int         // Int to capture total number of scrolls
	scrollNum := 1              // Seed scroll number variable to track scroll iterations

	// Populate the esSearch with data from flags
	esSearch := RequestBodySearch{Host: fmt.Sprintf("%s", *server), Query: fmt.Sprintf("%s", *query)}

	// Returns the JSON bytes used for the initial search POST
	esSearch.Body = Data(*data)

	// Checks the flag data for errors
	CheckParams(esSearch)

	// Unmarshall the request query JSON to determine the scroll size
	if err := json.Unmarshal(esSearch.Body, &requestSize); err != nil {
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
	searchResults := Search(esSearch)

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

	scrollID := []byte(respJSON.ScrollID) // Capture the scroll ID to perform scrolling

	// Modification of the body query search to be syntactically correct for the scroll API
	// This regex groups filter paths and _source filtering, however _source filter is not recognised by scroll API
	// Future Improvement: Need to test senarios where the order of query string items are shuffled around
	reg := regexp.MustCompile(`.*\/(.*\?)([^\/]*(scroll=[\d]+(d|h|m|s|ms|micros|nanos))(&filter_path=[^&]+)?(&_source=[^&]*)?)`)
	esSearch.Query = reg.ReplaceAllString(esSearch.Query, "${1}${3}${5}")
	esSearch.Query = strings.Replace(esSearch.Query, "_search", "_search/scroll", 1)

	// Scroll Search Loop
	for len(respJSON.Hits.Hits) > 4 {

		// Clear respJSON to avoid returning old results
		respJSON = nil

		// Perform the scroll search
		scroll := Scroll(esSearch, scrollID)

		// Track some metrics on the scrolling to form estimates
		scrollTime := int(time.Now().UnixNano())
		scrollNum++
		estimateTimeMs := ((scrollTime - startTime) / 1000000 / scrollNum) * (scrollTotal - scrollNum)
		eSec := (estimateTimeMs / 1000) % 60
		eMin := (estimateTimeMs / 1000 / 60) % 60
		eHrs := (estimateTimeMs / 1000 / 60) / 60

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
			fmt.Fprintf(os.Stderr, fmt.Sprintf("\r%s Scrolls: [ %d / %d ]  Estimated Time: [ %02d:%02d:%02d ]", logTimestamp(), scrollNum, scrollTotal, eHrs, eMin, eSec))
		}

	}

	// Successful end of escroll
	Log("NlnOK", "Scroll search finished")

}
