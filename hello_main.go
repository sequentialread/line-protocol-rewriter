package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

func main() {
	http.HandleFunc("/", func(responseWriter http.ResponseWriter, request *http.Request) {

		bytes, err := ioutil.ReadAll(request.Body)

		if err != nil {
			fmt.Fprintf(responseWriter, "ERROR: %s", err)
		}

		fmt.Print(string(bytes))

		fmt.Fprint(responseWriter, "hello")
	})

	log.Fatal(http.ListenAndServe(":5000", nil))
}
