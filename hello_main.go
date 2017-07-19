package main

import (
	"fmt"
	"net/http"
	"io/ioutil"
)

type Config struct {
	ListenPort  int
	BackendPort int
	BackendHost string
}

func main() {
	http.HandleFunc("/", func(responseWriter http.ResponseWriter, request *http.Request) {

		bytes, err := ioutil.ReadAll(request.Body)

		if err != nil {
			fmt.Fprintf(responseWriter, "ERROR: %s", err)
		}

		fmt.Println(string(bytes))

		fmt.Fprint(responseWriter, "hello")
	})
	http.ListenAndServe(":5000", nil)
}
