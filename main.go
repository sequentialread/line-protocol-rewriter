package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httputil"
	"reflect"
	"regexp"
	"strconv"
	"text/template"
	"time"

	"github.com/forestjohnsonpeoplenet/influxStyleEnvOverride"
)

type Config struct {
	ListenPort      int
	BackendHostPort string
	BackendScheme   string
	Rewriters       []*LineProtocolRewriter
	SelfMetrics     *SelfMetricsConfig
}

type SelfMetricsConfig struct {
	MetricName          string
	Database            string
	FlushInterval       string
	FlushIntervalParsed time.Duration
}

type LineProtocolRewriter struct {
	MatchedPointsMetricChannel          chan int
	SelectorErrorsChannel               chan SelectorError
	TotalMatchedPoints                  int
	SelectorErrors                      map[string]int
	Name                                string
	MeasurementNameMustMatch            string
	MeasurementNameMustNotMatch         string
	MustHaveTagKey                      string
	MeasurementNameMustMatchCompiled    *regexp.Regexp
	MeasurementNameMustNotMatchCompiled *regexp.Regexp
	AddFields                           []*Selector
	AddTags                             []*Selector
	RemoveFields                        []string
	RemoveTags                          []string
}

type Selector struct {
	Key                 string
	ValueTemplate       string
	ValueTemplateParsed *template.Template
}

type SelectorError struct {
	Template     string
	ErrorMessage string
}

type PointModel struct {
	MeasurementName        string
	MeasurementNameMatches []string
	Tags                   map[string]*string
	Fields                 map[string]*string
	Timestamp              int64
}

const (
	MeasurementNameState = iota
	TagKeyState
	TagValueState
	FieldKeyState
	FieldValueState
	TimestampState
)

const newlineByte = byte('\n')
const escapeByte = byte('\\')
const spaceByte = byte(' ')
const commaByte = byte(',')
const equalsByte = byte('=')

const pointsChannelSize = 10
const receiveBufferSizeBytes = 512
const maximumStringLengthBytes = 512
const selfMetricBufferSizeBytes = 8192
const metricsChannelSize = 10000
const metricsChannelFlushIntervalString = "10ms"
const pointCountKey = "Point-Count"

var metricsChannelFlushInterval time.Duration
var incomingPointsMetricChannel chan int
var outgoingPointsMetricChannel chan int
var droppedPointsMetricChannel chan int
var incomingPointsTotal int
var outgoingPointsTotal int
var droppedPointsTotal int
var incomingRequestsTotal int
var outgoingRequestsTotal int
var droppedRequestsTotal int
var lastMetricSent time.Time
var config *Config

func main() {
	fmt.Println("Hello from Golang")

	var err error
	metricsChannelFlushInterval, err = time.ParseDuration(metricsChannelFlushIntervalString)
	if err != nil {
		panic(err)
	}
	incomingPointsMetricChannel = make(chan int, metricsChannelSize)
	outgoingPointsMetricChannel = make(chan int, metricsChannelSize)
	droppedPointsMetricChannel = make(chan int, metricsChannelSize)
	lastMetricSent = time.Now()

	configBytes, err := ioutil.ReadFile("config.json")
	if err != nil {
		panic(err)
	}

	config = &Config{}
	err = json.Unmarshal(configBytes, config)
	if err != nil {
		panic(err)
	}

	influxStyleEnvOverride.ApplyInfluxStyleEnvOverrides("LPRW", reflect.ValueOf(config))

	for i, rewriter := range config.Rewriters {
		if rewriter.MeasurementNameMustMatch != "" {
			rewriter.MeasurementNameMustMatchCompiled = regexp.MustCompile(rewriter.MeasurementNameMustMatch)
		}
		if rewriter.MeasurementNameMustNotMatch != "" {
			rewriter.MeasurementNameMustNotMatchCompiled = regexp.MustCompile(rewriter.MeasurementNameMustNotMatch)
		}
		usedNames := make(map[string]bool)

		rewriter.MatchedPointsMetricChannel = make(chan int, metricsChannelSize)
		rewriter.SelectorErrorsChannel = make(chan SelectorError, metricsChannelSize)

		parseSelector := func(selector *Selector, selectorType string, selectorIndex int) {
			selectorName := fmt.Sprintf("rewriter-%d-%s-add-%s-selector-%d-add-%s", i, rewriter.Name, selectorType, selectorIndex, selector.Key)
			if selector.Key == "" {
				panic(fmt.Errorf("%s:  Key is empty.", selectorName))
			}
			if usedNames[selector.Key] {
				panic(fmt.Errorf("%s:  Key %s already declared", selectorName, selector.Key))
			}
			selector.ValueTemplateParsed = template.Must(template.New(selectorName).Parse(selector.ValueTemplate))
		}

		for j, selector := range rewriter.AddFields {
			parseSelector(selector, "field", j)
		}
		for j, selector := range rewriter.AddTags {
			parseSelector(selector, "tag", j)
		}
	}
	if config.SelfMetrics != nil {
		interval, err := time.ParseDuration(config.SelfMetrics.FlushInterval)
		if err != nil {
			panic(err)
		}
		config.SelfMetrics.FlushIntervalParsed = interval
	}

	proxy := &httputil.ReverseProxy{
		Director: func(request *http.Request) {
			request.Host = config.BackendHostPort
			request.URL.Host = config.BackendHostPort
			request.URL.Scheme = config.BackendScheme
			newBody, newContentLength, pointCoint := modifyBody(request.Body, request.ContentLength)
			request.Body = newBody
			request.ContentLength = int64(newContentLength)
			request.Header.Set(pointCountKey, strconv.Itoa(pointCoint))
		},
		Transport: &metricsCollectingTransport{
			UnderlyingTransport: http.DefaultTransport,
		},
	}

	if config.SelfMetrics != nil {
		go aggregateAndSendMetrics()
	}

	http.HandleFunc("/", func(responseWriter http.ResponseWriter, request *http.Request) {
		proxy.ServeHTTP(responseWriter, request)
	})
	log.Println(fmt.Sprintf("im about to try to listen on port %d and forward to %s://%s!", config.ListenPort, config.BackendScheme, config.BackendHostPort))
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", config.ListenPort), nil))
}

func modifyBody(body io.ReadCloser, contentLength int64) (io.ReadCloser, int, int) {

	if body == nil {
		return nil, 0, 0
	}

	pointsChannel := make(chan PointModel, pointsChannelSize)
	resultBuffer := bytes.NewBuffer(make([]byte, 0, contentLength*2))

	go readPoints(body, pointsChannel)

	incomingPointCount := 0
	outgoingPointCount := 0
	for {
		point, hasMore := <-pointsChannel

		if point.MeasurementName != "" {
			incomingPointCount++
			mutatePointModel(&point)

			if len(point.Fields) > 0 {
				outgoingPointCount++
				writePoint(resultBuffer, &point, hasMore)
			}
		}

		if !hasMore {
			break
		}
	}

	incomingPointsMetricChannel <- incomingPointCount

	return ioutil.NopCloser(resultBuffer), resultBuffer.Len(), outgoingPointCount
}

func mutatePointModel(point *PointModel) {
	for _, rewriter := range config.Rewriters {
		if rewriter.MustHaveTagKey != "" && point.Tags[rewriter.MustHaveTagKey] == nil {
			continue
		}
		if rewriter.MeasurementNameMustMatchCompiled != nil {
			matches := rewriter.MeasurementNameMustMatchCompiled.FindStringSubmatch(point.MeasurementName)
			if matches == nil {
				continue
			}
			point.MeasurementNameMatches = matches
		}
		if rewriter.MeasurementNameMustNotMatchCompiled != nil && rewriter.MeasurementNameMustNotMatchCompiled.MatchString(point.MeasurementName) {
			continue
		}

		rewriter.MatchedPointsMetricChannel <- 1

		evalSelector := func(selector *Selector) *string {
			valueBuffer := bytes.NewBuffer(make([]byte, 0, maximumStringLengthBytes))
			err := selector.ValueTemplateParsed.Execute(valueBuffer, point)
			if err != nil {
				rewriter.SelectorErrorsChannel <- SelectorError{
					Template:     selector.ValueTemplate,
					ErrorMessage: err.Error(),
				}
				return nil
			} else {
				result := valueBuffer.String()
				return &result
			}
		}

		var addFields map[string]*string
		var addTags map[string]*string

		if len(rewriter.AddFields) > 0 {
			addFields = make(map[string]*string)
			for _, selector := range rewriter.AddFields {
				value := evalSelector(selector)
				if value != nil {
					addFields[selector.Key] = value
				}
			}
		}

		if len(rewriter.AddTags) > 0 {
			addTags = make(map[string]*string)
			for _, selector := range rewriter.AddTags {
				value := evalSelector(selector)
				if value != nil {
					addTags[selector.Key] = value
				}
			}
		}

		for _, field := range rewriter.RemoveFields {
			delete(point.Fields, field)
		}
		for _, tag := range rewriter.RemoveTags {
			delete(point.Tags, tag)
		}

		if len(rewriter.AddFields) > 0 {
			for key, value := range addFields {
				if point.Tags[key] != nil || point.Fields[key] != nil {
					rewriter.SelectorErrorsChannel <- SelectorError{
						ErrorMessage: fmt.Sprintf("tag/field '%s' already exists", key),
					}
				} else {
					point.Fields[key] = value
				}
			}
		}

		if len(rewriter.AddTags) > 0 {
			for key, value := range addTags {
				if point.Tags[key] != nil || point.Fields[key] != nil {
					rewriter.SelectorErrorsChannel <- SelectorError{
						ErrorMessage: fmt.Sprintf("tag/field '%s' already exists", key),
					}
				} else {
					point.Tags[key] = value
				}
			}
		}

	}
}

func aggregateAndSendMetrics() {

	//fmt.Println("aggregateAndSendMetrics")

	countPointsAndRequests := func(channel chan int) (int, int) {
		points := 0
		requests := 0

		if channel != nil {
			done := false
			for !done {
				select {
				case c := <-channel:
					points += c
					requests++
				default:
					// break does not work here :\
					done = true
				}
			}
		} else {
			fmt.Println("Error: channel is nil!")
		}

		return points, requests
	}

	incomingPoints, incomingRequests := countPointsAndRequests(incomingPointsMetricChannel)
	outgoingPoints, outgoingRequests := countPointsAndRequests(outgoingPointsMetricChannel)
	droppedPoints, droppedRequests := countPointsAndRequests(droppedPointsMetricChannel)
	incomingPointsTotal += incomingPoints
	outgoingPointsTotal += outgoingPoints
	droppedPointsTotal += droppedPoints
	incomingRequestsTotal += incomingRequests
	outgoingRequestsTotal += outgoingRequests
	droppedRequestsTotal += droppedRequests

	for i, rewriter := range config.Rewriters {
		matchedPoints, _ := countPointsAndRequests(rewriter.MatchedPointsMetricChannel)
		rewriter.TotalMatchedPoints += matchedPoints
		if rewriter.SelectorErrorsChannel != nil {
			done := false
			for !done {
				select {
				case err := <-rewriter.SelectorErrorsChannel:
					rewriter.SelectorErrors[fmt.Sprintf("%s: %s", err.Template, err.ErrorMessage)] += 1
				default:
					// break does not work here :\
					done = true
				}
			}
		} else {
			fmt.Printf("Error: rw %d SelectorErrorsChannel is nil!\n", i)
		}
	}

	if time.Since(lastMetricSent) > config.SelfMetrics.FlushIntervalParsed {
		incomingPointsStr := strconv.Itoa(incomingPointsTotal)
		outgoingPointsStr := strconv.Itoa(outgoingPointsTotal)
		droppedPointsStr := strconv.Itoa(droppedPointsTotal)
		incomingRequestsStr := strconv.Itoa(incomingRequestsTotal)
		outgoingRequestsStr := strconv.Itoa(outgoingRequestsTotal)
		droppedRequestsStr := strconv.Itoa(droppedRequestsTotal)
		incomingPointsTotal = 0
		outgoingPointsTotal = 0
		droppedPointsTotal = 0
		incomingRequestsTotal = 0
		outgoingRequestsTotal = 0
		droppedRequestsTotal = 0

		selfMetricPoint := PointModel{
			MeasurementName: config.SelfMetrics.MetricName,
			Fields: map[string]*string{
				"incomingPoints":   &incomingPointsStr,
				"outgoingPoints":   &outgoingPointsStr,
				"droppedPoints":    &droppedPointsStr,
				"incomingRequests": &incomingRequestsStr,
				"outgoingRequests": &outgoingRequestsStr,
				"droppedRequests":  &droppedRequestsStr,
			},
			Timestamp: time.Now().UnixNano(),
		}

		for i, rewriter := range config.Rewriters {
			matchedPointsString := strconv.Itoa(rewriter.TotalMatchedPoints)
			selfMetricPoint.Fields[fmt.Sprintf("rw%d-%s-matched", i, rewriter.Name)] = &matchedPointsString

			for errorString, count := range rewriter.SelectorErrors {
				countString := strconv.Itoa(count)
				selfMetricPoint.Fields[fmt.Sprintf("rw%d-%s-err: %s", i, rewriter.Name, errorString)] = &countString
			}

			rewriter.TotalMatchedPoints = 0
			rewriter.SelectorErrors = make(map[string]int)
		}

		go func() {
			buffer := bytes.NewBuffer(make([]byte, 0, selfMetricBufferSizeBytes))
			writePoint(buffer, &selfMetricPoint, false)
			response, err := http.Post(fmt.Sprintf("%s://%s/write?db=%s", config.BackendScheme, config.BackendHostPort, config.SelfMetrics.Database), "text/plain", buffer)
			if err != nil {
				fmt.Printf("Error attempting to report self metrics: %s", err)
			} else if response.StatusCode < 200 || response.StatusCode >= 300 {
				fmt.Printf("HTTP %d (%s) attempting to report self metrics.", response.StatusCode, response.Status)
			}
		}()

		lastMetricSent = time.Now()
	}

	time.AfterFunc(metricsChannelFlushInterval, aggregateAndSendMetrics)
}

type metricsCollectingTransport struct {
	UnderlyingTransport http.RoundTripper
}

func (this *metricsCollectingTransport) RoundTrip(request *http.Request) (response *http.Response, err error) {

	pointCount, err := strconv.Atoi(request.Header.Get(pointCountKey))
	outgoingPointsMetricChannel <- pointCount

	response, err = this.UnderlyingTransport.RoundTrip(request)

	if err != nil {
		droppedPointsMetricChannel <- pointCount
	}

	return response, err
}

func readPoints(body io.ReadCloser, pointsChannel chan PointModel) {

	//https://golang.org/pkg/bufio/#SplitFunc  better way of doing this state machine most likely..?
	// lineScanner := bufio.NewScanner(body)
	// for lineScanner.Scan() {
	//     lineScanner.
	//     fmt.Println(scanner.Text())
	// }

	lineProtocolState := MeasurementNameState
	lastCharacterWasEscape := false
	readStringBuffer := make([]byte, maximumStringLengthBytes)
	readStringLength := 0
	var key string
	point := PointModel{
		Fields: make(map[string]*string),
		Tags:   make(map[string]*string),
	}

	bytesBuffer := make([]byte, receiveBufferSizeBytes)
	for {
		bytesRead, err := body.Read(bytesBuffer)

		//fmt.Println("------"+fmt.Sprintf("%d", bytesRead)+"\n\n"+string(bytesBuffer)+"\n\n")

		for i := 0; i < bytesRead; i++ {
			b := bytesBuffer[i]

			if !lastCharacterWasEscape && b == escapeByte {
				lastCharacterWasEscape = true
			} else {
				if lineProtocolState == MeasurementNameState {
					if b == spaceByte && !lastCharacterWasEscape {
						point.MeasurementName = string(readStringBuffer[:readStringLength])
						//fmt.Printf("1 %s \n", point.MeasurementName)
						readStringLength = 0
					} else if b == commaByte && !lastCharacterWasEscape {
						point.MeasurementName = string(readStringBuffer[:readStringLength])
						//fmt.Printf("1 %s \n", point.MeasurementName)
						lineProtocolState = TagKeyState
						readStringLength = 0
					} else if b == spaceByte && !lastCharacterWasEscape {
						point.MeasurementName = string(readStringBuffer[:readStringLength])
						//fmt.Printf("1 %s \n", point.MeasurementName)
						lineProtocolState = FieldKeyState
						readStringLength = 0
					} else {
						readStringBuffer[readStringLength] = b
						readStringLength++
					}
				} else if lineProtocolState == TagKeyState {
					if b == equalsByte && !lastCharacterWasEscape {
						key = string(readStringBuffer[:readStringLength])
						//fmt.Printf("2 %s \n", key)
						readStringLength = 0
						lineProtocolState = TagValueState
					} else {
						readStringBuffer[readStringLength] = b
						readStringLength++
					}
				} else if lineProtocolState == TagValueState {
					if b == commaByte && !lastCharacterWasEscape {
						value := string(readStringBuffer[:readStringLength])
						point.Tags[key] = &value
						//fmt.Printf("3 %s \n", point.Tags[key])
						readStringLength = 0
						lineProtocolState = TagKeyState
					} else if b == spaceByte && !lastCharacterWasEscape {
						value := string(readStringBuffer[:readStringLength])
						point.Tags[key] = &value
						//fmt.Printf("3 %s \n", point.Tags[key])
						readStringLength = 0
						lineProtocolState = FieldKeyState
					} else {
						readStringBuffer[readStringLength] = b
						readStringLength++
					}
				} else if lineProtocolState == FieldKeyState {
					if b == equalsByte && !lastCharacterWasEscape {
						key = string(readStringBuffer[:readStringLength])
						//fmt.Printf("4 %s \n", key)
						readStringLength = 0
						lineProtocolState = FieldValueState
					} else {
						readStringBuffer[readStringLength] = b
						readStringLength++
					}
				} else if lineProtocolState == FieldValueState {
					if b == commaByte && !lastCharacterWasEscape {
						value := string(readStringBuffer[:readStringLength])
						point.Fields[key] = &value
						//fmt.Printf("5 %s \n", point.Fields[key])
						readStringLength = 0
						lineProtocolState = FieldKeyState
					} else if b == spaceByte && !lastCharacterWasEscape {
						value := string(readStringBuffer[:readStringLength])
						point.Fields[key] = &value
						//fmt.Printf("5 %s \n", point.Fields[key])
						readStringLength = 0
						lineProtocolState = TimestampState
					} else {
						readStringBuffer[readStringLength] = b
						readStringLength++
					}
				} else if lineProtocolState == TimestampState {
					if b == newlineByte || (i == bytesRead-1) && err == io.EOF {
						value, err := strconv.ParseInt(string(readStringBuffer[:readStringLength]), 10, 64)
						readStringLength = 0
						if err == nil {
							point.Timestamp = value
							pointsChannel <- point
						}

						lineProtocolState = MeasurementNameState
						point = PointModel{
							Fields: make(map[string]*string),
							Tags:   make(map[string]*string),
						}

					} else {
						readStringBuffer[readStringLength] = b
						readStringLength++
					}
				}
				lastCharacterWasEscape = false
			}
		}

		if err == io.EOF {
			body.Close()
			close(pointsChannel)
			return
		}
	}
}

func writePoint(writer *bytes.Buffer, point *PointModel, hasMore bool) {
	writer.Write([]byte(point.MeasurementName))
	tagsCount := len(point.Tags)
	tagIndex := 0
	if tagsCount > 0 {
		writer.WriteByte(commaByte)
	}
	tagKeys := make([][]byte, 0, len(point.Tags))
	for k := range point.Tags {
		tagKeys = append(tagKeys, []byte(k))
	}
	sortedTagKeys := SortByteSlices(tagKeys)
	for _, k := range sortedTagKeys {
		writer.Write(k)
		writer.WriteByte(equalsByte)
		writer.Write([]byte(*point.Tags[string(k)]))
		tagIndex++
		if tagIndex < tagsCount {
			writer.WriteByte(commaByte)
		}
	}

	writer.WriteByte(spaceByte)
	fieldsCount := len(point.Fields)
	fieldIndex := 0
	for k, v := range point.Fields {
		writer.Write([]byte(k))
		writer.WriteByte(equalsByte)
		writer.Write([]byte(*v))
		fieldIndex++
		if fieldIndex < fieldsCount {
			writer.WriteByte(commaByte)
		}
	}
	writer.WriteByte(spaceByte)
	writer.Write([]byte(strconv.FormatInt(point.Timestamp, 10)))
	if hasMore {
		writer.WriteByte(newlineByte)
	}
}
