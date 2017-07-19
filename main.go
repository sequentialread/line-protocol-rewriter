package main

import (
	"encoding/json"
	"fmt"
  "regexp"
  "bytes"
	"io/ioutil"
  "io"
	"log"
	"net/http"
	"net/http/httputil"
	"reflect"
  "text/template"

	"github.com/forestjohnsonpeoplenet/influxStyleEnvOverride"
)

type Config struct {
	ListenPort      int
	BackendHostPort string
	BackendScheme   string
	Rewriters       []LineProtocolRewriter
}

type LineProtocolRewriter struct {
	MeasurementNameMustMatch    string
	MeasurementNameMustNotMatch string
	MustHaveTagKey              string
  MeasurementNameMustMatchParsed    regexp.Regexp
	MeasurementNameMustNotMatchParsed regexp.Regexp
	AddFields                   []Selector
  AddTags                     []Selector
  RemoveFields                []string
  RemoveTags                  []string
}

type Selector struct {
  Key string
  ValueTemplate string
  ValueTemplateParsed template.Template
}

type PointModel struct {
  MeasurementName string
  MeasurementNameMatches []string
  Tags map[string]string
  Fields map[string]string
  Timestamp string
}

const (
        MeasurementNameState = iota
        TagKeyState
        TagValueState
        FieldKeyState
        FieldValueState
        TimestampState
)

var newlineByte = byte('\n')
var escapeByte = byte('\\')
var spaceByte = byte(' ')
var commaByte = byte(',')
var equalsByte = byte('=')

const pointsChannelSize = 10
const receiveBufferSizeBytes = 512
const maximumStringLengthBytes = 512

func main() {
  fmt.Println("Hello from Golang")

	configBytes, err := ioutil.ReadFile("config.json")
	if err != nil {
		panic(err)
	}

	var config = &Config{}
	err = json.Unmarshal(configBytes, config)
	if err != nil {
		panic(err)
	}

	influxStyleEnvOverride.ApplyInfluxStyleEnvOverrides("LPRW", reflect.ValueOf(config))

	proxy := &httputil.ReverseProxy{Director: func(request *http.Request) {
		request.Host = config.BackendHostPort
		request.URL.Host = config.BackendHostPort
		request.URL.Scheme = config.BackendScheme
    newBody, newContentLength := modifyBody(config, request.Body, request.ContentLength)
		request.Body = newBody
		request.ContentLength = int64(newContentLength)
	}}

	http.HandleFunc("/", func(responseWriter http.ResponseWriter, request *http.Request) {
		proxy.ServeHTTP(responseWriter, request)
	})
	log.Println(fmt.Sprintf("im about to try to listen on port %d and forward to %s://%s!", config.ListenPort, config.BackendScheme, config.BackendHostPort))
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", config.ListenPort), nil))
}

//        // Ye olde javascript implementation
//        var influxEscape = (s) => {
//          return String(s).replace(',', '\\,')
//            .replace('=', '\\=')
//            .replace(' ', '\\ ')
//            .replace('"', '\\"');
//        };
//        var nameValueToInfluxKV = (x) => ` ${influxEscape(x.name)}=${influxEscape(x.value)}`;
//        var tagsLineProtocol = this.tags.map(nameValueToInfluxKV).join(',');
//        var metricsLineProtocol = this.metrics.map(nameValueToInfluxKV).join(',');
//        return `${influxEscape(this.measurement)}${tagsLineProtocol.length ? ',' : ''}${tagsLineProtocol} ${metricsLineProtocol} ${dateNs}`;



func modifyBody(config *Config, body io.ReadCloser, contentLength int64) (io.ReadCloser, int) {

	if body == nil {
		return nil, 0
	}

	pointsChannel := make(chan PointModel, pointsChannelSize)
	// Maybe we can avoid allocating a buffer by doing this ? https://stackoverflow.com/questions/2395192/http-headers-for-unknown-content-length
	// Does it matter / is it a good idea?  dont know until you try.
	resultBuffer := bytes.NewBuffer(make([]byte, 0, contentLength*2))

  go readPoints(body, pointsChannel)

	for {
		point, hasMore := <- pointsChannel
		mutatePointModel(config, &point)
		writePoint(resultBuffer, &point)
		if !hasMore {
			break
		}
	}

  return ioutil.NopCloser(resultBuffer), resultBuffer.Len()

  //return io.ReadCloser{
  //  Close: func() error {
  //    return body.Close()
  //  },
  //  Read: func(buffer []byte) (bytesRead int, err error) {
  //
  //  }
  //}
}

func mutatePointModel(config *Config, point *PointModel) {

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
		Fields: make(map[string]string),
	  Tags: make(map[string]string),
	}

	bytesBuffer := make([]byte, receiveBufferSizeBytes)
	for {
		bytesRead, err := body.Read(bytesBuffer)

		//fmt.Println("------"+fmt.Sprintf("%d", bytesRead)+"\n\n"+string(bytesBuffer)+"\n\n")

		for i := 0; i < bytesRead; i++ {
			b := bytesBuffer[i];

			if !lastCharacterWasEscape && b == escapeByte {
	      lastCharacterWasEscape = true;
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
	          readStringLength ++
	        }
	      } else if lineProtocolState == TagKeyState {
	        if b == equalsByte && !lastCharacterWasEscape {
	          key = string(readStringBuffer[:readStringLength])
						//fmt.Printf("2 %s \n", key)
	          readStringLength = 0
	          lineProtocolState = TagValueState
	        } else {
	          readStringBuffer[readStringLength] = b
	          readStringLength ++
	        }
	      } else if lineProtocolState == TagValueState {
	        if b == commaByte && !lastCharacterWasEscape {
	          point.Tags[key] = string(readStringBuffer[:readStringLength])
						//fmt.Printf("3 %s \n", point.Tags[key])
	          readStringLength = 0
	          lineProtocolState = TagKeyState
	        } else if b == spaceByte && !lastCharacterWasEscape {
	          point.Tags[key] = string(readStringBuffer[:readStringLength])
						//fmt.Printf("3 %s \n", point.Tags[key])
	          readStringLength = 0
	          lineProtocolState = FieldKeyState
	        } else {
	          readStringBuffer[readStringLength] = b
	          readStringLength ++
	        }
	      } else if lineProtocolState == FieldKeyState {
	        if b == equalsByte && !lastCharacterWasEscape {
	          key = string(readStringBuffer[:readStringLength])
						//fmt.Printf("4 %s \n", key)
	          readStringLength = 0
	          lineProtocolState = FieldValueState
	        } else {
	          readStringBuffer[readStringLength] = b
	          readStringLength ++
	        }
	      } else if lineProtocolState == FieldValueState {
	        if b == commaByte && !lastCharacterWasEscape {
	          point.Fields[key] = string(readStringBuffer[:readStringLength])
						//fmt.Printf("5 %s \n", point.Fields[key])
	          readStringLength = 0
	          lineProtocolState = FieldKeyState
	        } else if b == spaceByte && !lastCharacterWasEscape {
	          point.Fields[key] = string(readStringBuffer[:readStringLength])
						//fmt.Printf("5 %s \n", point.Fields[key])
	          readStringLength = 0
	          lineProtocolState = TimestampState
	        } else {
	          readStringBuffer[readStringLength] = b
	          readStringLength ++
	        }
	      } else if lineProtocolState == TimestampState {
	        if b == newlineByte || (i == bytesRead-1) && err == io.EOF {
	          point.Timestamp = string(readStringBuffer[:readStringLength])
						//fmt.Printf("6 %s \n", point.Timestamp)
	          readStringLength = 0

	          pointsChannel <- point

						point = PointModel{
							Fields: make(map[string]string),
						  Tags: make(map[string]string),
						}
	          lineProtocolState = MeasurementNameState
	        } else {
	          readStringBuffer[readStringLength] = b
	          readStringLength ++
	        }
	      }
	      lastCharacterWasEscape = false;
	    }
		}

		if err == io.EOF {
			body.Close()
			close(pointsChannel)
			return
		}
	}
}

func writePoint(writer *bytes.Buffer, point *PointModel) {
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
    writer.Write([]byte(point.Tags[string(k)]))
    tagIndex ++
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
    writer.Write([]byte(v))
    fieldIndex ++
    if fieldIndex < fieldsCount {
      writer.WriteByte(commaByte)
    }
  }
  writer.WriteByte(spaceByte)
  writer.Write([]byte(point.Timestamp))
  writer.WriteByte(newlineByte)
}
