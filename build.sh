#!/bin/bash

if [ -z $1 ]; then
  echo "missing first argument: tag"
  exit 1
fi

/usr/local/go/bin/go build -o line-protocol-rewriter main.go sortByteSlices.go \
 && docker build -t sequentialread/line-protocol-rewriter:$1 . \
 && docker push sequentialread/line-protocol-rewriter:$1
