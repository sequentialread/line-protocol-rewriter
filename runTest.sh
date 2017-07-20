#!/bin/bash

./line-protocol-rewriter &
line_protocol_rewriter_pid=$!

/usr/local/go/bin/go run hello_main.go > test-result.txt &
hello_main_pid=$!

sleep 1

curl -X POST --data-binary @line-protocol.txt  localhost:8086/write

sleep 1

kill $line_protocol_rewriter_pid
pkill -TERM -P $hello_main_pid

echo '

-------------

'
cat test-result.txt

echo '

-------------

'

diff test-result.txt line-protocol.txt

echo '

-------------

'

expected=$(cat line-protocol.txt)
actual=$(cat test-result.txt)

rm -f test-result.txt

if [ "$expected" = "$actual" ]; then
  echo "Test Passed!!"
else
  echo "Test Failed!!"
  exit 1
fi
