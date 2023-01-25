#!/bin/sh

curl -X POST http://elasticsearch:9200/_license/start_trial?acknowledge=true

curl -X PUT http://elasticsearch:9200/test_index/ \
  -H "Content-Type: application/json" \
  --data-binary "@/tmp/test-index.json"

curl -X PUT http://elasticsearch:9200/test_index/_bulk/ \
  -H "Content-Type: application/json" \
  --data-binary "@/tmp/test-data.json"