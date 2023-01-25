#!/bin/sh

curl -u elastic:passwd -X POST http://elasticsearch:9200/_license/start_trial?acknowledge=true

curl -X PUT http://elasticsearch:9200/test_index/ \
  -u elastic:passwd \
  -H "Content-Type: application/json" \
  --data-binary "@/tmp/test-index.json"

curl -X PUT http://elasticsearch:9200/test_index/_bulk/ \
  -u elastic:passwd \
  -H "Content-Type: application/json" \
  --data-binary "@/tmp/test-data.json"
