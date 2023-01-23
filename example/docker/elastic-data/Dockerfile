FROM alpine:3.17.1

RUN apk update && apk add curl

COPY test-index.json /tmp/
COPY test-data.json /tmp/
COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
