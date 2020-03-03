FROM spritsail/alpine-cmake:latest

LABEL "com.github.actions.name"="rust-codecov-action"
LABEL "com.github.actions.description"="Uploading rust codecoverage to github action"
LABEL "com.github.actions.icon"="umbrella"
LABEL "com.github.actions.color"="red"

RUN apk add --no-cache ca-certificates wget zlib-dev binutils-dev && update-ca-certificates

ADD entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]