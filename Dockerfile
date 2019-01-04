# Lightweight alpine OS, weight only 5mb, everything else is Go # # environment.
FROM golang:1.9.2

# Setup environment variables.
ENV GCS_BUCKET socialradar-post-images
ENV ES_URL http://35.196.164.154:9200

# Define (create) working directory within this docker image.
WORKDIR /go/src/github.com/cyyy24/social-radar


# Add (all) files from local directory to inside the docker image.
ADD . /go/src/github.com/cyyy24/social-radar

# Install dependencies within the container.
RUN go get -v \
cloud.google.com/go/bigtable \
cloud.google.com/go/storage \
github.com/auth0/go-jwt-middleware \
github.com/dgrijalva/jwt-go \
github.com/go-redis/redis \
github.com/gorilla/mux \
github.com/olivere/elastic \
github.com/pborman/uuid \
github.com/pkg/errors \
golang.org/x/oauth2/google

# Tell the container to open port 8080.
EXPOSE 8080

# Entrypoint (start the program).
CMD ["/usr/local/go/bin/go", "run", "main.go" ]