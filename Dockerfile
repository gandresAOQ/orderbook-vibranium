# --- build stage ---
FROM golang:1.26-alpine AS build
WORKDIR /src
# Copy manifests first so dependency download is cached independently of source.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Static binary, no CGO, so it runs on a distroless/scratch image.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/orderbook ./cmd/orderbook

# --- runtime stage ---
FROM gcr.io/distroless/static-debian12
COPY --from=build /bin/orderbook /orderbook
EXPOSE 3000
USER nonroot:nonroot
ENTRYPOINT ["/orderbook"]
