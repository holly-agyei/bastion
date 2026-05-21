FROM golang:1.22-alpine AS build
WORKDIR /src
COPY gateway/go.mod gateway/go.sum ./gateway/
RUN cd gateway && go mod download
COPY gateway ./gateway
COPY proto ./proto
RUN cd gateway && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /out/gateway ./cmd/gateway

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/gateway /gateway
EXPOSE 8080
ENTRYPOINT ["/gateway"]
