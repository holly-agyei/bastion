FROM golang:1.22-alpine AS build
WORKDIR /src
COPY autoscaler/go.mod autoscaler/go.sum* ./autoscaler/
RUN cd autoscaler && go mod download
COPY autoscaler ./autoscaler
RUN cd autoscaler && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /out/autoscaler ./cmd/autoscaler

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/autoscaler /autoscaler
ENTRYPOINT ["/autoscaler"]
