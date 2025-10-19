FROM golang:1.24 AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY proto ./proto
COPY DB2 ./
RUN go build -o DB2 .

FROM gcr.io/distroless/base-debian12
WORKDIR /app
COPY --from=builder /app/DB2 .
CMD ["./DB2"]