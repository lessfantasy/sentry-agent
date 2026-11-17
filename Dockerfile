FROM golang:1.27.1-trixie AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w -buildid=' -o /out/sentry-agent ./cmd/sentry-agent

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/sentry-agent /sentry-agent
EXPOSE 5148
ENTRYPOINT ["/sentry-agent"]
