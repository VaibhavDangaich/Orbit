# Multi-stage: the build stage has the full Go toolchain (~800MB); the
# final image has none of it, just the one static binary. distroless's
# static base has no shell, no package manager, not even libc -- nothing
# for an attacker to do with a shell they'd have to bring themselves, and
# nothing here for a CVE scanner to flag except the binary's own deps.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# CGO_ENABLED=0: a pure-static binary, which is what makes running it on
# a base image with no libc (distroless/static) possible at all.
RUN CGO_ENABLED=0 go build -o /out/scheduler ./cmd/scheduler

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/scheduler /scheduler
ENTRYPOINT ["/scheduler"]
