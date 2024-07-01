# This Dockerfile is infused with magic to speedup the build.
# In particular, it requires built binaries to be present (see COPY directive).

FROM alpine AS chmodder
ARG TARGETARCH
ARG COMPONENT
ARG FEATURE
COPY /artifacts/binaries-$TARGETARCH$FEATURE/$COMPONENT /app/$COMPONENT
RUN chmod +x /app/*

FROM gcr.io/distroless/cc-debian12
ARG COMPONENT
COPY --from=chmodder /app/$COMPONENT /usr/local/bin/samply
ENTRYPOINT [ "/usr/local/bin/fhir2sql" ]
