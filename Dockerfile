FROM scratch
COPY templates /templates
COPY bin/main /
ENTRYPOINT ["cat"]
