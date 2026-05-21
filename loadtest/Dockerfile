FROM grafana/k6:0.51.0
COPY loadtest/load.js /loadtest/load.js
# Default ENTRYPOINT in the base image is `k6`. Passing `run …` as args
# lets callers append --summary-export / --out / --vus etc. cleanly.
ENTRYPOINT ["k6"]
CMD ["run","/loadtest/load.js"]
