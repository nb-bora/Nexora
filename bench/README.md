# Overhead benchmark

```sh
docker compose -f bench/compose.yml up --build --abort-on-container-exit --exit-code-from bench
```

Measures the latency a gateway adds to a non-streaming completion, against an instant mock upstream, client / mock / gateway on the same Docker network. The run aborts as **invalid** if any response is not a 200.

To compare another gateway, add it to `EXTRA` (JSON array of `{name, host, port, headers, model}`) and to `compose.yml`, pin its image by digest, and keep the same machine. Do not publish a ranking from a single run on a laptop: record CPU, Docker version and image digests, and repeat.

What it does not measure: peak throughput (the Node client saturates first), real provider latency, streaming time-to-first-token, memory over time.
