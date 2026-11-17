# sentry-agent

`sentry-agent` is a small HTTP sidecar that accepts Sentry SDK envelopes locally, queues them in RAM, and sends them to Sentry in the background. A temporary Sentry outage therefore need not delay an application's request while queue capacity remains available. It is written with the Go standard library and has no runtime dependencies.

> **Volatile queue:** v1 stores every queued and in-flight envelope only in memory. A process exit, restart, or pod replacement loses them. HTTP 200 means accepted into RAM, not delivered to Sentry.

## Quick start

```sh
go run -buildvcs=false ./cmd/sentry-agent --help
go run -buildvcs=false ./cmd/sentry-agent
```

Configure your SDK transport to POST the original envelope bytes to `http://127.0.0.1:5148/api/<project-id>/envelope/`. The envelope's first JSON header line must contain the original DSN. Keep the application's real Sentry DSN in the SDK; only change its transport target. The project ID in the local URL must equal the DSN project ID. `Content-Encoding: gzip` is supported. SDKs that omit the DSN from the envelope header need a small transport adapter to add it; merely replacing the DSN host with localhost loses the upstream destination and is insufficient for multi-DSN routing.

Example envelope:

```text
{"dsn":"https://PUBLIC_KEY@o123.ingest.sentry.io/42"}
{"type":"event"}
{"event_id":"..."}
```

The agent returns HTTP 200 after accepting a valid request into the queue. It forwards the unchanged body to the DSN's `/api/42/envelope/` endpoint and sets Sentry ingestion authentication using the DSN public key. It never forwards arbitrary incoming headers.

### PHP SDK example

Install the [Sentry PHP SDK](https://github.com/getsentry/sentry-php) with `composer require sentry/sentry`, start the Go sidecar, then configure a custom SDK HTTP client:

```php
<?php

require __DIR__ . '/vendor/autoload.php';

use Sentry\HttpClient\HttpClientInterface;
use Sentry\HttpClient\Request;
use Sentry\HttpClient\Response;
use Sentry\Options;

$localClient = new class implements HttpClientInterface {
    public function sendRequest(Request $request, Options $options): Response
    {
        $dsn = $options->getDsn();
        $body = $request->getStringBody();
        if ($dsn === null || $body === null) {
            return new Response(0, [], 'Missing DSN or envelope body');
        }

        // PHP SDK client reports can have an empty envelope header. The Go
        // sidecar needs the original DSN there to choose the upstream.
        $parts = explode("\n", $body, 2);
        $header = json_decode($parts[0], true);
        if (!isset($parts[1]) || !is_array($header)) {
            return new Response(0, [], 'Invalid envelope header');
        }
        if (!isset($header['dsn'])) {
            $header['dsn'] = (string) $dsn;
            $firstLine = json_encode($header, JSON_UNESCAPED_SLASHES);
            if ($firstLine === false) {
                return new Response(0, [], 'Could not encode envelope header');
            }
            $body = $firstLine . "\n" . $parts[1];
        }

        $url = 'http://127.0.0.1:5148/api/' . $dsn->getProjectId() . '/envelope/';
        $curl = curl_init($url);
        if ($curl === false) {
            return new Response(0, [], 'Could not initialize cURL');
        }
        curl_setopt_array($curl, [
            CURLOPT_POST => true,
            CURLOPT_POSTFIELDS => $body,
            CURLOPT_HTTPHEADER => [
                'Content-Type: application/x-sentry-envelope',
                'X-Sentry-Auth: Sentry sentry_version=7, sentry_key=' . $dsn->getPublicKey(),
            ],
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_CONNECTTIMEOUT_MS => 200,
            CURLOPT_TIMEOUT_MS => 1000,
            CURLOPT_FOLLOWLOCATION => false,
        ]);
        $result = curl_exec($curl);
        $status = (int) curl_getinfo($curl, CURLINFO_HTTP_CODE);
        $error = $result === false ? curl_error($curl) : '';
        curl_close($curl);

        return new Response($status, [], $error);
    }
};

Sentry\init([
    'dsn' => getenv('SENTRY_DSN'), // e.g. https://PUBLIC_KEY@o123.ingest.sentry.io/42
    'http_client' => $localClient,
]);

Sentry\captureException(new RuntimeException('Example error'));
```

Set `SENTRY_DSN` to the real project DSN. The example uses each SDK client's DSN to select its local path, so separate clients can send to different projects through one sidecar. It uses the Go agent's **HTTP** endpoint; the PHP reference agent's `AgentClientBuilder` speaks a different local protocol and cannot be substituted here. The PHP cURL extension is required.

## Configuration

CLI flags override environment variables, which override defaults. Invalid explicit values fail startup. Every operational setting can be set through the `SENTRY_AGENT_` environment prefix.

| CLI flag | Environment variable | Default | Purpose |
| --- | --- | --- | --- |
| `--listen` | `SENTRY_AGENT_LISTEN` | `127.0.0.1:5148` | Local HTTP address |
| `--allowed-hosts` | `SENTRY_AGENT_ALLOWED_HOSTS` | `sentry.io,*.sentry.io` | Comma-separated allowed DSN hostnames; `*.example.com` matches subdomains |
| `--upstream-concurrency` | `SENTRY_AGENT_UPSTREAM_CONCURRENCY` | `10` | Delivery workers |
| `--upstream-timeout` | `SENTRY_AGENT_UPSTREAM_TIMEOUT` | `5s` | Per-delivery timeout |
| `--retry-interval` | `SENTRY_AGENT_RETRY_INTERVAL` | `5s` | Fixed retry delay |
| `--queue-limit` | `SENTRY_AGENT_QUEUE_LIMIT` | `1000` | Maximum queued plus in-flight items |
| `--queue-bytes` | `SENTRY_AGENT_QUEUE_BYTES` | `64MB` | Maximum queued plus in-flight body bytes |
| `--overflow-policy` | `SENTRY_AGENT_OVERFLOW_POLICY` | `drop-oldest` | `drop-oldest` or `reject-new` |
| `--max-request-size` | `SENTRY_AGENT_MAX_REQUEST_SIZE` | `20MB` | Maximum incoming compressed or plain body size |
| `--log-level` | `SENTRY_AGENT_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

Sizes use binary KB/MB/GB units. Durations use Go syntax such as `250ms`, `5s`, or `1m`. The 20MB default is a sidecar memory budget, not Sentry's maximum possible envelope size; increase both request and queue byte limits if your SDK sends large attachments.

## Delivery behavior

The queue is bounded by both item count and body bytes, including in-flight deliveries. With `drop-oldest`, the oldest waiting item is discarded to admit a new one and the drop is logged. An active delivery is never evicted; if only in-flight items occupy the needed capacity, admission returns 503. With `reject-new`, existing items are retained and a full queue returns 503. Request bodies over the configured limit return 413. Other invalid envelope requests return 400 or 415.

Workers send concurrently through one shared HTTP transport. Network errors, timeouts, 408, 429, and 5xx are retried after the **same fixed interval on every attempt**. Most other 4xx and redirects are permanent failures. A Sentry `Retry-After` or `X-Sentry-Rate-Limits` duration can extend the interval and pauses other queued envelopes for the same endpoint. Because envelopes are opaque and can contain mixed item types, category-specific limits conservatively delay the entire envelope. A bare 429 waits at least 60 seconds. Delivery errors and queue drops are logged without bodies, DSNs, or authentication values.

`/healthz` and `/readyz` return 200 while the HTTP server is serving; neither checks Sentry availability. `sentry-agent probe healthz` and `sentry-agent probe readyz` check those routes over loopback for Kubernetes exec probes, which allows the listener to stay local. On SIGTERM or SIGINT, the server stops admission and the workers stop. The agent does not drain or persist the queue. Keep SDK fallback or another durable path if loss on restart is unacceptable.

## Security model

The default listener binds to loopback for trusted same-pod callers. The agent accepts an upstream DSN from each envelope, so the host allowlist is essential SSRF protection. The default allows Sentry SaaS hosts only and requires HTTPS for them. Add exact self-hosted domains through `--allowed-hosts`; HTTP is possible for explicit non-Sentry hosts, so use it only on trusted networks. The agent validates the project path and public key, rejects DSN passwords, rejects arbitrary URL schemes, does not follow redirects, and sends only Sentry-specific headers. Restrict pod network access and keep the allowlist narrow; an operator-allowed hostname that resolves to an internal service can still reach that service.

## Docker and Kubernetes

Build with `docker build -t sentry-agent .`. The multistage image runs a static, non-root binary on a distroless base with CA certificates for HTTPS. The [sidecar example](examples/kubernetes-sidecar.yaml) shows local communication, resource limits, probes, and signal handling. Queue contents disappear whenever the sidecar container restarts. No volume is used.