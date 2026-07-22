# MCP Proxy

A transparent **stdio ↔ Streamable HTTP** transport bridge for MCP.

This project is the compatibility component embedded in the Burp MCP extension. End users normally install only
`burp-mcp-all.jar`; the extension extracts this proxy when a client supports stdio but cannot connect to Streamable
HTTP directly.

## Architecture

```text
stdio-only MCP client  <──stdio──>  mcp-proxy  <──Streamable HTTP──>  Burp /mcp
```

Clients with native Streamable HTTP support should skip the proxy and connect directly to:

```text
http://127.0.0.1:9876/mcp
```

The proxy relays JSON-RPC methods and parameters without maintaining a proxy-side method allowlist. Negotiated
capabilities, request IDs, progress, cancellation, sampling, and elicitation pass through for protocol result shapes
supported by the pinned MCP SDK. Unknown methods are forwarded, but an unknown response shape is reported as an error
instead of being retried or silently altered.

## Reliability

- Buffers the initial stdio handshake while Burp is still starting.
- Recreates and initializes the Streamable HTTP session after Burp restarts.
- Bounds normal request execution to 16 concurrent HTTP operations with a 64-request queue; excess requests receive
  an immediate not-forwarded error so lifecycle and cancellation messages remain responsive.
- Retries any request after send only when the server confirms the old session was not found, or the TCP connection
  was refused before delivery. Ambiguous failures are never retried, including for unknown future methods.
- Ordinary HTTP requests have no artificial execution timeout, allowing long-running Burp operations.

## Requirements

- JDK 21 or newer
- Gradle wrapper included

## Build

```bash
./gradlew shadowJar
```

The executable JAR is written under `build/libs/`.

## Usage

```bash
export BURP_MCP_BEARER_TOKEN='<copy the token from Burp MCP settings>'
java -jar mcp-proxy-all.jar \
  --mcp-url http://127.0.0.1:9876/mcp \
  --bearer-token-env BURP_MCP_BEARER_TOKEN
```

The default endpoint is `http://127.0.0.1:9876/mcp`, so the URL argument can usually be omitted. To prevent bearer-token
exfiltration, the proxy accepts only numeric loopback hosts (`127.0.0.1` or `::1`) and the exact `/mcp` path. The Burp
extension's installer configures the environment variable automatically. `--bearer-token <token>` is available for
launchers that cannot set environment variables, but the environment option avoids exposing the credential in process listings.

### Migration from the legacy proxy

Existing configurations using the old option continue to work:

```bash
java -jar mcp-proxy-all.jar \
  --sse-url http://127.0.0.1:9876
```

`--sse-url` is a deprecated compatibility alias. A root URL is converted to `/mcp`, and all communication uses
Streamable HTTP; the proxy no longer connects to the deprecated two-endpoint HTTP+SSE transport.

## Testing

```bash
./gradlew test
```

Integration tests cover direct tool calls, delayed Burp startup, session recovery after a restart, and a
server-initiated sampling request relayed back through stdio.
