# Corresponding source

The canonical source for this GNU GPL version 3 component is https://github.com/sehoon123/mcp-proxy. A distributed
proxy JAR identifies its exact source commit and checksum through the containing Independent MCP Bridge release's
`mcp-proxy-source.txt` and `SOURCE_IDENTITY.json`.

To reproduce a recorded proxy commit with JDK 21:

```bash
git clone https://github.com/sehoon123/mcp-proxy.git
cd mcp-proxy
git checkout --detach <full-recorded-commit-sha>
./gradlew clean test shadowJar writeRuntimeComponents --no-build-cache
```

The generated JAR and runtime component report must match the hashes recorded by the containing extension release.
