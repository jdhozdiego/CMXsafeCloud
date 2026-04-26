param(
    [string]$RepoRoot = "C:\Users\el_de\Documents\New project\CMXsafeMAC-IPv6"
)

$ErrorActionPreference = "Stop"

docker run --rm `
    -v "${RepoRoot}:/work" `
    -w /work `
    python:3.12-slim `
    python tools/generate-python-reference-pages.py
