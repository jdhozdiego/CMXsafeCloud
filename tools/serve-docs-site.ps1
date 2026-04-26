param(
    [string]$RepoRoot = "C:\Users\el_de\Documents\New project\CMXsafeMAC-IPv6",
    [int]$Port = 8000
)

$ErrorActionPreference = "Stop"

& (Join-Path $RepoRoot "tools\sync-docs-api.ps1") -RepoRoot $RepoRoot
& (Join-Path $RepoRoot "tools\generate-manifest-inventory.ps1") -RepoRoot $RepoRoot
& (Join-Path $RepoRoot "tools\generate-python-reference-pages.ps1") -RepoRoot $RepoRoot

docker run --rm -it `
    -p "${Port}:8000" `
    -v "${RepoRoot}:/work" `
    -w /work `
    python:3.12-slim `
    bash -lc "pip install -q -r docs/requirements-mkdocs.txt && mkdocs serve -a 0.0.0.0:8000"
