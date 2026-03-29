#!/bin/sh
set -eu

echo "[INFO] Bootstrapping Ollama container"
apt-get update
apt-get install -y --no-install-recommends ca-certificates

if [ -f /certs/corp-ca.crt ]; then
  echo "[INFO] Installing custom CA from /certs/corp-ca.crt"
  cp /certs/corp-ca.crt /usr/local/share/ca-certificates/corp-ca.crt
  update-ca-certificates
else
  echo "[WARN] /certs/corp-ca.crt not found. Starting without custom CA."
fi

exec ollama serve
