#!/bin/sh
# Generate a self-signed TLS certificate for local development.
#
# Usage: bash nginx/generate_certs.sh
#
# For production: replace nginx/ssl/cert.pem and nginx/ssl/key.pem with
# certificates signed by a trusted CA (e.g. Let's Encrypt / certbot).
#
# The generated cert covers:
#   - DNS: localhost
#   - IP:  127.0.0.1
#
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SSL_DIR="$SCRIPT_DIR/ssl"

mkdir -p "$SSL_DIR"

# Use Docker to avoid path-mangling on Windows Git Bash
docker run --rm -v "$SSL_DIR:/ssl" alpine sh -c \
    "apk add --no-cache openssl -q && \
     openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
         -keyout /ssl/key.pem -out /ssl/cert.pem \
         -subj '/C=US/ST=Dev/L=Dev/O=SDA-System/CN=localhost' \
         -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' && \
     chmod 600 /ssl/key.pem"

chmod 600 "$SSL_DIR/key.pem"
chmod 644 "$SSL_DIR/cert.pem"

echo ""
echo "Self-signed certificate generated:"
echo "  Certificate : $SSL_DIR/cert.pem"
echo "  Private key : $SSL_DIR/key.pem"
echo ""
echo "WARNING: This certificate is for local development only."
echo "         Replace with a CA-signed certificate before going to production."
