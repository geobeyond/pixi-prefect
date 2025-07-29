#!/bin/bash
set -e

# PostgreSQL repository setup
wget -qO- https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/postgresql.gpg > /dev/null
echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" | sudo tee /etc/apt/sources.list.d/postgresql.list > /dev/null

# Caddy repository setup
wget -qO- https://dl.cloudsmith.io/public/caddy/stable/gpg.key | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/caddy.gpg > /dev/null
echo "deb https://dl.cloudsmith.io/public/caddy/stable/deb/debian any-version main" | sudo tee /etc/apt/sources.list.d/caddy.list > /dev/null

# Update package lists
sudo apt update

mkdir -p /tmp/offline-packages/postgresql
POSTGRESQL_PACKAGES=$(cat offline-packages/postgresql-packages-to-download.txt)
pushd /tmp/offline-packages/postgresql
apt download $(echo "$POSTGRESQL_PACKAGES")
popd

mkdir -p /tmp/offline-packages/caddy
CADDY_PACKAGES=$(cat offline-packages/caddy-packages-to-download.txt)
pushd /tmp/offline-packages/caddy
apt download $(echo "$CADDY_PACKAGES")
popd


# Cleanup
sudo rm /etc/apt/sources.list.d/postgresql.list
sudo rm /etc/apt/sources.list.d/caddy.list
sudo rm /etc/apt/trusted.gpg.d/postgresql.gpg
sudo rm /etc/apt/trusted.gpg.d/caddy.gpg
sudo apt update

echo "Done!"
