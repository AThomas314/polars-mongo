#!/bin/bash
set -e

echo "🚀 Starting MongoDB setup..."
URL="https://archive.org/download/mongodb-sample-dataset/sample_data.tar"
# 1. Ensure Docker container is running
if [ ! "$(docker ps -q -f name=polars_mongo_db)" ]; then
    echo "⚠️  polars_mongo_db is not running. Starting it now..."
    docker compose up -d
    # Give Mongo a few seconds to warm up
    sleep 5
fi

# 2. Download samples if they don't exist
if [ ! -d "dump" ]; then
    echo "📥 Downloading all MongoDB sample datasets..."
    curl -L -o samples.tar.gz "$URL"
    echo "📦 Extracting..."
    tar -xzf samples.tar.gz
    rm samples.tar.gz
fi

echo "📦 Restoring all sample databases to Docker..."
docker exec -i polars_mongo_db mongorestore --drop --dir=dump

echo "✅ All samples are now in your DB!"
