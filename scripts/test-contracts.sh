#!/bin/bash
set -e

echo "Running SEZRA-ENGINE contract tests..."
docker compose run --rm contracts-tests