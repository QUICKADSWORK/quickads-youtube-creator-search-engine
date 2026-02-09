#!/bin/bash

# YouTube Channel Scraper - Run Script
# This script helps you run the application locally

set -e

echo "=========================================="
echo "  YouTube Channel Scraper"
echo "=========================================="
echo ""

# Check if .env exists
if [ ! -f .env ]; then
    echo "Creating .env file from .env.example..."
    cp .env.example .env
    echo ""
    echo "⚠️  Please edit .env file and add your YouTube API key!"
    echo "   Get one at: https://console.cloud.google.com/apis/credentials"
    echo ""
    exit 1
fi

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt --quiet

echo ""
echo "Starting server..."
echo "Open http://localhost:8000 in your browser"
echo ""

# Run the application
python main.py
