#!/bin/bash
# The Bud Tracker - Quick Start Script

set -e

echo "🌱 Starting The Bud Tracker setup..."

# Check Python version
python_version=$(python3 --version 2>&1 | grep -oP '3\.\d+')
if [[ "${python_version}" < "3.11" ]]; then
    echo "❌ Python 3.11+ required. Found: $python_version"
    exit 1
fi

# Create virtual environment
if [ ! -d ".venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv .venv
fi

# Activate
source .venv/bin/activate

# Install dependencies
echo "📥 Installing dependencies..."
pip install -r requirements.txt

# Install Playwright browsers
echo "🎭 Installing Playwright browsers..."
playwright install chromium

# Start database
echo "🐘 Starting PostgreSQL..."
docker-compose up -d db

# Wait for DB
echo "⏳ Waiting for database..."
sleep 5

# Run migrations
echo "🔄 Running database migrations..."
alembic upgrade head

echo ""
echo "✅ Setup complete!"
echo ""
echo "To start the API server:"
echo "  source .venv/bin/activate"
echo "  uvicorn src.main:app --reload"
echo ""
echo "To run tests:"
echo "  pytest tests/ -v"
echo ""
