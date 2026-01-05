#!/bin/bash
#
# Setup cron job for Crunchbase Pro scraper bot.
# Run once to configure, then cron handles daily runs automatically.
#
# USAGE:
#     ./scripts/setup_crunchbase_cron.sh
#
# PREREQUISITES:
#     1. Set CRUNCHBASE_SEARCH_URL in .env (your saved search URL)
#     2. Set BUD_TRACKER_API_KEY in .env (your API key)
#     3. Install dependencies: pip install playwright httpx python-dotenv
#     4. Install Playwright browsers: playwright install chromium
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BOT_SCRIPT="$PROJECT_ROOT/scripts/crunchbase_bot.py"
LOG_FILE="$PROJECT_ROOT/logs/crunchbase_cron.log"

# Detect Python - prefer venv if available
if [ -f "$PROJECT_ROOT/.venv/bin/python" ]; then
    PYTHON="$PROJECT_ROOT/.venv/bin/python"
elif command -v python3 &> /dev/null; then
    PYTHON="python3"
else
    echo "ERROR: Python not found. Install Python 3.9+ or create a .venv"
    exit 1
fi

echo "========================================"
echo "Crunchbase Bot Cron Setup"
echo "========================================"
echo "Project root: $PROJECT_ROOT"
echo "Python: $PYTHON"
echo "Bot script: $BOT_SCRIPT"
echo "Log file: $LOG_FILE"
echo ""

# Create logs directory
mkdir -p "$PROJECT_ROOT/logs"

# Check prerequisites
echo "Checking prerequisites..."

# Check .env file
if [ ! -f "$PROJECT_ROOT/.env" ]; then
    echo "WARNING: .env file not found. Make sure environment variables are set."
fi

# Check CRUNCHBASE_SEARCH_URL
if [ -f "$PROJECT_ROOT/.env" ]; then
    if grep -q "CRUNCHBASE_SEARCH_URL" "$PROJECT_ROOT/.env"; then
        echo "  CRUNCHBASE_SEARCH_URL is set"
    else
        echo "  WARNING: CRUNCHBASE_SEARCH_URL not found in .env"
    fi
fi

# Check Playwright is installed
if $PYTHON -c "import playwright" 2>/dev/null; then
    echo "  Playwright is installed"
else
    echo "  WARNING: Playwright not installed. Run: pip install playwright && playwright install chromium"
fi

echo ""

# Cron expression: 8 AM daily (Mac local time)
# Format: minute hour day-of-month month day-of-week
CRON_TIME="0 8 * * *"

# Build the cron command
# - cd to project root
# - source .env to load environment variables
# - run the bot script
# - append output to log file
CRON_CMD="cd $PROJECT_ROOT && source .env 2>/dev/null; $PYTHON $BOT_SCRIPT >> $LOG_FILE 2>&1"

echo "Cron schedule: $CRON_TIME (8:00 AM daily)"
echo "Cron command: $CRON_CMD"
echo ""

# Add to crontab (remove existing entry first to be idempotent)
(crontab -l 2>/dev/null | grep -v "crunchbase_bot.py") | crontab -
(crontab -l 2>/dev/null; echo "$CRON_TIME $CRON_CMD") | crontab -

echo "Cron job installed successfully!"
echo ""
echo "To verify, run:"
echo "  crontab -l"
echo ""
echo "To view logs:"
echo "  tail -f $LOG_FILE"
echo ""
echo "To test immediately:"
echo "  cd $PROJECT_ROOT && source .env && $PYTHON $BOT_SCRIPT --visible"
echo ""
echo "To run a dry-run (no API calls):"
echo "  cd $PROJECT_ROOT && source .env && $PYTHON $BOT_SCRIPT --dry-run --visible"
echo ""
echo "========================================"
