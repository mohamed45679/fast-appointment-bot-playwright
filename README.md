# Playwright Appointment Booker

A high-performance Playwright automation script that monitors appointment availability and books the first matching slot as fast as possible.

## Features
- Fast slot monitoring and selection
- One-shot (bulk) form filling (inputs + dropdowns) for speed
- CAPTCHA image prefetch (base64) in parallel with form filling
- Optional external CAPTCHA solver support (configured via environment variables)
- CSV/console logging to help measure performance bottlenecks

## Requirements
- Python 3.10+ (recommended)
- Playwright (Chromium)
- A valid `.env` file with your configuration

## Installation
```bash
pip install -r requirements.txt
playwright install
