# Storage Price Scraper

A Python web scraper for collecting storage unit pricing data from unitsstorage.com.

## Features

- MongoDB integration for data persistence
- Proxy rotation for request distribution
- Random user data generation
- Retry logic with exponential backoff
- Progress tracking and resume capability
- Multiple export formats

## Installation

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Ensure MongoDB is running locally

## Usage

1. Prepare a CSV file with zip codes (`us_zip_codes_5000.csv`)
2. Run: `python scraper.py`

## Configuration

Modify the `StoragePriceScraper` initialization parameters for different MongoDB configurations.

## License

MIT