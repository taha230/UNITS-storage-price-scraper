
<div align="center" style="margin-bottom:30px;">
  <img width="500" alt="UNITS price scrapping using python" src="https://github.com/user-attachments/assets/e5c02d4e-e3fe-40d7-9bcd-a9b7ac940878" />
</div>
<br><br> <!-- add more <br> if you need more space -->


# UNITS Storage Price Scraper


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
2. Run: `python UNITS_storage_price_scraper.py`

## Configuration

Modify the `StoragePriceScraper` initialization parameters for different MongoDB configurations.


## 📬 Contact
**Developer:** Taha Hamedani  
**Email:** taha.hamedani8@gmail.com  
