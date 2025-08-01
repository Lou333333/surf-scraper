import os
import requests
import schedule
import time
from datetime import datetime, date
from supabase import create_client, Client

# Don't use load_dotenv() in production/Railway - variables are already available
# Only load dotenv if running locally (when .env file exists)
try:
    from dotenv import load_dotenv
    if os.path.exists('.env'):
        load_dotenv()
        print("🔍 Debug: Loading from .env file (local development)")
    else:
        print("🔍 Debug: Using Railway environment variables")
except ImportError:
    print("🔍 Debug: python-dotenv not available, using system environment")

# Debug environment loading
print("🔍 Debug: Environment loading in Railway")
print(f"NEXT_PUBLIC_SUPABASE_URL found: {bool(os.getenv('NEXT_PUBLIC_SUPABASE_URL'))}")
print(f"SUPABASE_SERVICE_KEY found: {bool(os.getenv('SUPABASE_SERVICE_KEY'))}")
print(f"WILLY_WEATHER_API_KEY found: {bool(os.getenv('WILLY_WEATHER_API_KEY'))}")

# Get environment variables
SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
WILLY_WEATHER_API_KEY = os.getenv("WILLY_WEATHER_API_KEY")

# Validate required environment variables
if not SUPABASE_URL:
    raise ValueError("NEXT_PUBLIC_SUPABASE_URL environment variable is required")
if not SUPABASE_KEY:
    raise ValueError("SUPABASE_SERVICE_KEY environment variable is required")
if not WILLY_WEATHER_API_KEY:
    raise ValueError("WILLY_WEATHER_API_KEY environment variable is required")

print("✅ All required environment variables found")

# Initialize Supabase client with SERVICE KEY
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://api.willyweather.com.au/v2"

# Australian surf locations from WillyWeather
AUSTRALIAN_SURF_LOCATIONS = {
    # New South Wales
    "Sydney": {"location_id": 4950, "state": "NSW"},
    "Central Coast": {"location_id": 4934, "state": "NSW"},
    "Newcastle": {"location_id": 4988, "state": "NSW"},
    "Mid North Coast": {"location_id": 5049, "state": "NSW"},
    "Byron Bay": {"location_id": 4947, "state": "NSW"},
    "Wollongong": {"location_id": 17663, "state": "NSW"},
    "South Coast": {"location_id": 4923, "state": "NSW"},
    "Far North Coast": {"location_id": 4947, "state": "NSW"},
    
    # Queensland  
    "Gold Coast": {"location_id": 4958, "state": "QLD"},
    "Sunshine Coast": {"location_id": 5238, "state": "QLD"},
    "Fraser Coast": {"location_id": 5251, "state": "QLD"},
    "Capricorn Coast": {"location_id": 4929, "state": "QLD"},
    "Mackay": {"location_id": 4983, "state": "QLD"},
    "Townsville": {"location_id": 5085, "state": "QLD"},
    "Cairns": {"location_id": 4929, "state": "QLD"},
    
    # Victoria
    "Melbourne": {"location_id": 4994, "state": "VIC"},
    "Torquay": {"location_id": 5083, "state": "VIC"},
    "Phillip Island": {"location_id": 5016, "state": "VIC"},
    "East Gippsland": {"location_id": 4956, "state": "VIC"},
    "West Coast": {"location_id": 5095, "state": "VIC"},
    
    # South Australia
    "Adelaide": {"location_id": 4909, "state": "SA"},
    "Fleurieu Peninsula": {"location_id": 4957, "state": "SA"},
    "Yorke Peninsula": {"location_id": 5107, "state": "SA"},
    "Eyre Peninsula": {"location_id": 4955, "state": "SA"},
    "Kangaroo Island": {"location_id": 4972, "state": "SA"},
    
    # Western Australia
    "Perth": {"location_id": 5017, "state": "WA"},
    "Margaret River": {"location_id": 4987, "state": "WA"},
    "Geraldton": {"location_id": 4959, "state": "WA"},
    "Esperance": {"location_id": 4954, "state": "WA"},
    "Albany": {"location_id": 4913, "state": "WA"},
    "Exmouth": {"location_id": 4955, "state": "WA"},
    "Broome": {"location_id": 4927, "state": "WA"}
}

class WillyWeatherScraper:
    def __init__(self):
        self.api_key = WILLY_WEATHER_API_KEY
        self.base_url = BASE_URL
        
    def get_forecast_data(self, location_id, region_name):
        """Get forecast data from WillyWeather API"""
        try:
            print(f"🌊 Fetching forecast for {region_name} (ID: {location_id})")
            
            # API endpoint for weather forecast
            url = f"{self.base_url}/{self.api_key}/locations/{location_id}/weather.json"
            
            # Parameters for swell, wind, and tide forecasts
            params = {
                'forecasts': 'swell,wind,tides',
                'days': 3,
                'startDate': date.today().strftime('%Y-%m-%d')
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            print(f"✅ Successfully fetched data for {region_name}")
            
            # Process the forecast data
            forecast_records = self.process_forecast_data(data, region_name)
            return forecast_records
            
        except requests.RequestException as e:
            print(f"❌ API request failed for {region_name}: {str(e)}")
            return None
        except Exception as e:
            print(f"❌ Error processing {region_name}: {str(e)}")
            return None
    
    def process_forecast_data(self, data, region_name):
        """Process raw API data into forecast records"""
        try:
            forecast_records = []
            forecasts = data.get('forecasts', {})
            
            # ✅ FIXED: Better error handling for missing forecast data
            if not forecasts:
                print(f"⚠️  No forecasts found in API response for {region_name}")
                return []
            
            # Get swell and wind data with safe handling
            swell_data = forecasts.get('swell')
            wind_data = forecasts.get('wind')
            
            # Check if swell data exists and has the right structure
            if not swell_data or not isinstance(swell_data, dict) or not swell_data.get('days'):
                print(f"⚠️  No valid swell data found for {region_name}")
                return []
                
            swell_days = swell_data.get('days', [])
            wind_days = wind_data.get('days', []) if wind_data else []
            
            for day_idx, day in enumerate(swell_days[:3]):  # Process 3 days
                forecast_date = day.get('dateTime')
                entries = day.get('entries', [])
                
                # Get corresponding wind data for the same day
                wind_entries = []
                if day_idx < len(wind_days):
                    wind_entries = wind_days[day_idx].get('entries', [])
                
                for entry_idx, entry in enumerate(entries):
                    try:
                        # Get wind data for the same time
                        wind_entry = {}
                        if entry_idx < len(wind_entries):
                            wind_entry = wind_entries[entry_idx]
                        
                        # Create forecast record
                        record = {
                            'break_id': f"{region_name.lower().replace(' ', '_')}_{entry.get('dateTime', '')}",
                            'region': region_name,  # ✅ This will work once you add the column
                            'forecast_date': forecast_date,
                            'forecast_time': entry.get('dateTime'),
                            'time_period': entry.get('dateTime'),
                            'swell_height': entry.get('height'),
                            'swell_period': entry.get('period'),
                            'swell_direction': entry.get('direction'),
                            'wind_speed': wind_entry.get('speed'),
                            'wind_direction': wind_entry.get('direction'),
                            'created_at': datetime.now().isoformat(),
                            'updated_at': datetime.now().isoformat()
                        }
                        
                        forecast_records.append(record)
                        
                    except Exception as e:
                        print(f"⚠️  Error processing entry {entry_idx} for {region_name}: {str(e)}")
                        continue
            
            print(f"📊 Processed {len(forecast_records)} forecast records for {region_name}")
            return forecast_records
            
        except Exception as e:
            print(f"❌ Error processing forecast data for {region_name}: {str(e)}")
            return []

def get_all_breaks_to_scrape():
    """Get unique regions from the database that need scraping"""
    try:
        print("🔍 Getting surf breaks to scrape from database...")
        
        response = supabase.table('surf_breaks').select('region').execute()
        
        if response.data:
            # Get unique regions
            unique_regions = list(set([break_data['region'] for break_data in response.data]))
            print(f"📍 Found {len(unique_regions)} unique regions in database: {unique_regions}")
            return unique_regions
        else:
            print("⚠️  No surf breaks found in database")
            return []
            
    except Exception as e:
        print(f"❌ Error getting breaks from database: {str(e)}")
        return []

def save_forecast_data(forecast_records, region_name):
    """Save forecast records to Supabase"""
    if not forecast_records:
        print(f"⚠️  No forecast data to save for {region_name}")
        return
    
    try:
        print(f"💾 Saving {len(forecast_records)} forecast records for {region_name}...")
        
        total_saved = 0
        batch_size = 50
        
        # Process in batches
        for i in range(0, len(forecast_records), batch_size):
            batch = forecast_records[i:i + batch_size]
            
            # Use upsert to handle duplicates
            result = supabase.table('forecast_data').upsert(batch,
                on_conflict='break_id, forecast_date, forecast_time'
            ).execute()
            
            total_saved += len(batch)
                
        print(f"✅ Saved {total_saved} forecast records for {region_name}")
        print(f"📊 Sample data: {forecast_records[0] if forecast_records else 'No data'}")
        
    except Exception as e:
        print(f"❌ Error saving forecast data for {region_name}: {str(e)}")

def run_scraper():
    """Main scraper function"""
    print(f"\n🌊 Starting WillyWeather scraper run at {datetime.now()}")
    
    try:
        scraper = WillyWeatherScraper()
        print("✅ Scraper initialized successfully")
        
        # Get all regions to scrape
        regions_to_scrape = get_all_breaks_to_scrape()
        
        if not regions_to_scrape:
            print("❌ No regions found to scrape")
            return
        
        print(f"📍 Found {len(regions_to_scrape)} unique regions to scrape: {regions_to_scrape}")
        
        # Scrape each region
        successful_scrapes = 0
        for region_name in regions_to_scrape:
            try:
                print(f"\n--- Processing {region_name} ---")
                
                location_info = AUSTRALIAN_SURF_LOCATIONS.get(region_name)
                if not location_info:
                    print(f"⚠️  No location ID found for {region_name}")
                    continue
                
                location_id = location_info['location_id']
                forecast_records = scraper.get_forecast_data(location_id, region_name)
                
                if forecast_records:
                    save_forecast_data(forecast_records, region_name)
                    successful_scrapes += 1
                else:
                    print(f"❌ No forecast data returned for {region_name}")
                
                # Wait between requests to be respectful
                print("⏳ Waiting 5 seconds before next request...")
                time.sleep(5)
                
            except Exception as e:
                print(f"❌ Error processing {region_name}: {str(e)}")
                continue
        
        print(f"\n✅ Scraper run completed!")
        print(f"📊 Successfully scraped {successful_scrapes}/{len(regions_to_scrape)} regions")
        
    except Exception as e:
        print(f"❌ Error in scraper run: {str(e)}")

def test_single_location():
    """Test scraper with Wollongong only"""
    print("🧪 Testing scraper with Wollongong...")
    
    scraper = WillyWeatherScraper()
    location_id = 17663  # Wollongong City Beach
    
    forecast_records = scraper.get_forecast_data(location_id, "Wollongong")
    
    if forecast_records:
        print("✅ Test successful! Sample data:")
        print(f"📊 Created {len(forecast_records)} records")
        for i, record in enumerate(forecast_records):
            swell = f"{record['swell_height']}m" if record['swell_height'] else "N/A"
            wind = f"{record['wind_speed']}kt" if record['wind_speed'] else "N/A"
            print(f"  {record['time_period']}: Swell {swell}, Wind {wind}")
        
        # Try to save it
        save_forecast_data(forecast_records, "Wollongong")
    else:
        print("❌ Test failed - no data returned")

def schedule_scraper():
    """Schedule the scraper to run periodically"""
    print("🕒 Setting up WillyWeather scraper schedule...")
    print("📅 Will run every 6 hours and save hourly-specific data")
    
    # Run every 6 hours
    schedule.every(6).hours.do(run_scraper)
    
    # Also run once immediately
    run_scraper()
    
    # Keep the scheduler running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    print("🌊 Starting Enhanced WillyWeather Surf Scraper...")
    
    # For testing, run just once
    print("🧪 Running test mode...")
    test_single_location()
    
    # Run the full scraper
    schedule_scraper()