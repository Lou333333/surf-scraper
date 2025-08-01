import os
import requests
import schedule
import time
from datetime import datetime, date
from supabase import create_client, Client
import uuid

# Don't use load_dotenv() in production/Railway - variables are already available
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
}

class WillyWeatherScraper:
    def __init__(self):
        self.api_key = WILLY_WEATHER_API_KEY
        self.base_url = BASE_URL
        self.break_cache = {}  # Cache break UUIDs
        
    def get_break_uuid_for_region(self, region_name):
        """Get the UUID of the surf break for a given region"""
        if region_name in self.break_cache:
            return self.break_cache[region_name]
            
        try:
            response = supabase.table('surf_breaks').select('id').eq('region', region_name).limit(1).execute()
            
            if response.data and len(response.data) > 0:
                break_uuid = response.data[0]['id']
                self.break_cache[region_name] = break_uuid
                print(f"✅ Found break UUID for {region_name}: {break_uuid}")
                return break_uuid
            else:
                print(f"❌ No surf break found in database for region: {region_name}")
                return None
                
        except Exception as e:
            print(f"❌ Error getting break UUID for {region_name}: {str(e)}")
            return None
        
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
            
            if response.status_code == 200:
                print(f"✅ Successfully fetched data for {region_name}")
                return response.json()
            else:
                print(f"❌ API request failed for {region_name}: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Error fetching data for {region_name}: {str(e)}")
            return None

    def process_forecast_data(self, data, region_name):
        """Process raw forecast data into database records"""
        try:
            # Get the break UUID first
            break_uuid = self.get_break_uuid_for_region(region_name)
            if not break_uuid:
                print(f"❌ Cannot process data for {region_name} - no break UUID found")
                return []

            forecast_records = []
            forecasts = data.get('forecasts', {})
            
            # Debug API response structure
            print(f"🔍 DEBUG: API response structure for {region_name}")
            print(f" Forecast keys: {list(forecasts.keys())}")
            
            swell_data = forecasts.get('swell')
            wind_data = forecasts.get('wind')
            
            print(f" Swell structure: {type(swell_data)}")
            if swell_data:
                print(f" Swell keys: {list(swell_data.keys()) if isinstance(swell_data, dict) else 'Not a dict'}")
            
            print(f"🔍 DEBUG: Swell data type for {region_name}: {type(swell_data)}")
            print(f"🔍 DEBUG: Wind data type for {region_name}: {type(wind_data)}")
            
            # Check if swell data exists and is valid
            if not swell_data or not isinstance(swell_data, dict):
                print(f"❌ No forecast data returned for {region_name}")
                print(f" Swell structure: {type(swell_data)}")
                return []
            
            swell_days = swell_data.get('days', [])
            print(f"🔍 DEBUG: Found {len(swell_days)} swell days for {region_name}")
            
            if not swell_days:
                print(f"⚠️ No swell days found for {region_name}")
                return []
            
            # Process each day
            for day_idx, day in enumerate(swell_days):
                day_date = day.get('dateTime', '')[:10]  # Extract YYYY-MM-DD
                
                entries = day.get('entries', [])
                if not entries:
                    print(f"⚠️ No entries found for day {day_idx} in {region_name}")
                    continue
                
                # Process each hourly entry
                for entry_idx, entry in enumerate(entries):
                    try:
                        entry_time = entry.get('dateTime', '')
                        if not entry_time:
                            continue
                            
                        # Extract hour and convert to time format
                        hour = int(entry_time[11:13])
                        if hour == 6: time_slot = '6am'
                        elif hour == 8: time_slot = '8am'
                        elif hour == 10: time_slot = '10am'
                        elif hour == 12: time_slot = '12pm'
                        elif hour == 14: time_slot = '2pm'
                        elif hour == 16: time_slot = '4pm'
                        elif hour == 18: time_slot = '6pm'
                        else: continue  # Skip other hours
                        
                        # Get wind data for the same time
                        wind_entry = None
                        if wind_data and isinstance(wind_data, dict):
                            wind_days = wind_data.get('days', [])
                            if day_idx < len(wind_days):
                                wind_entries = wind_days[day_idx].get('entries', [])
                                if entry_idx < len(wind_entries):
                                    wind_entry = wind_entries[entry_idx]
                        
                        # Create forecast record with proper UUID
                        record = {
                            'break_id': break_uuid,  # Use actual UUID from database
                            'forecast_date': day_date,
                            'forecast_time': time_slot,
                            'swell_height': entry.get('height'),
                            'swell_direction': entry.get('direction'),
                            'swell_period': entry.get('period'),
                            'wind_speed': wind_entry.get('speed') if wind_entry else None,
                            'wind_direction': wind_entry.get('direction') if wind_entry else None,
                            'tide_height': None,  # Will be filled later if tide data available
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
            
            # Filter to only regions we have location IDs for
            valid_regions = [region for region in unique_regions if region in AUSTRALIAN_SURF_LOCATIONS]
            print(f"📍 Found {len(valid_regions)} valid regions to scrape: {valid_regions}")
            
            return valid_regions
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
            result = supabase.table('forecast_data').upsert(
                batch,
                on_conflict='break_id, forecast_date, forecast_time'
            ).execute()
            
            if result.data:
                batch_saved = len(result.data)
                total_saved += batch_saved
                print(f"  📦 Batch {i//batch_size + 1}: Saved {batch_saved} records")
            
        print(f"✅ Successfully saved {total_saved} forecast records for {region_name}")
        
    except Exception as e:
        print(f"❌ Error saving forecast data for {region_name}: {str(e)}")
        print(f"    Error details: {e}")

def run_enhanced_scraper():
    """Enhanced scraper that fetches from WillyWeather and saves to database"""
    print("🌊 Starting Enhanced WillyWeather Surf Scraper...")
    
    # Test mode - run once for specific location
    if os.getenv('TEST_MODE', 'false').lower() == 'true':
        print("🧪 Running test mode...")
        print("🧪 Testing scraper with Wollongong...")
        
        scraper = WillyWeatherScraper()
        test_location = AUSTRALIAN_SURF_LOCATIONS["Wollongong"]
        
        # Fetch data
        raw_data = scraper.get_forecast_data(test_location["location_id"], "Wollongong")
        
        if raw_data:
            # Process data
            forecast_records = scraper.process_forecast_data(raw_data, "Wollongong")
            
            if forecast_records:
                print("✅ Test successful! Sample data:")
                print(f"📊 Created {len(forecast_records)} records")
                for i, record in enumerate(forecast_records[:5]):  # Show first 5
                    print(f" {record['forecast_date']} {record['forecast_time']}: Swell {record['swell_height']}m")
                
                # Save to database
                save_forecast_data(forecast_records, "Wollongong")
            else:
                print("❌ No forecast records created")
        else:
            print("❌ Failed to fetch data")
        return
    
    # Production mode - scrape all regions
    print("🔄 Production mode - scraping all regions...")
    
    try:
        scraper = WillyWeatherScraper()
        
        # Get regions to scrape from database
        regions_to_scrape = get_all_breaks_to_scrape()
        
        if not regions_to_scrape:
            print("❌ No regions found to scrape")
            return
        
        print(f"🌊 Starting WillyWeather scraper run at {datetime.now()}")
        
        for region in regions_to_scrape:
            print(f"--- Processing {region} ---")
            
            # Get location info
            location_info = AUSTRALIAN_SURF_LOCATIONS.get(region)
            if not location_info:
                print(f"⚠️ No location ID found for {region}, skipping...")
                continue
            
            # Fetch forecast data
            raw_data = scraper.get_forecast_data(location_info["location_id"], region)
            
            if raw_data:
                # Process and save data
                forecast_records = scraper.process_forecast_data(raw_data, region)
                save_forecast_data(forecast_records, region)
            else:
                print(f"❌ Failed to fetch data for {region}")
            
            # Rate limiting - wait between requests
            print("⏳ Waiting 5 seconds before next request...")
            time.sleep(5)
        
        print("🎉 Scraper run completed!")
        
    except Exception as e:
        print(f"❌ Scraper run failed: {str(e)}")

if __name__ == "__main__":
    print("🕒 Setting up WillyWeather scraper schedule...")
    print("📅 Will run every 6 hours and save hourly-specific data")
    
    # Schedule the scraper to run every 6 hours
    schedule.every(6).hours.do(run_enhanced_scraper)
    
    # Run immediately on startup
    run_enhanced_scraper()
    
    print("✅ Scraper initialized successfully")
    
    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute