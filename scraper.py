import os
import requests
import schedule
import time
from datetime import datetime, date
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Supabase client with SERVICE KEY
supabase: Client = create_client(
    os.getenv("NEXT_PUBLIC_SUPABASE_URL"),
    os.getenv("SUPABASE_SERVICE_KEY")
)

# WillyWeather API configuration
WILLY_WEATHER_API_KEY = os.getenv("WILLY_WEATHER_API_KEY")
BASE_URL = "https://api.willyweather.com.au/v2"

# CORRECTED Australian surf locations from WillyWeather
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
    "Phillip Island": {"location_id": 5032, "state": "VIC"},
    "East Gippsland": {"location_id": 4948, "state": "VIC"},
    "West Coast": {"location_id": 5083, "state": "VIC"},
    
    # South Australia
    "Adelaide": {"location_id": 4909, "state": "SA"},
    "Fleurieu Peninsula": {"location_id": 5087, "state": "SA"},
    "Yorke Peninsula": {"location_id": 5095, "state": "SA"},
    "Eyre Peninsula": {"location_id": 4937, "state": "SA"},
    "Kangaroo Island": {"location_id": 4964, "state": "SA"},
    
    # Western Australia
    "Perth": {"location_id": 5026, "state": "WA"},
    "Margaret River": {"location_id": 4986, "state": "WA"},
    "Geraldton": {"location_id": 4957, "state": "WA"},
    "Esperance": {"location_id": 4951, "state": "WA"},
    "Albany": {"location_id": 4912, "state": "WA"},  # FIXED: was 4913, now 4912
    "Exmouth": {"location_id": 4952, "state": "WA"},
    "Broome": {"location_id": 4927, "state": "WA"},
    
    # Tasmania
    "Hobart": {"location_id": 4959, "state": "TAS"},
    "Launceston": {"location_id": 4975, "state": "TAS"},
    "North West Coast": {"location_id": 4944, "state": "TAS"},
    "East Coast": {"location_id": 5076, "state": "TAS"}
}

# Time slots with hour mapping for API data extraction
TIME_SLOTS = [
    {'period': '6am', 'hour': 6},
    {'period': '8am', 'hour': 8},
    {'period': '10am', 'hour': 10},
    {'period': '12pm', 'hour': 12},
    {'period': '2pm', 'hour': 14},
    {'period': '4pm', 'hour': 16},
    {'period': '6pm', 'hour': 18}
]

class WillyWeatherScraper:
    def __init__(self):
        self.api_key = WILLY_WEATHER_API_KEY
        if not self.api_key:
            raise ValueError("WILLY_WEATHER_API_KEY not found in environment variables")
    
    def get_forecast_data(self, location_id: int, region_name: str):
        """Get forecast data from WillyWeather API with improved error handling"""
        try:
            print(f"🌊 Fetching forecast for {region_name} (ID: {location_id})")
            
            # Build URL with proper parameters
            url = f"{BASE_URL}/{self.api_key}/locations/{location_id}/weather.json"
            params = {
                'forecasts': 'swell,wind,tides',
                'days': 3,
                'startDate': date.today().strftime('%Y-%m-%d')
            }
            
            # Make API request with timeout and error handling
            response = requests.get(url, params=params, timeout=15)
            
            # Check for HTTP errors
            if response.status_code == 404:
                print(f"❌ Location ID {location_id} not found for {region_name}")
                print(f"🔍 Try using a different location ID for {region_name}")
                return []
            elif response.status_code != 200:
                print(f"❌ API request failed for {region_name}: {response.status_code}")
                print(f"📝 Response: {response.text[:200]}...")
                return []
            
            # Parse JSON response
            try:
                data = response.json()
            except requests.exceptions.JSONDecodeError:
                print(f"❌ Invalid JSON response for {region_name}")
                return []
            
            # Validate data structure
            if not data or not isinstance(data, dict):
                print(f"❌ Empty or invalid data structure for {region_name}")
                return []
            
            print(f"✅ Successfully fetched data for {region_name}")
            
            # Extract forecast data with better error handling
            forecast_records = self.extract_forecast_records(data, region_name)
            
            if forecast_records:
                print(f"📊 Processed {len(forecast_records)} forecast records for {region_name}")
                return forecast_records
            else:
                print(f"⚠️  No forecast records could be extracted for {region_name}")
                return []
            
        except requests.exceptions.Timeout:
            print(f"⏰ Request timeout for {region_name}")
            return []
        except requests.exceptions.ConnectionError:
            print(f"🌐 Connection error for {region_name}")
            return []
        except Exception as e:
            print(f"❌ Unexpected error processing {region_name}: {str(e)}")
            return []
    
    def extract_forecast_records(self, data: dict, region_name: str):
        """Extract forecast records with improved error handling"""
        try:
            forecasts = data.get('forecasts', {})
            if not forecasts:
                print(f"⚠️  No forecasts section in API response for {region_name}")
                return []
            
            # Get forecast components
            swell_forecast = forecasts.get('swell', {}).get('days', [])
            wind_forecast = forecasts.get('wind', {}).get('days', [])
            tides_forecast = forecasts.get('tides', {}).get('days', [])
            
            if not swell_forecast and not wind_forecast:
                print(f"⚠️  No swell or wind forecast data for {region_name}")
                return []
            
            forecast_records = []
            
            # Process each day
            for day_index in range(min(3, len(swell_forecast))):
                try:
                    # Get data for this day
                    swell_day = swell_forecast[day_index] if day_index < len(swell_forecast) else {}
                    wind_day = wind_forecast[day_index] if day_index < len(wind_forecast) else {}
                    tides_day = tides_forecast[day_index] if day_index < len(tides_forecast) else {}
                    
                    # Get the date for this forecast
                    forecast_date = swell_day.get('dateTime', wind_day.get('dateTime', ''))
                    if not forecast_date:
                        continue
                    
                    forecast_date = forecast_date.split('T')[0]  # Extract just the date part
                    
                    # Get hourly data if available
                    swell_entries = swell_day.get('entries', [])
                    wind_entries = wind_day.get('entries', [])
                    tide_entries = tides_day.get('entries', [])
                    
                    # Process each time slot
                    for time_slot in TIME_SLOTS:
                        period = time_slot['period']
                        target_hour = time_slot['hour']
                        
                        # Find closest data for this time slot
                        swell_data = self.find_closest_entry(swell_entries, target_hour)
                        wind_data = self.find_closest_entry(wind_entries, target_hour)
                        tide_data = self.find_closest_entry(tide_entries, target_hour)
                        
                        # Create forecast record
                        record = {
                            'forecast_date': forecast_date,
                            'time_period': period,
                            'swell_height': self.safe_get_float(swell_data, 'height'),
                            'swell_direction': self.safe_get_float(swell_data, 'direction'),
                            'swell_period': self.safe_get_float(swell_data, 'period'),
                            'wind_speed': self.safe_get_float(wind_data, 'speed'),
                            'wind_direction': self.safe_get_float(wind_data, 'direction'),
                            'tide_height': self.safe_get_float(tide_data, 'height')
                        }
                        
                        forecast_records.append(record)
                
                except Exception as e:
                    print(f"⚠️  Error processing day {day_index} for {region_name}: {str(e)}")
                    continue
            
            return forecast_records
            
        except Exception as e:
            print(f"❌ Error extracting forecast data for {region_name}: {str(e)}")
            return []
    
    def find_closest_entry(self, entries: list, target_hour: int):
        """Find the entry closest to target hour"""
        if not entries:
            return {}
        
        closest_entry = None
        min_diff = float('inf')
        
        for entry in entries:
            if not isinstance(entry, dict):
                continue
                
            entry_time = entry.get('dateTime', '')
            if not entry_time:
                continue
                
            try:
                # Extract hour from dateTime
                if 'T' in entry_time:
                    time_part = entry_time.split('T')[1]
                    hour = int(time_part.split(':')[0])
                    
                    diff = abs(hour - target_hour)
                    if diff < min_diff:
                        min_diff = diff
                        closest_entry = entry
                        
            except (ValueError, IndexError):
                continue
        
        return closest_entry or {}
    
    def safe_get_float(self, data: dict, key: str):
        """Safely extract float value from data"""
        if not data or not isinstance(data, dict):
            return None
        
        value = data.get(key)
        if value is None:
            return None
        
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

def get_all_breaks_to_scrape():
    """Get all unique regions from surf_breaks table"""
    try:
        print("🔍 Getting surf breaks to scrape from database...")
        response = supabase.table('surf_breaks').select('region').execute()
        
        # Get unique regions that exist in our location mapping
        unique_regions = set()
        for break_data in response.data:
            region = break_data['region']
            if region and region in AUSTRALIAN_SURF_LOCATIONS:
                unique_regions.add(region)
        
        print(f"📍 Found {len(unique_regions)} unique regions in database: {list(unique_regions)}")
        print(f"📍 Found {len(unique_regions)} unique regions to scrape: {list(unique_regions)}")
        return list(unique_regions)
    except Exception as e:
        print(f"❌ Error getting breaks from database: {str(e)}")
        return []

def save_forecast_data(forecast_records: list, region_name: str):
    """Save forecast data to the database with better error handling"""
    try:
        print(f"💾 Saving {len(forecast_records)} forecast records for {region_name}...")
        
        # Get all breaks that use this region
        response = supabase.table('surf_breaks').select('id').eq('region', region_name).execute()
        
        if not response.data:
            print(f"⚠️  No breaks found for region {region_name}")
            return
        
        # Save forecast data for each break in this region
        total_saved = 0
        for break_data in response.data:
            break_id = break_data['id']
            
            for forecast_record in forecast_records:
                # Prepare forecast record for database
                db_record = {
                    'break_id': break_id,
                    'forecast_date': forecast_record.get('forecast_date'),
                    'forecast_time': forecast_record.get('time_period'),
                    'swell_height': forecast_record.get('swell_height'),
                    'swell_direction': forecast_record.get('swell_direction'),
                    'swell_period': forecast_record.get('swell_period'),
                    'wind_speed': forecast_record.get('wind_speed'),
                    'wind_direction': forecast_record.get('wind_direction'),
                    'tide_height': forecast_record.get('tide_height')
                }
                
                # Use upsert to avoid duplicates
                try:
                    result = supabase.table('forecast_data').upsert(
                        db_record,
                        on_conflict='break_id, forecast_date, forecast_time'
                    ).execute()
                    total_saved += 1
                    
                except Exception as save_error:
                    print(f"❌ Error saving individual record: {str(save_error)}")
                    continue
                
        print(f"✅ Saved {total_saved} forecast records for {region_name}")
        
        # Show sample of what was saved
        if forecast_records:
            sample = forecast_records[0]
            print(f"📊 Sample data: {sample['time_period']} - Swell: {sample.get('swell_height', 'N/A')}m, Wind: {sample.get('wind_speed', 'N/A')}kt")
        
    except Exception as e:
        print(f"❌ Error saving forecast data for {region_name}: {str(e)}")

def test_specific_location(location_id: int, region_name: str):
    """Test a specific location to debug API issues"""
    print(f"\n🧪 Testing {region_name} (ID: {location_id})")
    
    try:
        scraper = WillyWeatherScraper()
        
        # Test API call
        url = f"{BASE_URL}/{scraper.api_key}/locations/{location_id}/weather.json"
        params = {
            'forecasts': 'swell,wind,tides',
            'days': 1
        }
        
        print(f"🔗 Testing URL: {url}")
        response = requests.get(url, params=params, timeout=10)
        
        print(f"📡 Response status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            location_name = data.get('location', {}).get('name', 'Unknown')
            print(f"✅ Success! Location: {location_name}")
            
            # Check forecast structure
            forecasts = data.get('forecasts', {})
            print(f"📊 Available forecasts: {list(forecasts.keys())}")
            
            # Try processing with scraper
            forecast_records = scraper.extract_forecast_records(data, region_name)
            print(f"📈 Records extracted: {len(forecast_records)}")
            
        else:
            print(f"❌ Failed: {response.text[:200]}...")
            
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")

def run_scraper():
    """Main scraper function with improved error handling"""
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

def debug_failing_locations():
    """Debug the specific locations that are failing"""
    print("🔍 Debugging failing locations...\n")
    
    failing_locations = [
        (4913, "Albany (old ID)"),  # The one that's giving 404
        (4912, "Albany (correct ID)"),  # Test the correct ID
        (4923, "South Coast"),
        (4958, "Gold Coast")
    ]
    
    for location_id, name in failing_locations:
        test_specific_location(location_id, name)
        print("\n" + "="*50)

def schedule_scraper():
    """Schedule the scraper to run periodically"""
    print("🕒 Setting up WillyWeather scraper schedule...")
    print("📅 Will run every 6 hours")
    
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
    
    # Uncomment to debug specific locations
    # debug_failing_locations()
    
    # For testing, run just once
    print("🧪 Running test mode...")
    run_scraper()
    
    # Uncomment this to run the scheduler
    schedule_scraper()