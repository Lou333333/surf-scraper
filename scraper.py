# Add this at the top of your scraper.py, right after the imports:
import os
from dotenv import load_dotenv

# Debug environment loading
print("🔍 Debug: Environment loading in Railway")
load_dotenv()

print(f"NEXT_PUBLIC_SUPABASE_URL found: {bool(os.getenv('NEXT_PUBLIC_SUPABASE_URL'))}")
print(f"SUPABASE_SERVICE_KEY found: {bool(os.getenv('SUPABASE_SERVICE_KEY'))}")
print(f"WILLY_WEATHER_API_KEY found: {bool(os.getenv('WILLY_WEATHER_API_KEY'))}")

# Then your existing Supabase client creation...
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
    os.getenv("SUPABASE_SERVICE_KEY")  # Changed from NEXT_PUBLIC_SUPABASE_ANON_KEY
)

# WillyWeather API configuration
WILLY_WEATHER_API_KEY = os.getenv("WILLY_WEATHER_API_KEY")
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
    "Albany": {"location_id": 4912, "state": "WA"},
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
        """Get forecast data from WillyWeather API"""
        try:
            print(f"🌊 Getting forecast for {region_name} (ID: {location_id})")
            
            # Get current conditions and forecast for today and tomorrow
            url = f"{BASE_URL}/{self.api_key}/locations/{location_id}/weather.json"
            
            params = {
                'forecasts': 'swell,wind,tides',
                'days': 2,  # Get 2 days of data to have hourly forecasts
                'startDate': date.today().isoformat()
            }
            
            print(f"📡 Making API request to: {url}")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            print(f"📊 API Response received for {region_name}")
            
            # Debug: Print the structure of the response
            if 'forecasts' in data:
                print(f"🔍 Available forecasts: {list(data['forecasts'].keys())}")
            else:
                print(f"⚠️  No 'forecasts' key in response for {region_name}")
                print(f"🔍 Response keys: {list(data.keys())}")
            
            # Extract forecast data for all time slots with hourly specificity
            forecast_records = self.extract_hourly_forecasts(data, region_name)
            return forecast_records
            
        except requests.exceptions.RequestException as e:
            print(f"❌ API request failed for {region_name}: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response text: {e.response.text[:200]}...")
            return None
        except Exception as e:
            print(f"❌ Error getting forecast for {region_name}: {str(e)}")
            return None
    
    def extract_hourly_forecasts(self, data: dict, region_name: str):
        """Extract forecast data for specific hours when available"""
        try:
            forecast_records = []
            
            print(f"⏰ Extracting hourly forecast data for {region_name}")
            
            # Extract all available entries for today
            swell_entries = self.get_forecast_entries(data, 'swell')
            wind_entries = self.get_forecast_entries(data, 'wind')
            tide_entries = self.get_forecast_entries(data, 'tides')
            
            print(f"📊 Found {len(swell_entries)} swell, {len(wind_entries)} wind, {len(tide_entries)} tide entries")
            
            # Create forecast for each time slot
            for slot in TIME_SLOTS:
                time_period = slot['period']
                target_hour = slot['hour']
                
                print(f"⌚ Processing {time_period} (hour {target_hour})")
                
                # Find best matching data for this hour
                swell_data = self.find_best_match(swell_entries, target_hour)
                wind_data = self.find_best_match(wind_entries, target_hour)
                tide_data = self.find_best_match(tide_entries, target_hour)
                
                forecast_record = {
                    'region': region_name,
                    'forecast_date': date.today().isoformat(),
                    'time_period': time_period,
                    'swell_height': swell_data.get('height') if swell_data else None,
                    'swell_direction': swell_data.get('direction') if swell_data else None,
                    'swell_period': swell_data.get('period') if swell_data else None,
                    'wind_speed': wind_data.get('speed') if wind_data else None,
                    'wind_direction': wind_data.get('direction') if wind_data else None,
                    'tide_height': tide_data.get('height') if tide_data else None
                }
                
                forecast_records.append(forecast_record)
                
                # Log what data we found for this slot
                swell_info = f"{swell_data.get('height', 'N/A')}m" if swell_data else "N/A"
                wind_info = f"{wind_data.get('speed', 'N/A')}kt" if wind_data else "N/A"
                print(f"  ✅ {time_period}: Swell {swell_info}, Wind {wind_info}")
            
            print(f"✅ Created {len(forecast_records)} hourly forecast records for {region_name}")
            return forecast_records
            
        except Exception as e:
            print(f"❌ Error extracting hourly forecasts for {region_name}: {str(e)}")
            return None
    
    def get_forecast_entries(self, data: dict, forecast_type: str):
        """Extract all entries for a specific forecast type"""
        try:
            if 'forecasts' not in data or forecast_type not in data['forecasts']:
                return []
            
            forecast_data = data['forecasts'][forecast_type]
            if 'days' not in forecast_data or len(forecast_data['days']) == 0:
                return []
            
            # Get today's entries
            today_data = forecast_data['days'][0]
            entries = today_data.get('entries', [])
            
            # Add timestamp info to entries if available
            for entry in entries:
                if 'dateTime' in entry:
                    # Parse the datetime if available
                    try:
                        dt = datetime.fromisoformat(entry['dateTime'].replace('Z', '+00:00'))
                        entry['hour'] = dt.hour
                    except:
                        pass
            
            return entries
            
        except Exception as e:
            print(f"⚠️ Error getting {forecast_type} entries: {str(e)}")
            return []
    
    def find_best_match(self, entries: list, target_hour: int):
        """Find the entry closest to the target hour"""
        if not entries:
            return None
        
        # If entries have hour information, find closest match
        entries_with_hours = [e for e in entries if 'hour' in e]
        
        if entries_with_hours:
            # Find entry closest to target hour
            best_entry = min(entries_with_hours, 
                           key=lambda x: abs(x['hour'] - target_hour))
            return best_entry
        
        # Fallback: use entry based on position (rough time estimation)
        if len(entries) >= 4:
            # Assume entries are roughly evenly distributed through the day
            if target_hour <= 8:
                return entries[0]  # Early morning
            elif target_hour <= 12:
                return entries[1]  # Late morning
            elif target_hour <= 16:
                return entries[2]  # Afternoon
            else:
                return entries[3]  # Evening
        
        # Final fallback: just use first entry
        return entries[0] if entries else None

def get_all_breaks_to_scrape():
    """Get all unique regions from the database that need scraping"""
    try:
        print("📋 Getting breaks from database...")
        response = supabase.table('surf_breaks').select('region').execute()
        
        # Get unique regions
        unique_regions = set()
        for break_data in response.data:
            region = break_data['region']
            if region and region in AUSTRALIAN_SURF_LOCATIONS:
                unique_regions.add(region)
        
        print(f"📍 Found regions in database: {list(unique_regions)}")
        return list(unique_regions)
    except Exception as e:
        print(f"❌ Error getting breaks from database: {str(e)}")
        return []

def save_forecast_data(forecast_records: list, region_name: str):
    """Save forecast data to the database for all time slots"""
    try:
        print(f"💾 Saving forecast data for {region_name} (all time slots)...")
        
        # Get all breaks that use this region
        response = supabase.table('surf_breaks').select('id').eq('region', region_name).execute()
        
        if not response.data:
            print(f"⚠️  No breaks found for region {region_name}")
            return
        
        # Save forecast data for each break in this region, for each time slot
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
                result = supabase.table('forecast_data').upsert(
                    db_record,
                    on_conflict='break_id, forecast_date, forecast_time'
                ).execute()
                
                total_saved += 1
                
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
                print("⏳ Waiting 3 seconds before next request...")
                time.sleep(3)
                
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
    
    # Uncomment this to run the full scraper
    schedule_scraper()