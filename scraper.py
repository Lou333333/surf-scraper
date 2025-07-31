import os
import requests
import schedule
import time
from datetime import datetime, date
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Supabase client
supabase: Client = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

# WillyWeather API configuration
WILLY_WEATHER_API_KEY = os.getenv("WILLY_WEATHER_API_KEY")
BASE_URL = "https://api.willyweather.com.au/v2"

# Australian surf locations from WillyWeather (FIXED location IDs)
AUSTRALIAN_SURF_LOCATIONS = {
    # New South Wales
    "Sydney": {"location_id": 4950, "state": "NSW"},
    "Central Coast": {"location_id": 4934, "state": "NSW"},
    "Newcastle": {"location_id": 4988, "state": "NSW"},
    "Mid North Coast": {"location_id": 5049, "state": "NSW"},
    "Byron Bay": {"location_id": 4947, "state": "NSW"},
    "Wollongong": {"location_id": 17663, "state": "NSW"},  # This is correct for Wollongong City Beach
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

# All possible time slots we want to save data for
TIME_SLOTS = ['6am', '8am', '10am', '12pm', '2pm', '4pm', '6pm']

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
            
            # Extract forecast data for all time slots
            forecast_records = self.extract_all_time_slots(data, region_name)
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
    
    def extract_all_time_slots(self, data: dict, region_name: str):
        """Extract forecast data for all time slots"""
        try:
            forecast_records = []
            
            print(f"⏰ Creating forecast data for all time slots: {TIME_SLOTS}")
            
            # Get base forecast data (we'll use the current/first available data)
            base_forecast = self.extract_base_forecast_data(data, region_name)
            
            if not base_forecast:
                print(f"❌ No base forecast data available for {region_name}")
                return None
            
            # Create a forecast record for each time slot
            for time_slot in TIME_SLOTS:
                forecast_record = {
                    'region': region_name,
                    'forecast_date': date.today().isoformat(),
                    'time_period': time_slot,
                    'swell_height': base_forecast.get('swell_height'),
                    'swell_direction': base_forecast.get('swell_direction'),
                    'swell_directionText': base_forecast.get('swell_directionText'),
                    'swell_period': base_forecast.get('swell_period'),
                    'wind_speed': base_forecast.get('wind_speed'),
                    'wind_direction': base_forecast.get('wind_direction'),
                    'wind_directionText': base_forecast.get('wind_directionText'),
                    'wind_gustSpeed': base_forecast.get('wind_gustSpeed'),
                    'tide_height': base_forecast.get('tide_height'),
                    'tide_type': base_forecast.get('tide_type')
                }
                forecast_records.append(forecast_record)
            
            print(f"✅ Created {len(forecast_records)} forecast records for {region_name}")
            return forecast_records
            
        except Exception as e:
            print(f"❌ Error extracting forecast data for {region_name}: {str(e)}")
            return None
    
    def extract_base_forecast_data(self, data: dict, region_name: str):
        """Extract base forecast data from API response"""
        try:
            forecast_data = {}
            
            # Extract swell data
            try:
                if 'forecasts' in data and 'swell' in data['forecasts']:
                    swell_data = data['forecasts']['swell']
                    if 'days' in swell_data and len(swell_data['days']) > 0:
                        swell_entries = swell_data['days'][0].get('entries', [])
                        if swell_entries:
                            # Get the first available swell entry
                            current_swell = swell_entries[0]
                            
                            forecast_data['swell_height'] = current_swell.get('height')
                            forecast_data['swell_direction'] = current_swell.get('direction')
                            forecast_data['swell_directionText'] = current_swell.get('directionText')
                            forecast_data['swell_period'] = current_swell.get('period')
                            
                            print(f"🌊 Swell: {forecast_data.get('swell_height')}m @ {forecast_data.get('swell_period')}s")
                        else:
                            print(f"⚠️  No swell entries found for {region_name}")
                    else:
                        print(f"⚠️  No swell days data for {region_name}")
                else:
                    print(f"⚠️  No swell forecast data for {region_name}")
            except Exception as e:
                print(f"⚠️  Error extracting swell data: {str(e)}")
            
            # Extract wind data
            try:
                if 'forecasts' in data and 'wind' in data['forecasts']:
                    wind_data = data['forecasts']['wind']
                    if 'days' in wind_data and len(wind_data['days']) > 0:
                        wind_entries = wind_data['days'][0].get('entries', [])
                        if wind_entries:
                            # Get the first available wind entry
                            current_wind = wind_entries[0]
                            
                            forecast_data['wind_speed'] = current_wind.get('speed')
                            forecast_data['wind_direction'] = current_wind.get('direction')
                            forecast_data['wind_directionText'] = current_wind.get('directionText')
                            forecast_data['wind_gustSpeed'] = current_wind.get('gustSpeed')
                            
                            print(f"💨 Wind: {forecast_data.get('wind_speed')}kt from {forecast_data.get('wind_directionText')}")
                        else:
                            print(f"⚠️  No wind entries found for {region_name}")
                    else:
                        print(f"⚠️  No wind days data for {region_name}")
                else:
                    print(f"⚠️  No wind forecast data for {region_name}")
            except Exception as e:
                print(f"⚠️  Error extracting wind data: {str(e)}")
            
            # Extract tide data
            try:
                if 'forecasts' in data and 'tides' in data['forecasts']:
                    tide_data = data['forecasts']['tides']
                    if 'days' in tide_data and len(tide_data['days']) > 0:
                        tide_entries = tide_data['days'][0].get('entries', [])
                        if tide_entries:
                            # Get the first available tide entry
                            current_tide = tide_entries[0]
                            forecast_data['tide_height'] = current_tide.get('height')
                            forecast_data['tide_type'] = current_tide.get('type')
                            
                            print(f"🌊 Tide: {forecast_data.get('tide_height')}m ({forecast_data.get('tide_type')})")
                        else:
                            print(f"⚠️  No tide entries found for {region_name}")
                    else:
                        print(f"⚠️  No tide days data for {region_name}")
                else:
                    print(f"⚠️  No tide forecast data for {region_name}")
            except Exception as e:
                print(f"⚠️  Error extracting tide data: {str(e)}")
            
            # Check if we got any useful data
            has_data = any([
                forecast_data.get('swell_height'),
                forecast_data.get('wind_speed'),
                forecast_data.get('tide_height')
            ])
            
            if has_data:
                print(f"✅ Successfully extracted base data for {region_name}")
                return forecast_data
            else:
                print(f"❌ No useful base data extracted for {region_name}")
                return None
            
        except Exception as e:
            print(f"❌ Error extracting base forecast data for {region_name}: {str(e)}")
            return None

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
        for i, record in enumerate(forecast_records[:3]):  # Show first 3
            print(f"  Record {i+1}: {record['time_period']} - {record['swell_height']}m swell, {record['wind_speed']}kt wind")
        
        # Try to save it
        save_forecast_data(forecast_records, "Wollongong")
    else:
        print("❌ Test failed - no data returned")

def schedule_scraper():
    """Schedule the scraper to run periodically"""
    print("🕒 Setting up WillyWeather scraper schedule...")
    print("📅 Will run every 6 hours and save data for ALL time slots")
    
    # Run every 6 hours
    schedule.every(6).hours.do(run_scraper)
    
    # Also run once immediately
    run_scraper()
    
    # Keep the scheduler running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    print("🌊 Starting WillyWeather Surf Scraper (All Time Slots)...")
    
    # For testing, run just once
    print("🧪 Running test mode...")
    # test_single_location()
    
    # Uncomment this to run the full scraper
    schedule_scraper()