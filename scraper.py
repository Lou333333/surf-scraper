#!/usr/bin/env python3
"""
Fixed surf scraper that creates forecast data for EVERY individual surf break,
not just one per region.
"""

import os
import requests
import schedule
import time
from datetime import datetime, date, timedelta
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Supabase
SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ Missing Supabase credentials")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# WillyWeather API configuration
WILLY_WEATHER_API_KEY = os.getenv("WILLY_WEATHER_API_KEY")
BASE_URL = "https://api.willyweather.com.au/v2"

# Australian surf locations with their WillyWeather location IDs
AUSTRALIAN_SURF_LOCATIONS = {
    "Gold Coast": {"location_id": 3690, "state": "QLD"},
    "Byron Bay": {"location_id": 3690, "state": "NSW"},       # Same API endpoint as Gold Coast/Far North Coast
    "Wollongong": {"location_id": 17663, "state": "NSW"},
    "South Coast": {"location_id": 17621, "state": "NSW"},     # Merimbula
    "Far North Coast": {"location_id": 3690, "state": "NSW"},  # Same as Byron Bay
    "Central Coast": {"location_id": 17648, "state": "NSW"},   # Gosford area
}

class WillyWeatherScraper:
    def __init__(self):
        self.api_key = WILLY_WEATHER_API_KEY
        self.base_url = BASE_URL
        
    def get_all_breaks_by_region(self, region_name):
        """Get ALL surf breaks for a given region"""
        try:
            response = supabase.table('surf_breaks').select('id, name, region').eq('region', region_name).execute()
            
            if response.data:
                print(f"✅ Found {len(response.data)} breaks in {region_name}:")
                for break_data in response.data:
                    print(f"  - {break_data['name']} (ID: {break_data['id']})")
                return response.data
            else:
                print(f"❌ No surf breaks found for region: {region_name}")
                return []
                
        except Exception as e:
            print(f"❌ Error getting breaks for {region_name}: {str(e)}")
            return []
        
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
                print(f"✅ Successfully fetched forecast data for {region_name}")
                return response.json()
            else:
                print(f"❌ Failed to fetch forecast: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Error fetching forecast for {region_name}: {str(e)}")
            return None

    def get_tide_height_for_time_slot(self, tide_data, forecast_date, time_slot):
        """Extract tide height and direction for a specific 2-hour time slot from tide data"""
        if not tide_data or not tide_data.get('days'):
            return None, None
        
        # Map time slots to start and end hours
        time_to_hours = {
            '6am': (6, 8),   # 6am-8am
            '8am': (8, 10),  # 8am-10am
            '10am': (10, 12), # 10am-12pm
            '12pm': (12, 14), # 12pm-2pm
            '2pm': (14, 16),  # 2pm-4pm
            '4pm': (16, 18),  # 4pm-6pm
            '6pm': (18, 20)   # 6pm-8pm
        }
        
        hours_range = time_to_hours.get(time_slot)
        if hours_range is None:
            return None, None
        
        start_hour, end_hour = hours_range
        
        # Find the day matching our forecast date
        for tide_day in tide_data['days']:
            if tide_day['dateTime'][:10] == forecast_date:
                entries = tide_day.get('entries', [])
                
                # Find entries for start and end of time slot
                start_tide = None
                end_tide = None
                start_time_diff = float('inf')
                end_time_diff = float('inf')
                
                for entry in entries:
                    entry_datetime = entry.get('dateTime')
                    if entry_datetime and 'height' in entry:
                        try:
                            # Parse the datetime string
                            entry_dt = datetime.fromisoformat(entry_datetime.replace('Z', '+00:00'))
                            entry_hour = entry_dt.hour
                            
                            # Check if this is closest to start hour
                            start_diff = abs(entry_hour - start_hour)
                            if start_diff < start_time_diff:
                                start_time_diff = start_diff
                                start_tide = entry['height']
                            
                            # Check if this is closest to end hour
                            end_diff = abs(entry_hour - end_hour)
                            if end_diff < end_time_diff:
                                end_time_diff = end_diff
                                end_tide = entry['height']
                                
                        except:
                            continue
                
                # Calculate average tide height and direction
                if start_tide is not None and end_tide is not None:
                    avg_tide = (start_tide + end_tide) / 2
                    
                    # Determine tide direction
                    tide_diff = end_tide - start_tide
                    if tide_diff > 0.1:  # Rising by more than 10cm
                        tide_direction = "Rising"
                    elif tide_diff < -0.1:  # Falling by more than 10cm
                        tide_direction = "Falling"
                    else:  # Change is less than 10cm
                        tide_direction = "Stable"
                    
                    return avg_tide, tide_direction
                    
                elif start_tide is not None:
                    # Only have start tide
                    return start_tide, "Unknown"
                elif end_tide is not None:
                    # Only have end tide
                    return end_tide, "Unknown"
                    
        return None, None

    def process_forecast_data(self, api_data, region_name, all_breaks):
        """Process API data and create forecast records for ALL breaks in the region"""
        try:
            print(f"📊 Processing forecast data for {len(all_breaks)} breaks in {region_name}")
            
            forecasts = api_data.get('forecasts', {})
            swell_data = forecasts.get('swell')
            wind_data = forecasts.get('wind')
            tide_data = forecasts.get('tides')  # Get tide data
            
            if not swell_data or not swell_data.get('days'):
                print(f"❌ No swell data available for {region_name}")
                return []
            
            all_forecast_records = []
            
            # Process each day's forecast
            for day in swell_data['days']:
                forecast_date = day['dateTime'][:10]  # Extract YYYY-MM-DD
                
                # Get entries for the day (different times)
                day_entries = day.get('entries', [])
                wind_entries = []
                
                # Get corresponding wind data
                if wind_data and wind_data.get('days'):
                    for wind_day in wind_data['days']:
                        if wind_day['dateTime'][:10] == forecast_date:
                            wind_entries = wind_day.get('entries', [])
                            break
                
                # Process each time entry for the day
                for entry_idx, entry in enumerate(day_entries):
                    try:
                        # Map entry index to time slots
                        time_slots = ['6am', '8am', '10am', '12pm', '2pm', '4pm', '6pm']
                        if entry_idx >= len(time_slots):
                            continue
                        
                        forecast_time = time_slots[entry_idx]
                        wind_entry = wind_entries[entry_idx] if entry_idx < len(wind_entries) else None
                        
                        # Get tide height for this specific time slot
                        tide_height, tide_direction = self.get_tide_height_for_time_slot(tide_data, forecast_date, forecast_time)
                        
                        # CREATE A FORECAST RECORD FOR EACH BREAK IN THE REGION
                        for break_data in all_breaks:
                            record = {
                                'break_id': break_data['id'],  # Specific break ID
                                'forecast_date': forecast_date,
                                'forecast_time': forecast_time,
                                'swell_height': entry.get('height'),
                                'swell_direction': entry.get('direction'),
                                'swell_period': entry.get('period'),
                                'wind_speed': wind_entry.get('speed') if wind_entry else None,
                                'wind_direction': wind_entry.get('direction') if wind_entry else None,
                                'tide_height': tide_height,  # Average tide height as number
                                'tide_direction': tide_direction,  # "Rising", "Falling", or "Stable"
                                'region': region_name,  # Keep region for reference
                                'created_at': datetime.now().isoformat(),
                                'updated_at': datetime.now().isoformat()
                            }
                            
                            all_forecast_records.append(record)
                        
                    except Exception as e:
                        print(f"⚠️  Error processing entry {entry_idx} for {region_name}: {str(e)}")
                        continue
            
            print(f"📊 Generated {len(all_forecast_records)} total forecast records")
            print(f"   ({len(all_forecast_records) // len(all_breaks)} time slots × {len(all_breaks)} breaks)")
            return all_forecast_records
            
        except Exception as e:
            print(f"❌ Error processing forecast data for {region_name}: {str(e)}")
            return []

    def save_forecast_data(self, forecast_records):
        """Save forecast data to database using upsert"""
        if not forecast_records:
            print("⚠️  No forecast records to save")
            return False
        
        try:
            print(f"💾 Saving {len(forecast_records)} forecast records...")
            
            # Use upsert to handle duplicates
            response = supabase.table('forecast_data').upsert(
                forecast_records,
                on_conflict='break_id, forecast_date, forecast_time'
            ).execute()
            
            if response.data:
                print(f"✅ Successfully saved {len(response.data)} forecast records")
                return True
            else:
                print("❌ Failed to save forecast data")
                return False
                
        except Exception as e:
            print(f"❌ Error saving forecast data: {str(e)}")
            return False

def get_unique_regions_from_database():
    """Get all unique regions that have surf breaks in the database"""
    try:
        print("🔍 Getting unique regions from database...")
        
        response = supabase.table('surf_breaks').select('region').execute()
        
        if response.data:
            # Get unique regions
            unique_regions = list(set([break_data['region'] for break_data in response.data]))
            print(f"📍 Found {len(unique_regions)} unique regions: {unique_regions}")
            
            # Filter to only regions we have location IDs for
            valid_regions = [region for region in unique_regions if region in AUSTRALIAN_SURF_LOCATIONS]
            print(f"📍 Valid regions to scrape: {valid_regions}")
            
            return valid_regions
        else:
            print("⚠️  No surf breaks found in database")
            return []
            
    except Exception as e:
        print(f"❌ Error getting regions: {str(e)}")
        return []

def run_scraper():
    """Main scraper function"""
    print("\n" + "="*60)
    print(f"🏄 SURF FORECAST SCRAPER - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    scraper = WillyWeatherScraper()
    
    # Get regions to scrape
    regions_to_scrape = get_unique_regions_from_database()
    
    if not regions_to_scrape:
        print("❌ No valid regions to scrape")
        return
    
    total_saved = 0
    
    # Process each region
    for region in regions_to_scrape:
        print(f"\n🎯 Processing region: {region}")
        print("-" * 40)
        
        # Get all breaks in this region
        all_breaks = scraper.get_all_breaks_by_region(region)
        
        if not all_breaks:
            print(f"⚠️  No breaks found for {region}, skipping...")
            continue
        
        # Get location ID for API call
        location_config = AUSTRALIAN_SURF_LOCATIONS.get(region)
        if not location_config:
            print(f"⚠️  No location ID configured for {region}, skipping...")
            continue
        
        location_id = location_config['location_id']
        
        # Fetch forecast data from API
        api_data = scraper.get_forecast_data(location_id, region)
        
        if not api_data:
            print(f"❌ Failed to get API data for {region}")
            continue
        
        # Process forecast data for ALL breaks in the region
        forecast_records = scraper.process_forecast_data(api_data, region, all_breaks)
        
        if not forecast_records:
            print(f"❌ No forecast records generated for {region}")
            continue
        
        # Save forecast data
        if scraper.save_forecast_data(forecast_records):
            total_saved += len(forecast_records)
            print(f"✅ {region} complete - saved {len(forecast_records)} records")
        else:
            print(f"❌ Failed to save data for {region}")
    
    print(f"\n🎉 Scraper complete! Total records saved: {total_saved}")

def main():
    print("🚀 Starting Individual Break Forecast Scraper")
    
    # Test mode - run once
    if os.getenv("TEST_MODE", "false").lower() == "true":
        print("🧪 TEST MODE - Running once")
        run_scraper()
        return
    
    # Production mode - schedule runs
    print("⏰ PRODUCTION MODE - Scheduling runs")
    
    # Schedule scraper to run every 6 hours
    schedule.every(6).hours.do(run_scraper)
    
    # Run once immediately
    run_scraper()
    
    # Keep running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    main()
