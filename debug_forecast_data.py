#!/usr/bin/env python3

import os
from datetime import datetime, date
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Supabase
SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ Missing Supabase credentials in .env")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def debug_forecast_data():
    """Debug forecast data availability"""
    print("🔍 DEBUGGING FORECAST DATA AVAILABILITY")
    print("=" * 50)
    
    try:
        # Check what breaks exist
        breaks_response = supabase.table('surf_breaks').select('*').execute()
        
        if not breaks_response.data:
            print("❌ No surf breaks found in database")
            return
        
        print(f"📍 Found {len(breaks_response.data)} surf breaks:")
        for break_data in breaks_response.data:
            print(f"  - {break_data['name']} ({break_data['region']}) - ID: {break_data['id']}")
        
        print("\n" + "=" * 50)
        
        # Check forecast data for each break
        for break_data in breaks_response.data:
            print(f"\n🔍 CHECKING FORECAST DATA FOR: {break_data['name']} ({break_data['region']})")
            print("-" * 40)
            
            # Get all forecast data for this break
            forecast_response = supabase.table('forecast_data').select('*').eq('break_id', break_data['id']).execute()
            
            if not forecast_response.data:
                print(f"❌ No forecast data found for {break_data['name']}")
                continue
            
            print(f"✅ Found {len(forecast_response.data)} forecast records")
            
            # Get today's data
            today = date.today().isoformat()
            current_hour = datetime.now().hour
            
            print(f"📅 Today: {today}")
            print(f"🕐 Current hour: {current_hour}")
            
            # Determine expected time slot
            time_slots = ['6am', '8am', '10am', '12pm', '2pm', '4pm', '6pm']
            expected_time = '6am'  # Default
            
            if 6 <= current_hour < 8: expected_time = '6am'
            elif 8 <= current_hour < 10: expected_time = '8am'
            elif 10 <= current_hour < 12: expected_time = '10am'
            elif 12 <= current_hour < 14: expected_time = '12pm'
            elif 14 <= current_hour < 16: expected_time = '2pm'
            elif 16 <= current_hour < 18: expected_time = '4pm'
            elif 18 <= current_hour < 20: expected_time = '6pm'
            
            print(f"🎯 Expected time slot: {expected_time}")
            
            # Check for today's data
            today_forecasts = [f for f in forecast_response.data if f['forecast_date'] == today]
            print(f"📊 Today's forecasts: {len(today_forecasts)}")
            
            if today_forecasts:
                print("⏰ Available times today:")
                for forecast in today_forecasts:
                    time_str = forecast['forecast_time']
                    swell_height = forecast.get('swell_height', 'N/A')
                    wind_speed = forecast.get('wind_speed', 'N/A')
                    print(f"  - {time_str}: Swell {swell_height}ft, Wind {wind_speed}kt")
                
                # Check for current time slot
                current_forecast = next((f for f in today_forecasts if f['forecast_time'] == expected_time), None)
                
                if current_forecast:
                    print(f"✅ Found current forecast for {expected_time}:")
                    print(f"   Swell Height: {current_forecast.get('swell_height')}ft")
                    print(f"   Wind Speed: {current_forecast.get('wind_speed')}kt")
                    print(f"   Swell Period: {current_forecast.get('swell_period')}s")
                    print(f"   Swell Direction: {current_forecast.get('swell_direction')}")
                else:
                    print(f"❌ No current forecast found for {expected_time}")
                    print(f"   Available times: {[f['forecast_time'] for f in today_forecasts]}")
            else:
                print(f"❌ No forecasts for today ({today})")
                
                # Show latest available dates
                all_dates = sorted(list(set([f['forecast_date'] for f in forecast_response.data])))
                print(f"📅 Available dates: {all_dates[-5:] if len(all_dates) > 5 else all_dates}")
    
    except Exception as e:
        print(f"❌ Error debugging forecast data: {str(e)}")

def test_predictions_query():
    """Test the exact query used by predictions page"""
    print("\n🧪 TESTING PREDICTIONS PAGE QUERY")
    print("=" * 50)
    
    try:
        # Get the first break
        breaks_response = supabase.table('surf_breaks').select('*').limit(1).execute()
        
        if not breaks_response.data:
            print("❌ No breaks to test")
            return
            
        break_data = breaks_response.data[0]
        print(f"🎯 Testing with break: {break_data['name']} ({break_data['region']})")
        
        # Replicate predictions page logic
        today = date.today().isoformat()
        current_hour = datetime.now().hour
        
        time_of_day = '6am'
        if current_hour >= 8 and current_hour < 10: time_of_day = '8am'
        elif current_hour >= 10 and current_hour < 12: time_of_day = '10am'
        elif current_hour >= 12 and current_hour < 14: time_of_day = '12pm'
        elif current_hour >= 14 and current_hour < 16: time_of_day = '2pm'
        elif current_hour >= 16 and current_hour < 18: time_of_day = '4pm'
        elif current_hour >= 18 and current_hour < 20: time_of_day = '6pm'
        
        print(f"📅 Query params:")
        print(f"   Date: {today}")
        print(f"   Time: {time_of_day}")
        print(f"   Break ID: {break_data['id']}")
        
        # Run the exact query from predictions page
        current_forecast_response = supabase.table('forecast_data').select('*').eq('break_id', break_data['id']).eq('forecast_date', today).eq('forecast_time', time_of_day).execute()
        
        print(f"\n📊 Query result:")
        if current_forecast_response.data:
            print(f"✅ Found {len(current_forecast_response.data)} records")
            for record in current_forecast_response.data:
                print(f"   Swell: {record.get('swell_height')}ft")
                print(f"   Wind: {record.get('wind_speed')}kt")
                print(f"   Period: {record.get('swell_period')}s")
                print(f"   Direction: {record.get('swell_direction')}")
        else:
            print("❌ No records found with exact query")
            
            # Try broader search
            print("\n🔍 Trying broader search...")
            
            # All data for this break today
            broad_response = supabase.table('forecast_data').select('*').eq('break_id', break_data['id']).eq('forecast_date', today).execute()
            
            if broad_response.data:
                print(f"📅 Found {len(broad_response.data)} records for today:")
                for record in broad_response.data:
                    print(f"   {record['forecast_time']}: {record.get('swell_height')}ft swell")
            else:
                print("❌ No records for today at all")
                
                # Check any data for this break
                any_response = supabase.table('forecast_data').select('*').eq('break_id', break_data['id']).limit(5).execute()
                
                if any_response.data:
                    print(f"📊 Found {len(any_response.data)} records total (showing first 5):")
                    for record in any_response.data:
                        print(f"   {record['forecast_date']} {record['forecast_time']}: {record.get('swell_height')}ft")
                else:
                    print("❌ No forecast data at all for this break")
    
    except Exception as e:
        print(f"❌ Error testing predictions query: {str(e)}")

def check_data_structure():
    """Check the structure of existing forecast data"""
    print("\n🏗️  CHECKING DATA STRUCTURE")
    print("=" * 50)
    
    try:
        # Get a sample of forecast data
        sample_response = supabase.table('forecast_data').select('*').limit(5).execute()
        
        if not sample_response.data:
            print("❌ No forecast data to examine")
            return
        
        print(f"📊 Examining {len(sample_response.data)} sample records:")
        
        for i, record in enumerate(sample_response.data):
            print(f"\n📋 Record {i+1}:")
            for key, value in record.items():
                print(f"   {key}: {value} ({type(value).__name__})")
    
    except Exception as e:
        print(f"❌ Error checking data structure: {str(e)}")

if __name__ == "__main__":
    print("🔧 SURF FORECAST DATA DIAGNOSTIC TOOL")
    print("🔧 " + "=" * 48)
    
    debug_forecast_data()
    test_predictions_query()
    check_data_structure()
    
    print("\n" + "=" * 50)
    print("🏁 Diagnostic complete!")
    print("\nNEXT STEPS:")
    print("1. If no current forecast data: Run the scraper to get today's data")
    print("2. If time format mismatch: Check forecast_time format consistency")
    print("3. If break ID mismatch: Verify break IDs are correct")
    print("4. If data is old: Scraper may need to run more frequently")