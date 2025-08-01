#!/usr/bin/env python3
"""
Debug script to fix scraper database issues
"""

import os
import requests
from supabase import create_client, Client
from dotenv import load_dotenv
import uuid

# Load environment variables
load_dotenv()

def test_database_structure():
    """Check the actual database structure and existing data"""
    print("🔍 Checking database structure...")
    
    try:
        supabase: Client = create_client(
            os.getenv("NEXT_PUBLIC_SUPABASE_URL"),
            os.getenv("SUPABASE_SERVICE_KEY")  # Use service key for full access
        )
        
        # Check surf_breaks table
        print("\n📋 SURF BREAKS TABLE:")
        breaks_response = supabase.table('surf_breaks').select('id, name, region').limit(5).execute()
        
        if breaks_response.data:
            for break_data in breaks_response.data:
                print(f"  ID: {break_data['id']} | Name: {break_data['name']} | Region: {break_data['region']}")
                print(f"      UUID type: {type(break_data['id'])}")
        else:
            print("  No surf breaks found")
        
        # Check forecast_data table structure
        print("\n📊 FORECAST DATA TABLE:")
        forecast_response = supabase.table('forecast_data').select('*').limit(3).execute()
        
        if forecast_response.data:
            print(f"  Found {len(forecast_response.data)} existing forecast records")
            for record in forecast_response.data:
                print(f"  Break ID: {record.get('break_id')} | Date: {record.get('forecast_date')} | Time: {record.get('forecast_time')}")
        else:
            print("  No forecast data found")
            
        return True
        
    except Exception as e:
        print(f"❌ Database structure check failed: {str(e)}")
        return False

def test_uuid_insertion():
    """Test inserting forecast data with proper UUIDs"""
    print("\n🧪 Testing UUID insertion...")
    
    try:
        supabase: Client = create_client(
            os.getenv("NEXT_PUBLIC_SUPABASE_URL"),
            os.getenv("SUPABASE_SERVICE_KEY")
        )
        
        # Get a real break UUID from Wollongong
        breaks_response = supabase.table('surf_breaks').select('id, region').eq('region', 'Wollongong').limit(1).execute()
        
        if not breaks_response.data:
            print("❌ No Wollongong break found in database")
            return False
        
        break_uuid = breaks_response.data[0]['id']
        print(f"✅ Found Wollongong break UUID: {break_uuid}")
        
        # Create test forecast record with proper UUID
        test_forecast = {
            'break_id': break_uuid,  # Use actual UUID
            'forecast_date': '2025-08-01',
            'forecast_time': '6am',
            'swell_height': 2.2,
            'swell_direction': 180,
            'swell_period': 8,
            'wind_speed': 15,
            'wind_direction': 90,
            'tide_height': 1.2
        }
        
        print(f"📤 Attempting to insert test record...")
        print(f"   Break ID: {test_forecast['break_id']} (type: {type(test_forecast['break_id'])})")
        
        response = supabase.table('forecast_data').upsert(
            test_forecast,
            on_conflict='break_id, forecast_date, forecast_time'
        ).execute()
        
        if response.data:
            print("✅ Test forecast data inserted successfully!")
            print(f"📊 Inserted record: {response.data[0]}")
            return True
        else:
            print("❌ No data returned from insert")
            return False
        
    except Exception as e:
        print(f"❌ UUID insertion test failed: {str(e)}")
        return False

def test_willyweather_api():
    """Test WillyWeather API with debug info"""
    print("\n🌊 Testing WillyWeather API...")
    
    api_key = os.getenv("WILLY_WEATHER_API_KEY")
    if not api_key:
        print("❌ WILLY_WEATHER_API_KEY not found")
        return False
    
    try:
        # Test Gold Coast (the one failing)
        print("Testing Gold Coast (the problematic one)...")
        url = f"https://api.willyweather.com.au/v2/{api_key}/locations/4958/weather.json"
        params = {
            'forecasts': 'swell,wind,tides',
            'days': 1
        }
        
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Gold Coast API call successful!")
            
            forecasts = data.get('forecasts', {})
            swell_data = forecasts.get('swell')
            
            print(f"🔍 Forecast keys: {list(forecasts.keys())}")
            print(f"🔍 Swell data type: {type(swell_data)}")
            
            if swell_data is None:
                print("❌ Swell data is None - this explains the error!")
                print("🔍 Full forecast structure:")
                for key, value in forecasts.items():
                    print(f"   {key}: {type(value)}")
            else:
                print(f"✅ Swell data available: {list(swell_data.keys()) if isinstance(swell_data, dict) else 'Not a dict'}")
            
            return True
        else:
            print(f"❌ API returned status {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ WillyWeather API test failed: {str(e)}")
        return False

def create_missing_breaks():
    """Create any missing surf breaks in the database"""
    print("\n🏗️  Creating missing surf breaks...")
    
    # Regions that should exist
    required_regions = ["Wollongong", "South Coast", "Gold Coast"]
    
    try:
        supabase: Client = create_client(
            os.getenv("NEXT_PUBLIC_SUPABASE_URL"),
            os.getenv("SUPABASE_SERVICE_KEY")
        )
        
        # Check which regions exist
        existing_response = supabase.table('surf_breaks').select('region').execute()
        existing_regions = [b['region'] for b in existing_response.data] if existing_response.data else []
        
        missing_regions = [r for r in required_regions if r not in existing_regions]
        
        if not missing_regions:
            print("✅ All required regions already exist")
            return True
        
        print(f"📝 Creating {len(missing_regions)} missing regions: {missing_regions}")
        
        # You'll need a user ID - get the first user or create test user
        users_response = supabase.table('profiles').select('id').limit(1).execute()
        if not users_response.data:
            print("❌ No users found - create a user account first")
            return False
        
        user_id = users_response.data[0]['id']
        
        # Create missing breaks
        for region in missing_regions:
            new_break = {
                'name': f"{region} Main Break",
                'region': region,
                'user_id': user_id,
                'swellnet_url': f"https://swell.willyweather.com.au/{region.lower().replace(' ', '-')}.html"
            }
            
            result = supabase.table('surf_breaks').insert(new_break).execute()
            if result.data:
                print(f"✅ Created break for {region}")
            else:
                print(f"❌ Failed to create break for {region}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating missing breaks: {str(e)}")
        return False

def main():
    print("🔧 SURF SCRAPER DEBUG & FIX TOOL\n")
    
    # Test 1: Database structure
    print("="*60)
    db_ok = test_database_structure()
    
    # Test 2: Create missing breaks if needed
    print("\n" + "="*60)
    create_missing_breaks()
    
    # Test 3: UUID insertion
    print("\n" + "="*60)
    uuid_ok = test_uuid_insertion()
    
    # Test 4: WillyWeather API with debug
    print("\n" + "="*60)
    api_ok = test_willyweather_api()
    
    # Summary
    print("\n" + "="*60)
    print("🏁 DEBUG SUMMARY:")
    print(f"  Database Structure: {'✅' if db_ok else '❌'}")
    print(f"  UUID Insertion: {'✅' if uuid_ok else '❌'}")
    print(f"  WillyWeather API: {'✅' if api_ok else '❌'}")
    
    if all([db_ok, uuid_ok, api_ok]):
        print("\n🎉 All tests passed! Your scraper should work now.")
        print("\n📝 Next steps:")
        print("1. Deploy the fixed scraper.py to Railway")
        print("2. Set TEST_MODE=true in Railway environment")
        print("3. Check the logs")
        print("4. Set TEST_MODE=false for production")
    else:
        print("\n⚠️  Some issues found. Review the errors above.")

if __name__ == "__main__":
    main()