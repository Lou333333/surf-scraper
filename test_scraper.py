import asyncio
import os
from dotenv import load_dotenv
from scraper import WillyWeatherScraper, AUSTRALIAN_SURF_LOCATIONS

# Load environment variables
load_dotenv()

async def test_single_location():
    """Test scraper on a single location"""
    print("🧪 Testing WillyWeather scraper...")
    
    # Check if API key is set
    api_key = os.getenv("WILLY_WEATHER_API_KEY")
    if not api_key:
        print("❌ WILLY_WEATHER_API_KEY not found in .env file")
        return
    
    print(f"✅ API Key found: {api_key[:10]}...")
    
    try:
        scraper = WillyWeatherScraper()
        
        # Test with Wollongong (location ID 17663)
        test_region = "Wollongong"
        location_info = AUSTRALIAN_SURF_LOCATIONS[test_region]
        location_id = location_info['location_id']
        
        print(f"🌊 Testing {test_region} (ID: {location_id})")
        
        result = scraper.get_forecast_data(location_id, test_region)
        
        if result:
            print("✅ Scraper test successful!")
            print("📊 Forecast data received:")
            for key, value in result.items():
                print(f"   {key}: {value}")
        else:
            print("❌ No data scraped")
            
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")

async def test_api_connection():
    """Test API connection with a simple request"""
    import requests
    
    api_key = os.getenv("WILLY_WEATHER_API_KEY")
    if not api_key:
        print("❌ No API key found")
        return
    
    try:
        # Simple test request
        url = f"https://api.willyweather.com.au/v2/{api_key}/locations/17663/weather.json"
        params = {'forecasts': 'swell,wind,tides', 'days': 1}
        
        print("🔗 Testing API connection...")
        response = requests.get(url, params=params, timeout=10)
        
        print(f"📡 Response status: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ API connection successful!")
            data = response.json()
            print(f"📍 Location: {data.get('location', {}).get('name', 'Unknown')}")
        else:
            print(f"❌ API error: {response.text}")
            
    except Exception as e:
        print(f"❌ Connection test failed: {str(e)}")

if __name__ == "__main__":
    print("🧪 Running WillyWeather scraper tests...\n")
    
    # Test 1: API Connection
    asyncio.run(test_api_connection())
    print("\n" + "="*50 + "\n")
    
    # Test 2: Full scraper test
    asyncio.run(test_single_location())