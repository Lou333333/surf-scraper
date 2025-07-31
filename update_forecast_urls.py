from supabase import create_client, Client
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Initialize Supabase client
supabase: Client = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

# CORRECT WillyWeather URLs (swell forecast pages)
WILLY_WEATHER_URLS = {
    # New South Wales
    "Sydney": "https://swell.willyweather.com.au/nsw/sydney.html",
    "Central Coast": "https://swell.willyweather.com.au/nsw/central-coast.html",
    "Newcastle": "https://swell.willyweather.com.au/nsw/hunter.html",
    "Mid North Coast": "https://swell.willyweather.com.au/nsw/mid-north-coast.html",
    "Byron Bay": "https://swell.willyweather.com.au/nsw/far-north-coast.html",
    "Wollongong": "https://swell.willyweather.com.au/nsw/illawarra.html",
    "South Coast": "https://swell.willyweather.com.au/nsw/south-coast.html",
    "Far North Coast": "https://swell.willyweather.com.au/nsw/far-north-coast.html",
    
    # Queensland
    "Gold Coast": "https://swell.willyweather.com.au/qld/gold-coast.html",
    "Sunshine Coast": "https://swell.willyweather.com.au/qld/sunshine-coast.html",
    "Fraser Coast": "https://swell.willyweather.com.au/qld/fraser-coast.html",
    "Capricorn Coast": "https://swell.willyweather.com.au/qld/capricornia.html",
    "Mackay": "https://swell.willyweather.com.au/qld/mackay.html",
    "Townsville": "https://swell.willyweather.com.au/qld/townsville.html",
    "Cairns": "https://swell.willyweather.com.au/qld/far-north-queensland.html",
    
    # Victoria
    "Melbourne": "https://swell.willyweather.com.au/vic/melbourne.html",
    "Torquay": "https://swell.willyweather.com.au/vic/surf-coast.html",
    "Phillip Island": "https://swell.willyweather.com.au/vic/gippsland.html",
    "East Gippsland": "https://swell.willyweather.com.au/vic/gippsland.html",
    "West Coast": "https://swell.willyweather.com.au/vic/surf-coast.html",
    
    # South Australia
    "Adelaide": "https://swell.willyweather.com.au/sa/adelaide.html",
    "Fleurieu Peninsula": "https://swell.willyweather.com.au/sa/fleurieu-peninsula.html",
    "Yorke Peninsula": "https://swell.willyweather.com.au/sa/yorke-peninsula.html",
    "Eyre Peninsula": "https://swell.willyweather.com.au/sa/eyre-peninsula.html",
    "Kangaroo Island": "https://swell.willyweather.com.au/sa/kangaroo-island.html",
    
    # Western Australia
    "Perth": "https://swell.willyweather.com.au/wa/perth.html",
    "Margaret River": "https://swell.willyweather.com.au/wa/south-west.html",
    "Geraldton": "https://swell.willyweather.com.au/wa/mid-west.html",
    "Esperance": "https://swell.willyweather.com.au/wa/goldfields-esperance.html",
    "Albany": "https://swell.willyweather.com.au/wa/great-southern.html",
    "Exmouth": "https://swell.willyweather.com.au/wa/pilbara.html",
    "Broome": "https://swell.willyweather.com.au/wa/kimberley.html",
    
    # Tasmania
    "Hobart": "https://swell.willyweather.com.au/tas/hobart.html",
    "Launceston": "https://swell.willyweather.com.au/tas/launceston.html",
    "North West Coast": "https://swell.willyweather.com.au/tas/north-west.html",
    "East Coast": "https://swell.willyweather.com.au/tas/east-coast.html"
}

def update_forecast_urls():
    """Update all surf break URLs to use correct WillyWeather swell forecast pages"""
    try:
        print("🔄 Updating forecast URLs to WillyWeather swell forecasts...")
        
        # Get all surf breaks
        response = supabase.table('surf_breaks').select('*').execute()
        
        updated_count = 0
        for break_data in response.data:
            region = break_data['region']
            willy_url = WILLY_WEATHER_URLS.get(region)
            
            if willy_url:
                # Update the URL
                supabase.table('surf_breaks').update({
                    'swellnet_url': willy_url  # Keep the column name for now
                }).eq('id', break_data['id']).execute()
                
                print(f"✅ Updated {break_data['name']} ({region}) -> {willy_url}")
                updated_count += 1
            else:
                print(f"⚠️  No WillyWeather URL found for {region}")
        
        print(f"\n🎉 Successfully updated {updated_count} surf breaks!")
        print("📱 Forecast links will now open WillyWeather swell forecast pages")
        print("\n💡 Test a few links in your app to make sure they work!")
        
    except Exception as e:
        print(f"❌ Error updating URLs: {str(e)}")

if __name__ == "__main__":
    update_forecast_urls()