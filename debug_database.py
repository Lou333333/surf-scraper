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

def check_database_urls():
    """Check what URLs are actually in the database"""
    try:
        print("🔍 Checking URLs currently in database...")
        
        # Get all surf breaks
        response = supabase.table('surf_breaks').select('name, region, swellnet_url').execute()
        
        print(f"Found {len(response.data)} surf breaks:\n")
        
        for break_data in response.data:
            name = break_data['name']
            region = break_data['region']
            url = break_data['swellnet_url']
            
            print(f"📍 {name} ({region})")
            print(f"   URL: {url}")
            
            # Check if it's the correct format
            if url and 'swell.willyweather.com.au' in url:
                print(f"   ✅ Correct WillyWeather swell URL")
            elif url and 'willyweather.com' in url:
                print(f"   ⚠️  WillyWeather URL but not swell format")
            elif url and 'swellnet.com' in url:
                print(f"   ❌ Still using old Swellnet URL")
            else:
                print(f"   ❓ Unknown URL format")
            print()
        
    except Exception as e:
        print(f"❌ Error checking database: {str(e)}")

if __name__ == "__main__":
    check_database_urls()