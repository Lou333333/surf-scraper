#!/usr/bin/env python3
"""
Debug script to check environment variables
"""

import os
from dotenv import load_dotenv

print("🔍 Debug: Environment Variables Loading")
print("=" * 50)

# Check if .env file exists
env_file_exists = os.path.exists('.env')
print(f"📁 .env file exists: {env_file_exists}")

if env_file_exists:
    with open('.env', 'r') as f:
        content = f.read()
        print(f"📄 .env file content ({len(content)} characters):")
        # Show first few characters of each line (hide sensitive data)
        for i, line in enumerate(content.split('\n')):
            if line.strip():
                if '=' in line:
                    key, value = line.split('=', 1)
                    print(f"  Line {i+1}: {key}={value[:10]}{'...' if len(value) > 10 else ''}")
                else:
                    print(f"  Line {i+1}: {line}")

print("\n🔧 Loading .env file...")
load_result = load_dotenv()
print(f"✅ load_dotenv() returned: {load_result}")

print("\n🌍 Environment variables after loading:")
supabase_url = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
supabase_key = os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY") 
willy_key = os.getenv("WILLY_WEATHER_API_KEY")

print(f"NEXT_PUBLIC_SUPABASE_URL: {'✅ Found' if supabase_url else '❌ Missing'}")
if supabase_url:
    print(f"  Value: {supabase_url[:30]}...")

print(f"NEXT_PUBLIC_SUPABASE_ANON_KEY: {'✅ Found' if supabase_key else '❌ Missing'}")
if supabase_key:
    print(f"  Value: {supabase_key[:30]}...")

print(f"WILLY_WEATHER_API_KEY: {'✅ Found' if willy_key else '❌ Missing'}")
if willy_key:
    print(f"  Value: {willy_key[:20]}...")

print("\n🔍 All environment variables starting with 'NEXT_' or 'WILLY':")
for key, value in os.environ.items():
    if key.startswith(('NEXT_', 'WILLY')):
        print(f"  {key}: {value[:20]}...")

print("\n💡 Recommendations:")
if not supabase_url:
    print("❌ Add NEXT_PUBLIC_SUPABASE_URL to your .env file")
if not supabase_key:
    print("❌ Add NEXT_PUBLIC_SUPABASE_ANON_KEY to your .env file")  
if not willy_key:
    print("❌ Add WILLY_WEATHER_API_KEY to your .env file")

if all([supabase_url, supabase_key, willy_key]):
    print("✅ All required environment variables are present!")