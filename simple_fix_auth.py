#!/usr/bin/env python3

import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

# Initialize Supabase
SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fix_break_ownership():
    """Fix break ownership and clean up duplicates"""
    print("🔧 FIXING BREAK OWNERSHIP")
    print("=" * 30)
    
    try:
        # Get all surf breaks
        breaks_response = supabase.table('surf_breaks').select('*').execute()
        
        if not breaks_response.data:
            print("❌ No breaks found")
            return
        
        # Find the working break (has forecast data)
        working_break_id = "72ca1687-3d42-4984-b154-fffabd18c741"  # "Mma" from diagnostic
        working_break = next((b for b in breaks_response.data if b['id'] == working_break_id), None)
        
        if not working_break:
            print("❌ Working break not found")
            return
        
        print(f"✅ Found working break: {working_break['name']} ({working_break['region']})")
        
        # Get the first user from any existing break (they're all your breaks)
        user_id = None
        for break_data in breaks_response.data:
            if break_data.get('user_id'):
                user_id = break_data['user_id']
                break
        
        if not user_id:
            print("❌ No user ID found in existing breaks")
            print("💡 You need to create breaks through your app first")
            return
        
        print(f"👤 Using user ID: {user_id}")
        
        # Update the working break to have a clear name and ensure correct user
        update_response = supabase.table('surf_breaks').update({
            'user_id': user_id,
            'name': 'Wollongong Main Beach',
            'region': 'Wollongong'
        }).eq('id', working_break_id).execute()
        
        if update_response.data:
            print("✅ Updated working break")
        
        # Find and clean up duplicate Wollongong breaks
        wollongong_breaks = [b for b in breaks_response.data if b['region'] == 'Wollongong']
        
        print(f"📍 Found {len(wollongong_breaks)} Wollongong breaks")
        
        for break_data in wollongong_breaks:
            if break_data['id'] != working_break_id:
                # Check if this break has any forecast data
                forecast_check = supabase.table('forecast_data').select('id').eq('break_id', break_data['id']).limit(1).execute()
                
                if not forecast_check.data:
                    print(f"🗑️  Deleting empty break: {break_data['name']}")
                    
                    # Move any surf sessions to working break
                    move_sessions = supabase.table('surf_sessions').update({
                        'break_id': working_break_id
                    }).eq('break_id', break_data['id']).execute()
                    
                    if move_sessions.data:
                        print(f"  📦 Moved {len(move_sessions.data)} sessions to working break")
                    
                    # Delete empty break
                    delete_response = supabase.table('surf_breaks').delete().eq('id', break_data['id']).execute()
                    
                    if delete_response.data:
                        print(f"  ✅ Deleted empty break")
        
        print("✅ Break cleanup complete!")
        
        # Test current forecast lookup
        test_current_forecast(working_break_id)
        
    except Exception as e:
        print(f"❌ Error fixing breaks: {str(e)}")

def test_current_forecast(break_id):
    """Test current forecast lookup"""
    print(f"\n🧪 TESTING CURRENT FORECAST")
    print("=" * 30)
    
    try:
        from datetime import date
        
        today = date.today().isoformat()
        time_slots = ['6am', '8am', '10am', '12pm', '2pm', '4pm', '6pm']
        
        print(f"📅 Testing for date: {today}")
        print(f"🎯 Break ID: {break_id}")
        
        # Try each time slot
        found_forecast = None
        for time_slot in time_slots:
            forecast_response = supabase.table('forecast_data').select('*').eq('break_id', break_id).eq('forecast_date', today).eq('forecast_time', time_slot).execute()
            
            if forecast_response.data:
                found_forecast = forecast_response.data[0]
                print(f"✅ Found forecast for {time_slot}:")
                print(f"   🌊 Swell: {found_forecast.get('swell_height')}ft")
                print(f"   💨 Wind: {found_forecast.get('wind_speed')}kt")
                break
        
        if found_forecast:
            print("\n🎉 SUCCESS! This should now work in your predictions page!")
        else:
            print(f"\n❌ No forecasts found for today")
            
            # Check what dates ARE available
            all_forecasts = supabase.table('forecast_data').select('forecast_date, forecast_time').eq('break_id', break_id).execute()
            
            if all_forecasts.data:
                dates = sorted(list(set([f['forecast_date'] for f in all_forecasts.data])))
                print(f"📅 Available dates: {dates}")
            else:
                print("❌ No forecast data at all for this break")
        
    except Exception as e:
        print(f"❌ Error testing forecast: {str(e)}")

def create_test_session():
    """Create a test surf session if needed"""
    print(f"\n🏄 CREATING TEST SESSION")
    print("=" * 25)
    
    try:
        working_break_id = "72ca1687-3d42-4984-b154-fffabd18c741"
        
        # Get user ID from the working break
        break_response = supabase.table('surf_breaks').select('user_id').eq('id', working_break_id).single().execute()
        
        if not break_response.data or not break_response.data.get('user_id'):
            print("❌ No user ID found for working break")
            return
        
        user_id = break_response.data['user_id']
        
        # Check if user already has sessions for this break
        existing_sessions = supabase.table('surf_sessions').select('id').eq('user_id', user_id).eq('break_id', working_break_id).execute()
        
        if existing_sessions.data and len(existing_sessions.data) > 0:
            print(f"✅ User already has {len(existing_sessions.data)} sessions for this break")
            return
        
        # Create a test session
        test_session = {
            'user_id': user_id,
            'break_id': working_break_id,
            'session_date': '2025-08-01',  # Yesterday
            'session_time': 'morning',
            'rating': 'fun',
            'wave_count': 8,
            'session_duration': 90,
            'notes': 'Test session for predictions'
        }
        
        session_response = supabase.table('surf_sessions').insert(test_session).execute()
        
        if session_response.data:
            print("✅ Created test surf session")
            print("   This will help generate predictions")
        
    except Exception as e:
        print(f"❌ Error creating test session: {str(e)}")

if __name__ == "__main__":
    print("🚀 SIMPLE FIX FOR CURRENT CONDITIONS")
    print("🚀 " + "=" * 35)
    
    fix_break_ownership()
    create_test_session()
    
    print("\n" + "=" * 40)
    print("🎉 FIX COMPLETE!")
    print("\n📱 Now refresh your predictions page.")
    print("   You should see current conditions!")
    print("\n💡 If still showing 'Unknown', the issue is likely")
    print("   that you need more surf sessions logged to generate predictions.")