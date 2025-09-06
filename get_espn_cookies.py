#!/usr/bin/env python3
"""
Helper script to get ESPN authentication cookies.

Instructions:
1. Go to https://fantasy.espn.com in your browser
2. Log in with your ESPN account
3. Open Developer Tools (F12)
4. Go to Network tab
5. Make any request to fantasy.espn.com
6. Look at the request headers for 'Cookie' 
7. Find the espn_s2 and SWID values
8. Add them to your .env file

Example cookie values:
espn_s2=ABC123...XYZ
SWID={1234-5678-9ABC-DEF0}
"""

import requests

def test_espn_league_public_access(league_id: int, year: int = 2025):
    """Test if an ESPN league is publicly accessible"""
    
    print(f"Testing public access to ESPN league {league_id} for year {year}")
    
    # Try the ESPN API directly
    url = f"https://fantasy.espn.com/apis/v3/games/ffl/seasons/{year}/segments/0/leagues/{league_id}"
    
    try:
        response = requests.get(url, timeout=10)
        print(f"HTTP Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            league_name = data.get('settings', {}).get('name', 'Unknown')
            print(f"✓ League found: {league_name}")
            print(f"✓ League is publicly accessible")
            return True
        elif response.status_code == 401:
            print("❌ League requires authentication (private league)")
            print("You need to get ESPN_S2 and SWID cookies from your browser")
            return False
        elif response.status_code == 404:
            print("❌ League not found - check the league ID")
            return False
        else:
            print(f"❌ Unexpected response: {response.status_code}")
            print(f"Response: {response.text[:200]}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing league access: {e}")
        return False

def print_cookie_instructions():
    """Print instructions for getting ESPN cookies"""
    
    print("\n" + "="*60)
    print("HOW TO GET ESPN AUTHENTICATION COOKIES")
    print("="*60)
    print("""
1. Open https://fantasy.espn.com in your browser
2. Log in with your ESPN account 
3. Navigate to your fantasy league
4. Open Developer Tools (F12 or right-click → Inspect)
5. Go to the Network tab
6. Refresh the page or click around the league
7. Look for any request to fantasy.espn.com
8. Click on the request and look at Request Headers
9. Find the 'Cookie' header
10. Look for these two values in the cookie string:
    - espn_s2=LONG_STRING_HERE
    - SWID={GUID-LIKE-STRING}

Example:
espn_s2=ABC123DEF456GHI789JKL012MNO345...
SWID={12345678-ABCD-EFGH-IJKL-123456789012}

11. Add these to your .env file:
ESPN_S2=your_espn_s2_value_here
SWID=your_swid_value_here
""")

if __name__ == "__main__":
    league_id = 2046935275
    
    print("ESPN League Access Tester")
    print("=" * 40)
    
    success = test_espn_league_public_access(league_id)
    
    if not success:
        print_cookie_instructions()
    
    print(f"\nTest completed. League accessible: {'YES' if success else 'NO'}")