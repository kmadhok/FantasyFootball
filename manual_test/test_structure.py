#!/usr/bin/env python3
"""
Simple structure test to verify manual testing framework
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def test_imports():
    """Test that all modules can be imported"""
    print("Testing manual testing framework structure...")
    
    try:
        from manual_test.data_models import LeagueInfo, TeamInfo, RosterInfo, WaiverInfo, APIResponse, TestResults
        print("✓ Data models imported successfully")
    except ImportError as e:
        print(f"✗ Data models import failed: {e}")
        return False
    
    # Test data model creation
    try:
        league = LeagueInfo(
            league_id="test",
            name="Test League",
            season="2024",
            total_rosters=10,
            platform="test"
        )
        print(f"✓ Data model creation works: {league}")
    except Exception as e:
        print(f"✗ Data model creation failed: {e}")
        return False
    
    # Test that API client files exist
    sleeper_client_path = Path(__file__).parent / "sleeper_client.py"
    mfl_client_path = Path(__file__).parent / "mfl_client.py"
    comprehensive_test_path = Path(__file__).parent / "comprehensive_test.py"
    
    if sleeper_client_path.exists():
        print("✓ Sleeper client file exists")
    else:
        print("✗ Sleeper client file missing")
        return False
    
    if mfl_client_path.exists():
        print("✓ MFL client file exists")
    else:
        print("✗ MFL client file missing")
        return False
    
    if comprehensive_test_path.exists():
        print("✓ Comprehensive test file exists")
    else:
        print("✗ Comprehensive test file missing")
        return False
    
    return True

def test_configuration():
    """Test configuration loading"""
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        print("✓ .env file found")
        
        # Read .env file
        with open(env_path, 'r') as f:
            content = f.read()
            if "SLEEPER_LEAGUE_ID" in content:
                print("✓ Sleeper league ID configured")
            else:
                print("⚠️  Sleeper league ID not found in .env")
            
            if "MFL_LEAGUE_ID" in content:
                print("✓ MFL league ID configured")
            else:
                print("⚠️  MFL league ID not found in .env")
    else:
        print("⚠️  .env file not found")
    
    return True

def main():
    """Main test function"""
    print("=" * 60)
    print("MANUAL TESTING FRAMEWORK STRUCTURE TEST")
    print("=" * 60)
    
    structure_ok = test_imports()
    config_ok = test_configuration()
    
    print("\n" + "=" * 60)
    print("STRUCTURE TEST SUMMARY")
    print("=" * 60)
    
    if structure_ok and config_ok:
        print("🎉 Manual testing framework structure is complete!")
        print("\nNext steps:")
        print("1. Install required dependencies: pip install requests python-dotenv")
        print("2. Run the comprehensive test: python3 manual_test/comprehensive_test.py")
        print("3. Or run interactively: python3 manual_test/comprehensive_test.py --interactive")
    else:
        print("❌ Structure test failed - check the issues above")
    
    return structure_ok and config_ok

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)