#!/usr/bin/env python3
"""
Comprehensive Manual Testing Script for Fantasy Football APIs
Tests core functionality: league info, team names, roster information, and waiver lists
Uses only official API documentation for Sleeper and MFL
"""
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional
import logging

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import our manual test clients and data models
from manual_test.sleeper_client import SleeperManualTestClient
from manual_test.mfl_client import MFLManualTestClient
from manual_test.data_models import TestResults, LeagueInfo, TeamInfo, RosterInfo, WaiverInfo

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ComprehensiveFantasyTester:
    """Main testing coordinator for both Sleeper and MFL platforms"""
    
    def __init__(self):
        # Load environment variables
        self._load_environment()
        
        # Initialize clients
        self.sleeper_client = None
        self.mfl_client = None
        
        if self.sleeper_league_id:
            self.sleeper_client = SleeperManualTestClient(self.sleeper_league_id)
            
        if self.mfl_league_id:
            self.mfl_client = MFLManualTestClient(self.mfl_league_id)
        
        # Results storage
        self.results = TestResults()
    
    def _load_environment(self):
        """Load environment variables from .env file"""
        env_path = project_root / ".env"
        if env_path.exists():
            from dotenv import load_dotenv
            load_dotenv(env_path)
        
        self.sleeper_league_id = os.getenv("SLEEPER_LEAGUE_ID")
        self.mfl_league_id = os.getenv("MFL_LEAGUE_ID")
        
        print(f"Environment loaded:")
        print(f"  Sleeper League ID: {self.sleeper_league_id}")
        print(f"  MFL League ID: {self.mfl_league_id}")
    
    def test_sleeper_functionality(self) -> bool:
        """Test all Sleeper functionality"""
        if not self.sleeper_client:
            self.results.add_error("Sleeper client not available (missing league ID)")
            return False
        
        print("\n" + "=" * 60)
        print("TESTING SLEEPER FUNCTIONALITY")
        print("=" * 60)
        
        success = True
        
        # Test 1: League Information
        print("\n1. Testing Sleeper League Information...")
        league_response = self.sleeper_client.get_league_info()
        if league_response.success:
            self.results.league_info = league_response.data
            print(f"   ✓ Success: {league_response.data}")
        else:
            self.results.add_error(f"Sleeper league info failed: {league_response.error_message}")
            print(f"   ✗ Failed: {league_response.error_message}")
            success = False
        
        # Test 2: Team Names (Users)
        print("\n2. Testing Sleeper Team Names...")
        users_response = self.sleeper_client.get_users()
        if users_response.success:
            sleeper_teams = users_response.data
            self.results.teams.extend(sleeper_teams)
            print(f"   ✓ Success: Found {len(sleeper_teams)} teams")
            for team in sleeper_teams[:5]:  # Show first 5
                print(f"     - {team}")
            if len(sleeper_teams) > 5:
                print(f"     ... and {len(sleeper_teams) - 5} more")
        else:
            self.results.add_error(f"Sleeper teams failed: {users_response.error_message}")
            print(f"   ✗ Failed: {users_response.error_message}")
            success = False
        
        # Test 3: Roster Information
        print("\n3. Testing Sleeper Roster Information...")
        rosters_response = self.sleeper_client.get_rosters()
        if rosters_response.success:
            sleeper_rosters = rosters_response.data
            self.results.rosters.extend(sleeper_rosters)
            print(f"   ✓ Success: Found {len(sleeper_rosters)} rosters")
            for roster in sleeper_rosters[:3]:  # Show first 3
                print(f"     - {roster}")
            if len(sleeper_rosters) > 3:
                print(f"     ... and {len(sleeper_rosters) - 3} more")
        else:
            self.results.add_error(f"Sleeper rosters failed: {rosters_response.error_message}")
            print(f"   ✗ Failed: {rosters_response.error_message}")
            success = False
        
        # Test 4: Waiver Information
        print("\n4. Testing Sleeper Waiver Information...")
        waivers_response = self.sleeper_client.get_waiver_orders()
        if waivers_response.success:
            sleeper_waivers = waivers_response.data
            self.results.waivers.extend(sleeper_waivers)
            print(f"   ✓ Success: Found {len(sleeper_waivers)} waiver claims")
            for waiver in sleeper_waivers[:3]:  # Show first 3
                print(f"     - {waiver}")
            if len(sleeper_waivers) > 3:
                print(f"     ... and {len(sleeper_waivers) - 3} more")
        else:
            self.results.add_error(f"Sleeper waivers failed: {waivers_response.error_message}")
            print(f"   ✗ Failed: {waivers_response.error_message}")
            # Don't mark as failure since waivers might be empty
        
        return success
    
    def test_mfl_functionality(self) -> bool:
        """Test all MFL functionality"""
        if not self.mfl_client:
            self.results.add_error("MFL client not available (missing league ID)")
            return False
        
        print("\n" + "=" * 60)
        print("TESTING MFL FUNCTIONALITY")
        print("=" * 60)
        
        success = True
        
        # Test 1: League Information
        print("\n1. Testing MFL League Information...")
        league_response = self.mfl_client.get_league_info()
        if league_response.success:
            # If we don't have league info yet, use MFL's
            if not self.results.league_info:
                self.results.league_info = league_response.data
            print(f"   ✓ Success: {league_response.data}")
        else:
            self.results.add_error(f"MFL league info failed: {league_response.error_message}")
            print(f"   ✗ Failed: {league_response.error_message}")
            success = False
        
        # Test 2: Team Names (Franchises)
        print("\n2. Testing MFL Team Names...")
        franchises_response = self.mfl_client.get_franchises()
        if franchises_response.success:
            mfl_teams = franchises_response.data
            self.results.teams.extend(mfl_teams)
            print(f"   ✓ Success: Found {len(mfl_teams)} franchises")
            for team in mfl_teams[:5]:  # Show first 5
                print(f"     - {team}")
            if len(mfl_teams) > 5:
                print(f"     ... and {len(mfl_teams) - 5} more")
        else:
            self.results.add_error(f"MFL franchises failed: {franchises_response.error_message}")
            print(f"   ✗ Failed: {franchises_response.error_message}")
            success = False
        
        # Test 3: Roster Information
        print("\n3. Testing MFL Roster Information...")
        rosters_response = self.mfl_client.get_rosters()
        if rosters_response.success:
            mfl_rosters = rosters_response.data
            self.results.rosters.extend(mfl_rosters)
            print(f"   ✓ Success: Found {len(mfl_rosters)} rosters")
            for roster in mfl_rosters[:3]:  # Show first 3
                print(f"     - {roster}")
            if len(mfl_rosters) > 3:
                print(f"     ... and {len(mfl_rosters) - 3} more")
        else:
            self.results.add_error(f"MFL rosters failed: {rosters_response.error_message}")
            print(f"   ✗ Failed: {rosters_response.error_message}")
            success = False
        
        # Test 4: FAAB Balances
        print("\n4. Testing MFL FAAB Balances...")
        faab_response = self.mfl_client.get_faab_balances()
        if faab_response.success:
            mfl_faab = faab_response.data
            self.results.waivers.extend(mfl_faab)
            print(f"   ✓ Success: Found {len(mfl_faab)} FAAB balances")
            for faab in mfl_faab[:5]:  # Show first 5
                print(f"     - {faab}")
            if len(mfl_faab) > 5:
                print(f"     ... and {len(mfl_faab) - 5} more")
        else:
            self.results.add_error(f"MFL FAAB failed: {faab_response.error_message}")
            print(f"   ✗ Failed: {faab_response.error_message}")
            # Don't mark as failure since FAAB might not be enabled
        
        # Test 5: Transactions
        print("\n5. Testing MFL Transactions...")
        transactions_response = self.mfl_client.get_transactions()
        if transactions_response.success:
            mfl_transactions = transactions_response.data
            self.results.waivers.extend(mfl_transactions)
            print(f"   ✓ Success: Found {len(mfl_transactions)} transactions")
            for transaction in mfl_transactions[:3]:  # Show first 3
                print(f"     - {transaction}")
            if len(mfl_transactions) > 3:
                print(f"     ... and {len(mfl_transactions) - 3} more")
        else:
            self.results.add_error(f"MFL transactions failed: {transactions_response.error_message}")
            print(f"   ✗ Failed: {transactions_response.error_message}")
            # Don't mark as failure since transactions might be empty
        
        return success
    
    def run_comprehensive_test(self) -> TestResults:
        """Run comprehensive tests for both platforms"""
        print("COMPREHENSIVE FANTASY FOOTBALL API TESTING")
        print("=" * 60)
        print("Testing core functionality using official API documentation:")
        print("- League information")
        print("- Team names") 
        print("- Roster information")
        print("- Waiver/transaction data")
        
        sleeper_success = False
        mfl_success = False
        
        # Test Sleeper
        if self.sleeper_client:
            sleeper_success = self.test_sleeper_functionality()
        else:
            print("\nSkipping Sleeper tests (no league ID configured)")
        
        # Test MFL
        if self.mfl_client:
            mfl_success = self.test_mfl_functionality()
        else:
            print("\nSkipping MFL tests (no league ID configured)")
        
        # Print summary
        self.print_summary(sleeper_success, mfl_success)
        
        return self.results
    
    def print_summary(self, sleeper_success: bool, mfl_success: bool):
        """Print comprehensive test summary"""
        print("\n" + "=" * 60)
        print("COMPREHENSIVE TEST SUMMARY")
        print("=" * 60)
        
        print(f"\nResults Summary:")
        print(f"  League Info: {'✓' if self.results.league_info else '✗'}")
        print(f"  Teams Found: {len(self.results.teams)}")
        print(f"  Rosters Found: {len(self.results.rosters)}")
        print(f"  Waiver/FAAB Records: {len(self.results.waivers)}")
        print(f"  Errors: {len(self.results.errors)}")
        
        print(f"\nPlatform Status:")
        print(f"  Sleeper: {'✓ SUCCESS' if sleeper_success else '✗ FAILED' if self.sleeper_client else '- SKIPPED'}")
        print(f"  MFL: {'✓ SUCCESS' if mfl_success else '✗ FAILED' if self.mfl_client else '- SKIPPED'}")
        
        if self.results.errors:
            print(f"\nErrors Encountered:")
            for error in self.results.errors:
                print(f"  ✗ {error}")
        
        print(f"\nDetailed Summary:")
        print(self.results.summary())
        
        # Overall success determination
        platforms_tested = sum([bool(self.sleeper_client), bool(self.mfl_client)])
        platforms_successful = sum([sleeper_success, mfl_success])
        
        if platforms_tested == 0:
            print(f"\n⚠️  NO PLATFORMS TESTED - Check your .env configuration")
        elif platforms_successful == platforms_tested:
            print(f"\n🎉 ALL TESTS SUCCESSFUL! ({platforms_successful}/{platforms_tested} platforms)")
        elif platforms_successful > 0:
            print(f"\n⚠️  PARTIAL SUCCESS ({platforms_successful}/{platforms_tested} platforms)")
        else:
            print(f"\n❌ ALL TESTS FAILED ({platforms_successful}/{platforms_tested} platforms)")

def interactive_menu():
    """Interactive menu for running specific tests"""
    tester = ComprehensiveFantasyTester()
    
    while True:
        print("\n" + "=" * 60)
        print("FANTASY FOOTBALL API TESTING MENU")
        print("=" * 60)
        print("1. Run Comprehensive Test (Both Platforms)")
        print("2. Test Sleeper Only")
        print("3. Test MFL Only")
        print("4. Show Current Configuration")
        print("5. Export Results to File")
        print("0. Exit")
        print("-" * 60)
        
        choice = input("Enter your choice (0-5): ").strip()
        
        if choice == "1":
            tester.run_comprehensive_test()
        elif choice == "2":
            if tester.sleeper_client:
                tester.test_sleeper_functionality()
            else:
                print("Sleeper client not available (missing league ID)")
        elif choice == "3":
            if tester.mfl_client:
                tester.test_mfl_functionality()
            else:
                print("MFL client not available (missing league ID)")
        elif choice == "4":
            print(f"\nCurrent Configuration:")
            print(f"  Sleeper League ID: {tester.sleeper_league_id or 'Not configured'}")
            print(f"  MFL League ID: {tester.mfl_league_id or 'Not configured'}")
            print(f"  Sleeper Client: {'Available' if tester.sleeper_client else 'Not available'}")
            print(f"  MFL Client: {'Available' if tester.mfl_client else 'Not available'}")
        elif choice == "5":
            export_results(tester.results)
        elif choice == "0":
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")

def export_results(results: TestResults, filename: str = None):
    """Export test results to a file"""
    if not filename:
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"test_results_{timestamp}.txt"
    
    try:
        with open(filename, 'w') as f:
            f.write("FANTASY FOOTBALL API TEST RESULTS\n")
            f.write("=" * 60 + "\n\n")
            f.write(results.summary())
            f.write(f"\n\nDetailed Results:\n")
            f.write(f"League Info: {results.league_info}\n")
            f.write(f"Teams ({len(results.teams)}):\n")
            for team in results.teams:
                f.write(f"  - {team}\n")
            f.write(f"Rosters ({len(results.rosters)}):\n")
            for roster in results.rosters:
                f.write(f"  - {roster}\n")
            f.write(f"Waivers/FAAB ({len(results.waivers)}):\n")
            for waiver in results.waivers:
                f.write(f"  - {waiver}\n")
            if results.errors:
                f.write(f"Errors ({len(results.errors)}):\n")
                for error in results.errors:
                    f.write(f"  - {error}\n")
        
        print(f"Results exported to: {filename}")
    except Exception as e:
        print(f"Failed to export results: {e}")

def main():
    """Main entry point"""
    if len(sys.argv) > 1 and sys.argv[1] == "--interactive":
        interactive_menu()
    else:
        # Run comprehensive test by default
        tester = ComprehensiveFantasyTester()
        results = tester.run_comprehensive_test()
        
        # Optionally export results
        export_results(results)

if __name__ == "__main__":
    main()