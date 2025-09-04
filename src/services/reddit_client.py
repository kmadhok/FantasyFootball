#!/usr/bin/env python3
"""
Reddit Fantasy Football Client (Task 4.1)
Real Reddit API client for r/fantasyfootball streaming
No fake data - only real Reddit posts and comments
"""
import os
import sys
from pathlib import Path
from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime
import time

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "manual_test"))

# Import Reddit API
try:
    import praw
    from praw.models import MoreComments
    PRAW_AVAILABLE = True
except ImportError:
    print("❌ Error: praw not installed. Run: pip install praw")
    sys.exit(1)

# Import our existing roster function
from get_kanum_roster import load_environment, SleeperManualTestClient

@dataclass
class RedditPost:
    """Data structure for Reddit post information"""
    post_id: str
    title: str
    content: str
    author: str
    url: str
    timestamp: datetime
    subreddit: str
    score: int
    num_comments: int
    players_mentioned: List[str]
    is_kanum_player: bool
    post_type: str  # "post" or "comment"

class RedditFantasyClient:
    """Reddit client for Fantasy Football news from r/fantasyfootball"""
    
    def __init__(self):
        self.reddit = None
        self.kanum_players = []
        self.all_nfl_players = {}
        self.rate_limit_delay = 0.6  # 100 requests per minute = 0.6 seconds between requests
        self.last_request_time = 0
        
        # Initialize Reddit API connection
        if not self._setup_reddit_api():
            print("❌ Failed to setup Reddit API. Stopping.")
            sys.exit(1)
        
        # Load player data
        self._load_player_data()
    
    def _setup_reddit_api(self) -> bool:
        """Setup Reddit API connection with credentials from .env"""
        print("🔧 Setting up Reddit API connection...")
        
        # Load environment variables
        env_path = project_root / ".env"
        if env_path.exists():
            from dotenv import load_dotenv
            load_dotenv(env_path)
        
        # Get Reddit API credentials
        client_id = os.getenv("REDDIT_CLIENT_ID")
        client_secret = os.getenv("REDDIT_CLIENT_SECRET")
        user_agent = os.getenv("REDDIT_USER_AGENT", "FantasyFootballBot/1.0")
        
        # Check if credentials are available
        if not client_id or not client_secret:
            print("❌ Reddit API credentials missing from .env file")
            print("Required variables:")
            print("  REDDIT_CLIENT_ID=your_client_id")
            print("  REDDIT_CLIENT_SECRET=your_client_secret")
            print("  REDDIT_USER_AGENT=FantasyFootballBot/1.0")
            print("")
            print("Get credentials from: https://www.reddit.com/prefs/apps/")
            return False
        
        if client_id == "your_reddit_client_id_here" or client_secret == "your_reddit_client_secret_here":
            print("❌ Reddit API credentials not configured (still contain placeholder values)")
            print("Please update .env file with real Reddit API credentials")
            return False
        
        # Initialize Reddit API
        try:
            self.reddit = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                user_agent=user_agent
            )
            
            # Test connection
            self.reddit.subreddit("fantasyfootball").display_name
            print("✅ Reddit API connection successful")
            return True
            
        except Exception as e:
            print(f"❌ Failed to connect to Reddit API: {e}")
            print("Check your Reddit API credentials in .env file")
            return False
    
    def _rate_limit(self):
        """Implement rate limiting to stay under 100 req/min"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _load_player_data(self):
        """Load Kanum's roster players and NFL player database"""
        print("📱 Loading player data...")
        
        try:
            # Load environment
            sleeper_league_id = load_environment()
            client = SleeperManualTestClient(sleeper_league_id)
            
            # Get Kanum's roster
            from get_kanum_roster import find_kanum_team, get_kanum_roster, get_player_details
            
            users_response = client.get_users()
            if users_response.success:
                kanum_user_id, kanum_team = find_kanum_team(users_response.data)
                if kanum_user_id:
                    kanum_roster = get_kanum_roster(client, kanum_user_id)
                    if kanum_roster:
                        all_player_ids = kanum_roster.starters + kanum_roster.bench
                        player_details = get_player_details(client, all_player_ids)
                        
                        # Extract player names for Kanum's roster
                        self.kanum_players = [
                            player_details[pid]['name'] for pid in all_player_ids 
                            if pid in player_details and player_details[pid]['name']
                        ]
                        print(f"✅ Loaded {len(self.kanum_players)} Kanum players")
            
            # Get NFL player database (limited for efficiency)
            players_response = client._make_request("/players/nfl")
            if players_response.success:
                all_players = players_response.data
                self.all_nfl_players = {}
                
                # Only include players with reasonable names
                for player_id, player_data in list(all_players.items())[:1000]:  # Limit for efficiency
                    first_name = player_data.get('first_name', '')
                    last_name = player_data.get('last_name', '')
                    full_name = f"{first_name} {last_name}".strip()
                    if len(full_name) > 3 and ' ' in full_name:  # Valid full name
                        self.all_nfl_players[full_name] = {
                            'position': player_data.get('position'),
                            'team': player_data.get('team'),
                            'player_id': player_id
                        }
                
                print(f"✅ Loaded {len(self.all_nfl_players)} NFL players for monitoring")
                
        except Exception as e:
            print(f"❌ Error loading player data: {e}")
            print("Stopping - cannot proceed without player data")
            sys.exit(1)
    
    def search_kanum_players(self, limit: int = 20) -> List[RedditPost]:
        """
        Search r/fantasyfootball for posts mentioning Kanum's roster players
        """
        print(f"🔍 Searching r/fantasyfootball for Kanum's players...")
        
        all_posts = []
        
        try:
            # Search for each Kanum player
            for player in self.kanum_players:
                print(f"   Searching for: {player}")
                self._rate_limit()
                
                try:
                    # Search posts mentioning this player
                    subreddit = self.reddit.subreddit("fantasyfootball")
                    search_results = subreddit.search(
                        query=f'"{player}"',
                        sort="new",
                        time_filter="week",
                        limit=5  # Limit per player to avoid spam
                    )
                    
                    for post in search_results:
                        reddit_post = self._process_reddit_post(post, player)
                        if reddit_post:
                            all_posts.append(reddit_post)
                
                except Exception as e:
                    print(f"⚠️  Error searching for {player}: {e}")
                    continue
            
            # Remove duplicates by post ID
            unique_posts = []
            seen_ids = set()
            for post in all_posts:
                if post.post_id not in seen_ids:
                    unique_posts.append(post)
                    seen_ids.add(post.post_id)
            
            print(f"✅ Found {len(unique_posts)} unique posts mentioning Kanum players")
            return unique_posts[:limit]
            
        except Exception as e:
            print(f"❌ Error during Kanum player search: {e}")
            print("Stopping search due to error")
            return []
    
    def search_all_nfl_players(self, limit: int = 10) -> List[RedditPost]:
        """
        Search r/fantasyfootball for general NFL player discussions
        """
        print(f"🔍 Searching r/fantasyfootball for general NFL player news...")
        
        try:
            self._rate_limit()
            
            # Get recent hot posts from r/fantasyfootball
            subreddit = self.reddit.subreddit("fantasyfootball")
            hot_posts = subreddit.hot(limit=limit)
            
            all_posts = []
            for post in hot_posts:
                reddit_post = self._process_reddit_post(post)
                if reddit_post and reddit_post.players_mentioned:
                    all_posts.append(reddit_post)
            
            print(f"✅ Found {len(all_posts)} posts mentioning NFL players")
            return all_posts
            
        except Exception as e:
            print(f"❌ Error during general NFL search: {e}")
            print("Stopping search due to error")
            return []
    
    def _process_reddit_post(self, post, target_player: str = None) -> Optional[RedditPost]:
        """Process a Reddit post and check for player mentions"""
        try:
            # Get post content
            title = post.title
            content = post.selftext if hasattr(post, 'selftext') else ""
            full_text = f"{title} {content}"
            
            # Find player mentions
            players_mentioned = self._find_player_mentions(full_text, target_player)
            
            if not players_mentioned and not target_player:
                # Skip posts with no player mentions for general search
                return None
            
            # Check if any mentioned players are Kanum's
            is_kanum_player = any(
                player in self.kanum_players for player in players_mentioned
            )
            
            return RedditPost(
                post_id=str(post.id),
                title=title,
                content=content[:200] + "..." if len(content) > 200 else content,
                author=str(post.author) if post.author else "[deleted]",
                url=f"https://reddit.com{post.permalink}",
                timestamp=datetime.fromtimestamp(post.created_utc),
                subreddit=str(post.subreddit),
                score=post.score,
                num_comments=post.num_comments,
                players_mentioned=players_mentioned,
                is_kanum_player=is_kanum_player,
                post_type="post"
            )
            
        except Exception as e:
            print(f"⚠️  Warning: Error processing post: {e}")
            return None
    
    def _find_player_mentions(self, text: str, target_player: str = None) -> List[str]:
        """Find NFL player mentions in text"""
        mentioned_players = []
        text_lower = text.lower()
        
        # If we have a target player, prioritize it
        if target_player and self._is_player_mentioned(text, target_player):
            mentioned_players.append(target_player)
        
        # Check Kanum players first
        for player in self.kanum_players:
            if player != target_player and self._is_player_mentioned(text, player):
                mentioned_players.append(player)
                if len(mentioned_players) >= 5:  # Limit to avoid spam
                    break
        
        # Check other NFL players if we haven't found many
        if len(mentioned_players) < 3:
            for player_name in list(self.all_nfl_players.keys())[:100]:  # Limit for performance
                if player_name not in mentioned_players:
                    if self._is_player_mentioned(text, player_name):
                        mentioned_players.append(player_name)
                        if len(mentioned_players) >= 5:
                            break
        
        return mentioned_players
    
    def _is_player_mentioned(self, text: str, player_name: str) -> bool:
        """Check if player name is mentioned in text"""
        text_lower = text.lower()
        player_lower = player_name.lower()
        
        # Check full name
        if player_lower in text_lower:
            return True
        
        # Check last name for well-known players
        if len(player_name.split()) >= 2:
            last_name = player_name.split()[-1].lower()
            # Only check last names that are distinctive (longer than 4 chars)
            if len(last_name) > 4 and last_name in text_lower:
                return True
        
        return False
    
    def display_posts(self, posts: List[RedditPost]):
        """Display Reddit posts in a readable format"""
        if not posts:
            print("📭 No posts found matching criteria")
            return
        
        print(f"\n🟢 REDDIT FANTASY FOOTBALL NEWS ({len(posts)} posts)")
        print("=" * 80)
        
        for i, post in enumerate(posts, 1):
            # Display post header
            kanum_flag = "🔥 KANUM PLAYER" if post.is_kanum_player else ""
            print(f"\n{i}. r/{post.subreddit} - {post.timestamp.strftime('%Y-%m-%d %H:%M')} {kanum_flag}")
            print(f"   👤 u/{post.author} | ⬆️  {post.score} | 💬 {post.num_comments}")
            
            # Display title
            print(f"   📰 {post.title}")
            
            # Display content preview if available
            if post.content:
                print(f"   📄 {post.content}")
            
            # Display mentioned players
            if post.players_mentioned:
                players_str = ", ".join(post.players_mentioned[:5])
                if len(post.players_mentioned) > 5:
                    players_str += f" (+{len(post.players_mentioned) - 5} more)"
                print(f"   🏈 Players: {players_str}")
            
            # Display URL
            print(f"   🔗 {post.url}")
        
        print("=" * 80)

def test_reddit_client():
    """Test the Reddit client with real data"""
    print("🧪 TESTING REDDIT FANTASY CLIENT")
    print("=" * 60)
    
    client = RedditFantasyClient()
    
    # Test 1: Search for Kanum players
    print("\n1. Testing Kanum player search...")
    kanum_posts = client.search_kanum_players(limit=10)
    
    if kanum_posts:
        print(f"✅ Found {len(kanum_posts)} posts mentioning Kanum players")
        client.display_posts(kanum_posts)
    else:
        print("📭 No posts found mentioning Kanum players this week")
    
    # Test 2: Search for general NFL players
    print("\n2. Testing general NFL player search...")
    nfl_posts = client.search_all_nfl_players(limit=5)
    
    if nfl_posts:
        print(f"✅ Found {len(nfl_posts)} posts mentioning NFL players")
        client.display_posts(nfl_posts)
    else:
        print("📭 No general NFL player posts found")
    
    return client

def main():
    """Main function"""
    print("🟢 REDDIT FANTASY FOOTBALL CLIENT")
    print("=" * 60)
    print("Task 4.1: Reddit API client for r/fantasyfootball streaming")
    print("Using REAL Reddit data only - no fake content")
    
    try:
        client = test_reddit_client()
        print("\n✅ Reddit client test completed successfully")
        
    except KeyboardInterrupt:
        print("\n⚠️  Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        print("Stopping due to error - no fake data fallback")

if __name__ == "__main__":
    main()