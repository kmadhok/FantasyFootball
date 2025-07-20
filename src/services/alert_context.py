import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from src.database import SessionLocal, Player, RosterEntry, WaiverState, Alert, NewsItem
from src.services.waiver_tracker import WaiverTrackerService
from src.database.roster_storage import RosterStorageService

logger = logging.getLogger(__name__)

class AlertContextService:
    """Service to enhance alerts with waiver and roster context"""
    
    def __init__(self):
        self.waiver_tracker = WaiverTrackerService()
        self.roster_storage = RosterStorageService()
    
    def add_waiver_context_to_alert(self, alert_id: int) -> bool:
        """Add comprehensive waiver context to an existing alert"""
        try:
            db = SessionLocal()
            try:
                # Get the alert and related data
                alert = db.query(Alert).filter(Alert.id == alert_id).first()
                if not alert:
                    logger.error(f"Alert {alert_id} not found")
                    return False
                
                player = db.query(Player).filter(Player.id == alert.player_id).first()
                news_item = db.query(NewsItem).filter(NewsItem.id == alert.news_item_id).first()
                
                if not player or not news_item:
                    logger.error(f"Missing player or news item for alert {alert_id}")
                    return False
                
                # Generate waiver context
                waiver_context = self._generate_waiver_context(player, news_item, db)
                roster_context = self._generate_roster_context(player, db)
                
                # Update alert with context
                alert.waiver_info = json.dumps(waiver_context)
                alert.roster_context = json.dumps(roster_context)
                alert.waiver_recommendation = waiver_context.get('recommendation')
                alert.faab_suggestion = waiver_context.get('faab_suggestion')
                alert.waiver_urgency = waiver_context.get('urgency')
                
                db.commit()
                logger.info(f"Added waiver context to alert {alert_id}")
                return True
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to add waiver context to alert {alert_id}: {e}")
            return False
    
    def _generate_waiver_context(self, player: Player, news_item: NewsItem, db) -> Dict[str, Any]:
        """Generate comprehensive waiver context for a player"""
        context = {
            'timestamp': datetime.utcnow().isoformat(),
            'player_info': {
                'name': player.name,
                'position': player.position,
                'team': player.team,
                'is_starter': player.is_starter
            },
            'news_context': {
                'event_type': news_item.event_type,
                'confidence_score': news_item.confidence_score,
                'source': news_item.source
            }
        }
        
        # Get waiver states for both platforms
        sleeper_waivers = db.query(WaiverState).filter(
            WaiverState.platform == 'sleeper'
        ).all()
        
        mfl_waivers = db.query(WaiverState).filter(
            WaiverState.platform == 'mfl'
        ).all()
        
        # Process Sleeper waiver information
        if sleeper_waivers:
            sleeper_context = self._process_sleeper_waiver_context(sleeper_waivers)
            context['sleeper'] = sleeper_context
        
        # Process MFL waiver information
        if mfl_waivers:
            mfl_context = self._process_mfl_waiver_context(mfl_waivers)
            context['mfl'] = mfl_context
        
        # Generate recommendations
        context['recommendation'] = self._generate_waiver_recommendation(player, news_item, context)
        context['urgency'] = self._determine_waiver_urgency(news_item, context)
        context['faab_suggestion'] = self._suggest_faab_bid(player, news_item, context)
        
        return context
    
    def _process_sleeper_waiver_context(self, waiver_states: List[WaiverState]) -> Dict[str, Any]:
        """Process Sleeper waiver order information"""
        waiver_orders = []
        
        for state in waiver_states:
            waiver_orders.append({
                'user_id': state.user_id,
                'waiver_order': state.waiver_order,
                'total_claims': state.total_claims,
                'successful_claims': state.successful_claims,
                'last_updated': state.timestamp.isoformat()
            })
        
        # Sort by waiver order (lower is better)
        waiver_orders.sort(key=lambda x: x['waiver_order'] or 999)
        
        return {
            'waiver_type': 'priority_order',
            'total_teams': len(waiver_orders),
            'waiver_orders': waiver_orders,
            'next_reset': None  # Would need league settings to determine
        }
    
    def _process_mfl_waiver_context(self, waiver_states: List[WaiverState]) -> Dict[str, Any]:
        """Process MFL FAAB information"""
        faab_balances = []
        total_faab = 0
        
        for state in waiver_states:
            faab_balance = state.faab_balance or 0
            total_faab += faab_balance
            
            faab_balances.append({
                'user_id': state.user_id,
                'faab_balance': faab_balance,
                'initial_faab': state.initial_faab,
                'faab_used': state.waiver_budget_used or 0,
                'total_claims': state.total_claims,
                'last_updated': state.timestamp.isoformat()
            })
        
        # Sort by FAAB balance (higher is more competitive)
        faab_balances.sort(key=lambda x: x['faab_balance'], reverse=True)
        
        average_faab = total_faab / len(faab_balances) if faab_balances else 0
        
        return {
            'waiver_type': 'faab',
            'total_teams': len(faab_balances),
            'total_faab_remaining': total_faab,
            'average_faab_remaining': round(average_faab, 2),
            'faab_balances': faab_balances
        }
    
    def _generate_roster_context(self, player: Player, db) -> Dict[str, Any]:
        """Generate roster context showing who has the player"""
        roster_context = {
            'is_rostered': False,
            'rostered_by': [],
            'available_on': []
        }
        
        # Check roster entries for this player
        roster_entries = db.query(RosterEntry).filter(
            RosterEntry.player_id == player.id,
            RosterEntry.is_active == True
        ).all()
        
        platforms = set()
        
        for entry in roster_entries:
            roster_context['is_rostered'] = True
            roster_context['rostered_by'].append({
                'platform': entry.platform,
                'league_id': entry.league_id,
                'user_id': entry.user_id,
                'roster_slot': entry.roster_slot
            })
            platforms.add(entry.platform)
        
        # Determine availability
        all_platforms = {'sleeper', 'mfl'}
        available_platforms = all_platforms - platforms
        roster_context['available_on'] = list(available_platforms)
        
        return roster_context
    
    def _generate_waiver_recommendation(self, player: Player, news_item: NewsItem, context: Dict[str, Any]) -> str:
        """Generate waiver recommendation based on player and news context"""
        # Base recommendation on player position and starter status
        if player.is_starter:
            base_priority = 'high_priority'
        elif player.position in ['QB', 'RB', 'WR', 'TE']:
            base_priority = 'medium_priority'
        else:
            base_priority = 'low_priority'
        
        # Adjust based on news confidence and event type
        confidence_score = news_item.confidence_score or 0
        event_type = news_item.event_type or ''
        
        # High-impact events get priority boost
        if event_type in ['injury', 'trade', 'promotion'] and confidence_score > 0.7:
            if base_priority == 'medium_priority':
                return 'high_priority'
            elif base_priority == 'low_priority':
                return 'medium_priority'
        
        # Low confidence news gets priority reduction
        if confidence_score < 0.3:
            if base_priority == 'high_priority':
                return 'medium_priority'
            elif base_priority == 'medium_priority':
                return 'low_priority'
        
        return base_priority
    
    def _determine_waiver_urgency(self, news_item: NewsItem, context: Dict[str, Any]) -> str:
        """Determine how urgently the waiver claim should be made"""
        event_type = news_item.event_type or ''
        confidence_score = news_item.confidence_score or 0
        
        # Immediate urgency for high-confidence injury/promotion news
        if event_type in ['injury', 'promotion'] and confidence_score > 0.8:
            return 'immediate'
        
        # Next cycle for medium confidence or general trade news
        if confidence_score > 0.5 or event_type in ['trade', 'start']:
            return 'next_cycle'
        
        # Monitor for everything else
        return 'monitor'
    
    def _suggest_faab_bid(self, player: Player, news_item: NewsItem, context: Dict[str, Any]) -> Optional[float]:
        """Suggest FAAB bid amount based on context"""
        # Only suggest FAAB for MFL leagues
        if 'mfl' not in context:
            return None
        
        mfl_context = context['mfl']
        average_faab = mfl_context.get('average_faab_remaining', 0)
        
        if average_faab == 0:
            return None
        
        # Base suggestion on player value and news impact
        base_percentage = 0.05  # 5% of average FAAB as baseline
        
        if player.is_starter:
            base_percentage = 0.15  # 15% for starters
        elif player.position in ['RB', 'WR']:
            base_percentage = 0.10  # 10% for skill positions
        
        # Adjust for news confidence
        confidence_score = news_item.confidence_score or 0
        confidence_multiplier = 1 + (confidence_score - 0.5)  # 0.5 to 1.5 range
        
        # Adjust for event type
        event_multipliers = {
            'injury': 1.5,  # Other player injured, this player benefits
            'promotion': 1.3,
            'trade': 1.2,
            'start': 1.1
        }
        
        event_type = news_item.event_type or ''
        event_multiplier = event_multipliers.get(event_type, 1.0)
        
        # Calculate final suggestion
        suggested_percentage = base_percentage * confidence_multiplier * event_multiplier
        suggested_amount = average_faab * suggested_percentage
        
        # Round to reasonable amounts
        if suggested_amount < 1:
            return 1.0
        elif suggested_amount > average_faab * 0.5:  # Cap at 50% of average
            return round(average_faab * 0.5)
        else:
            return round(suggested_amount)
    
    def get_waiver_context_for_player(self, player_id: int) -> Optional[Dict[str, Any]]:
        """Get current waiver context for a specific player"""
        try:
            db = SessionLocal()
            try:
                player = db.query(Player).filter(Player.id == player_id).first()
                if not player:
                    return None
                
                # Create a mock news item for context generation
                mock_news = NewsItem(
                    player_id=player_id,
                    headline=f"Context check for {player.name}",
                    headline_hash="context_check",
                    source="system",
                    confidence_score=1.0
                )
                
                waiver_context = self._generate_waiver_context(player, mock_news, db)
                roster_context = self._generate_roster_context(player, db)
                
                return {
                    'waiver_context': waiver_context,
                    'roster_context': roster_context
                }
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to get waiver context for player {player_id}: {e}")
            return None
    
    def bulk_update_alert_contexts(self, alert_ids: List[int]) -> Dict[str, int]:
        """Bulk update waiver contexts for multiple alerts"""
        results = {'success': 0, 'failed': 0}
        
        for alert_id in alert_ids:
            try:
                if self.add_waiver_context_to_alert(alert_id):
                    results['success'] += 1
                else:
                    results['failed'] += 1
            except Exception as e:
                logger.error(f"Failed to update context for alert {alert_id}: {e}")
                results['failed'] += 1
        
        logger.info(f"Bulk context update completed: {results['success']} success, {results['failed']} failed")
        return results

# Convenience functions
def enhance_alert_with_waiver_context(alert_id: int) -> bool:
    """Convenience function to enhance a single alert with waiver context"""
    service = AlertContextService()
    return service.add_waiver_context_to_alert(alert_id)

def get_player_waiver_summary(player_id: int) -> Optional[Dict[str, Any]]:
    """Get a summary of waiver context for a player"""
    service = AlertContextService()
    return service.get_waiver_context_for_player(player_id)

def update_multiple_alert_contexts(alert_ids: List[int]) -> Dict[str, int]:
    """Update waiver contexts for multiple alerts"""
    service = AlertContextService()
    return service.bulk_update_alert_contexts(alert_ids)

# Test function
def test_alert_context_service():
    """Test the alert context service functionality"""
    print("Testing Alert Context Service...")
    print("=" * 60)
    
    service = AlertContextService()
    
    try:
        # Test getting waiver context for a player (would need real player ID)
        print("\n1. Testing player waiver context...")
        # context = service.get_waiver_context_for_player(1)
        # print(f"   Context: {context}")
        print("   Skipped - requires real player data")
        
        # Test waiver statistics
        print("\n2. Testing waiver statistics...")
        stats = service.waiver_tracker.get_waiver_statistics()
        if 'error' not in stats:
            print(f"   Sleeper waiver count: {stats['waiver_data']['sleeper_count']}")
            print(f"   MFL FAAB count: {stats['waiver_data']['mfl_count']}")
        else:
            print(f"   Error: {stats['error']}")
        
        return True
        
    except Exception as e:
        print(f"   ✗ Alert context test failed: {e}")
        return False

if __name__ == "__main__":
    test_alert_context_service()