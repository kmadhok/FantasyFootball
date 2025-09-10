#!/usr/bin/env python3
"""
Analytics and Data Quality Reporting for Canonical Model Data

Provides comprehensive analysis of the canonical fantasy football data,
including data quality metrics, player distribution analysis, and projection insights.
"""

import argparse
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Optional
# Optional visualization imports
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
from datetime import datetime
from dataclasses import dataclass

# Import existing infrastructure
from src.database import SessionLocal, Player, PlayerProjections
from src.config.config import get_config


@dataclass
class DataQualityMetrics:
    """Container for data quality metrics."""
    total_players: int
    total_projections: int
    match_rate: float
    position_distribution: Dict[str, int]
    projection_completeness: Dict[str, float]
    outliers: List[str]
    data_freshness: datetime


class CanonicalDataAnalyzer:
    def __init__(self, canonical_dir: Path):
        self.canonical_dir = canonical_dir
        self.session = SessionLocal()
        
        # Load canonical data
        self.dim_player = self._load_parquet('dim_player.parquet')
        self.fact_projections = self._load_parquet('fact_projections.parquet')
        self.fact_weekly_stats = self._load_parquet('fact_weekly_stats.parquet')
        self.data_quality_report = self._load_parquet('data_quality_report.parquet')
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()
        
    def _load_parquet(self, filename: str) -> Optional[pd.DataFrame]:
        """Load parquet file if it exists."""
        path = self.canonical_dir / filename
        if path.exists():
            return pd.read_parquet(path)
        else:
            print(f"⚠️ File not found: {filename}")
            return None
    
    def generate_comprehensive_report(self) -> Dict:
        """Generate comprehensive analytics report."""
        print("📊 Generating comprehensive analytics report...")
        
        report = {
            'data_quality': self.analyze_data_quality(),
            'player_analysis': self.analyze_player_distribution(),
            'projection_analysis': self.analyze_projections(),
            'database_integration': self.analyze_database_integration(),
            'statistical_insights': self.generate_statistical_insights(),
            'recommendations': self.generate_recommendations()
        }
        
        return report
    
    def analyze_data_quality(self) -> Dict:
        """Analyze data quality metrics."""
        print("🔍 Analyzing data quality...")
        
        if self.data_quality_report is None:
            return {"error": "Data quality report not available"}
            
        metrics = {}
        
        # Convert data quality report to metrics
        if not self.data_quality_report.empty:
            dq_dict = self.data_quality_report.set_index('metric')['value'].to_dict()
            
            metrics.update({
                'match_rate': dq_dict.get('match_rate', 0),
                'total_csv_players': dq_dict.get('total_csv_players', 0),
                'matched_players': dq_dict.get('matched_players', 0),
                'position_match_rates': {
                    'QB': dq_dict.get('match_rate_QB', 0),
                    'RB': dq_dict.get('match_rate_RB', 0), 
                    'WR': dq_dict.get('match_rate_WR', 0),
                    'TE': dq_dict.get('match_rate_TE', 0),
                    'K': dq_dict.get('match_rate_K', 0),
                    'DST': dq_dict.get('match_rate_DST', 0)
                }
            })
            
        # Data completeness analysis
        if self.fact_projections is not None:
            completeness = {}
            for col in ['proj_points', 'proj_yards', 'proj_td', 'avg_adp']:
                if col in self.fact_projections.columns:
                    completeness[col] = (1 - self.fact_projections[col].isna().mean()) * 100
            metrics['projection_completeness'] = completeness
            
        return metrics
    
    def analyze_player_distribution(self) -> Dict:
        """Analyze player distribution across positions and teams."""
        print("👤 Analyzing player distribution...")
        
        analysis = {}
        
        if self.dim_player is not None:
            # Position distribution
            pos_dist = self.dim_player['position'].value_counts().to_dict()
            analysis['position_distribution'] = pos_dist
            
            # Team distribution  
            team_dist = self.dim_player['team'].value_counts().head(10).to_dict()
            analysis['top_teams'] = team_dist
            
            # Platform coverage
            platform_coverage = {}
            for platform in ['sleeper_id', 'mfl_id', 'fantasypros_id']:
                if platform in self.dim_player.columns:
                    coverage = (1 - self.dim_player[platform].isna().mean()) * 100
                    platform_coverage[platform] = coverage
            analysis['platform_coverage'] = platform_coverage
            
        return analysis
    
    def analyze_projections(self) -> Dict:
        """Analyze projection data patterns."""
        print("📈 Analyzing projection patterns...")
        
        analysis = {}
        
        if self.fact_projections is None:
            return {"error": "Projections data not available"}
            
        # Basic projection statistics
        proj_stats = {}
        if 'proj_points' in self.fact_projections.columns:
            points = self.fact_projections['proj_points'].dropna()
            proj_stats['points'] = {
                'mean': points.mean(),
                'median': points.median(),
                'std': points.std(),
                'min': points.min(),
                'max': points.max(),
                'top_10_threshold': points.quantile(0.9)
            }
            
        # Position-based analysis
        if self.dim_player is not None:
            proj_with_pos = self.fact_projections.merge(
                self.dim_player[['player_sk', 'position']], 
                on='player_sk', 
                how='left'
            )
            
            position_stats = {}
            for pos in ['QB', 'RB', 'WR', 'TE']:
                pos_data = proj_with_pos[proj_with_pos['position'] == pos]['proj_points'].dropna()
                if len(pos_data) > 0:
                    position_stats[pos] = {
                        'count': len(pos_data),
                        'mean': pos_data.mean(),
                        'top_player': pos_data.max(),
                        'top_10_avg': pos_data.nlargest(10).mean()
                    }
            analysis['position_stats'] = position_stats
            
        # ADP vs Projections correlation
        if all(col in self.fact_projections.columns for col in ['avg_adp', 'proj_points']):
            adp_proj = self.fact_projections[['avg_adp', 'proj_points']].dropna()
            if len(adp_proj) > 10:
                correlation = adp_proj.corr().iloc[0,1]
                analysis['adp_projection_correlation'] = correlation
                
        analysis['basic_stats'] = proj_stats
        return analysis
    
    def analyze_database_integration(self) -> Dict:
        """Analyze how well the canonical data integrated with the database."""
        print("🗄️ Analyzing database integration...")
        
        analysis = {}
        
        # Count players and projections in database
        db_player_count = self.session.query(Player).count()
        db_projection_count = self.session.query(PlayerProjections).count()
        
        analysis['database_counts'] = {
            'players': db_player_count,
            'projections': db_projection_count
        }
        
        # Analyze platform ID coverage in database
        platform_counts = {}
        for platform in ['sleeper_id', 'mfl_id']:
            count = self.session.query(Player).filter(
                getattr(Player, platform).isnot(None)
            ).count()
            platform_counts[platform] = count
            
        analysis['platform_id_coverage'] = platform_counts
        
        # Recent projection data
        recent_projections = self.session.query(PlayerProjections).filter(
            PlayerProjections.season == 2024,
            PlayerProjections.week == 1,
            PlayerProjections.source == 'csv_rankings'
        ).count()
        
        analysis['recent_csv_projections'] = recent_projections
        
        return analysis
    
    def generate_statistical_insights(self) -> Dict:
        """Generate statistical insights and correlations."""
        print("📊 Generating statistical insights...")
        
        insights = {}
        
        if self.fact_projections is None:
            return {"error": "Projections data not available"}
            
        # Value analysis (points vs ADP)
        if all(col in self.fact_projections.columns for col in ['proj_points', 'avg_adp', 'rank']):
            # Calculate value scores (projected points relative to draft position)
            proj_clean = self.fact_projections[
                (self.fact_projections['proj_points'] > 0) & 
                (self.fact_projections['avg_adp'] > 0)
            ].copy()
            
            if len(proj_clean) > 0:
                # Simple value calculation: points per draft round equivalent
                proj_clean['draft_round_equiv'] = np.ceil(proj_clean['avg_adp'] / 12)
                proj_clean['value_score'] = proj_clean['proj_points'] / proj_clean['draft_round_equiv']
                
                # Top value players
                top_values = proj_clean.nlargest(10, 'value_score')
                if self.dim_player is not None:
                    top_values_with_names = top_values.merge(
                        self.dim_player[['player_sk', 'player_name', 'position']], 
                        on='player_sk'
                    )
                    insights['top_value_players'] = top_values_with_names[
                        ['player_name', 'position', 'proj_points', 'avg_adp', 'value_score']
                    ].to_dict('records')
                    
        # Sleeper picks analysis
        if 'avg_adp' in self.fact_projections.columns:
            late_round_gems = self.fact_projections[
                (self.fact_projections['avg_adp'] > 100) & 
                (self.fact_projections['proj_points'] > 10)
            ]
            insights['late_round_gems_count'] = len(late_round_gems)
            
        return insights
    
    def generate_recommendations(self) -> List[str]:
        """Generate actionable recommendations based on analysis."""
        print("💡 Generating recommendations...")
        
        recommendations = []
        
        # Data quality recommendations
        if self.data_quality_report is not None:
            dq_dict = self.data_quality_report.set_index('metric')['value'].to_dict()
            match_rate = dq_dict.get('match_rate', 0)
            
            if match_rate < 0.95:
                recommendations.append(
                    f"Match rate is {match_rate:.1%} - consider improving player name normalization"
                )
                
            # DST matching issues
            dst_match_rate = dq_dict.get('match_rate_DST', 0)
            if dst_match_rate < 0.5:
                recommendations.append(
                    "DST matching is poor - implement team-based matching for defenses"
                )
                
        # Platform coverage recommendations
        if self.dim_player is not None:
            for platform in ['sleeper_id', 'mfl_id']:
                if platform in self.dim_player.columns:
                    coverage = (1 - self.dim_player[platform].isna().mean()) * 100
                    if coverage < 70:
                        recommendations.append(
                            f"{platform} coverage is low ({coverage:.1f}%) - consider updating ID mappings"
                        )
                        
        # Projection completeness recommendations
        if self.fact_projections is not None:
            for col in ['proj_yards', 'proj_td', 'proj_rec']:
                if col in self.fact_projections.columns:
                    completeness = (1 - self.fact_projections[col].isna().mean()) * 100
                    if completeness < 80:
                        recommendations.append(
                            f"{col} is only {completeness:.1f}% complete - consider data enrichment"
                        )
                        
        # Performance recommendations
        recommendations.append("Consider setting up automated weekly data refresh")
        recommendations.append("Implement alerts for significant ADP vs projection discrepancies")
        
        if len(recommendations) == 0:
            recommendations.append("Data quality looks excellent - system is performing well!")
            
        return recommendations
    
    def create_visualizations(self, output_dir: Path = None):
        """Create visualization plots for the analysis."""
        if not HAS_MATPLOTLIB:
            print("⚠️ matplotlib not available - skipping visualizations")
            return
            
        if output_dir is None:
            output_dir = Path("analytics_output")
        output_dir.mkdir(exist_ok=True)
        
        print(f"📈 Creating visualizations in {output_dir}...")
        
        try:
            plt.style.use('seaborn-v0_8')  # Updated style name
        except:
            pass  # Use default style if seaborn not available
        
        # Position distribution
        if self.dim_player is not None and 'position' in self.dim_player.columns:
            plt.figure(figsize=(10, 6))
            pos_counts = self.dim_player['position'].value_counts()
            pos_counts.plot(kind='bar')
            plt.title('Player Distribution by Position')
            plt.xlabel('Position')
            plt.ylabel('Number of Players')
            plt.tight_layout()
            plt.savefig(output_dir / 'position_distribution.png')
            plt.close()
            
        # Projection distribution
        if self.fact_projections is not None and 'proj_points' in self.fact_projections.columns:
            plt.figure(figsize=(12, 8))
            
            # Overall distribution
            plt.subplot(2, 2, 1)
            self.fact_projections['proj_points'].hist(bins=50, alpha=0.7)
            plt.title('Distribution of Projected Points')
            plt.xlabel('Projected Points')
            plt.ylabel('Frequency')
            
            # By position if possible
            if self.dim_player is not None:
                proj_with_pos = self.fact_projections.merge(
                    self.dim_player[['player_sk', 'position']], 
                    on='player_sk', 
                    how='left'
                )
                
                plt.subplot(2, 2, 2)
                for pos in ['QB', 'RB', 'WR', 'TE']:
                    pos_data = proj_with_pos[proj_with_pos['position'] == pos]['proj_points'].dropna()
                    if len(pos_data) > 0:
                        plt.hist(pos_data, alpha=0.6, label=pos, bins=20)
                plt.title('Projected Points by Position')
                plt.xlabel('Projected Points')
                plt.ylabel('Frequency')
                plt.legend()
                
            # ADP vs Projections if available
            if 'avg_adp' in self.fact_projections.columns:
                plt.subplot(2, 2, 3)
                adp_proj = self.fact_projections[['avg_adp', 'proj_points']].dropna()
                if len(adp_proj) > 0:
                    plt.scatter(adp_proj['avg_adp'], adp_proj['proj_points'], alpha=0.6)
                    plt.title('ADP vs Projected Points')
                    plt.xlabel('Average Draft Position')
                    plt.ylabel('Projected Points')
                    
            plt.tight_layout()
            plt.savefig(output_dir / 'projection_analysis.png')
            plt.close()
            
        print(f"✅ Visualizations saved to {output_dir}")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze canonical fantasy football data'
    )
    parser.add_argument('--canonical-dir', type=Path, default='out',
                       help='Directory containing canonical model outputs')
    parser.add_argument('--output-dir', type=Path, default='analytics_output',
                       help='Directory for analytics outputs')
    parser.add_argument('--create-visualizations', action='store_true',
                       help='Create visualization plots')
    parser.add_argument('--save-report', action='store_true',
                       help='Save detailed report as JSON')
    
    args = parser.parse_args()
    
    if not args.canonical_dir.exists():
        print(f"❌ Canonical directory not found: {args.canonical_dir}")
        return 1
        
    try:
        with CanonicalDataAnalyzer(args.canonical_dir) as analyzer:
            # Generate comprehensive report
            report = analyzer.generate_comprehensive_report()
            
            # Print summary
            print("\n" + "="*60)
            print("CANONICAL DATA ANALYTICS SUMMARY")
            print("="*60)
            
            # Data Quality Summary
            if 'data_quality' in report:
                dq = report['data_quality']
                print(f"📊 Data Quality:")
                print(f"  Match Rate: {dq.get('match_rate', 0):.2%}")
                print(f"  Total Players: {dq.get('total_csv_players', 0)}")
                print(f"  Matched Players: {dq.get('matched_players', 0)}")
                
            # Player Distribution
            if 'player_analysis' in report:
                pa = report['player_analysis']
                if 'position_distribution' in pa:
                    print(f"\n👤 Position Distribution:")
                    for pos, count in pa['position_distribution'].items():
                        print(f"  {pos}: {count} players")
                        
            # Projection Insights  
            if 'projection_analysis' in report:
                proj = report['projection_analysis']
                if 'basic_stats' in proj and 'points' in proj['basic_stats']:
                    points_stats = proj['basic_stats']['points']
                    print(f"\n📈 Projection Stats:")
                    print(f"  Average Points: {points_stats['mean']:.1f}")
                    print(f"  Top Player: {points_stats['max']:.1f} points")
                    print(f"  Top 10% Threshold: {points_stats['top_10_threshold']:.1f} points")
                    
            # Database Integration
            if 'database_integration' in report:
                db = report['database_integration']
                print(f"\n🗄️ Database Integration:")
                print(f"  Players in DB: {db.get('database_counts', {}).get('players', 0)}")
                print(f"  Projections in DB: {db.get('database_counts', {}).get('projections', 0)}")
                print(f"  CSV Projections Loaded: {db.get('recent_csv_projections', 0)}")
                
            # Recommendations
            if 'recommendations' in report:
                print(f"\n💡 Recommendations:")
                for i, rec in enumerate(report['recommendations'][:5], 1):
                    print(f"  {i}. {rec}")
                    
            # Create visualizations if requested
            if args.create_visualizations:
                analyzer.create_visualizations(args.output_dir)
                
            # Save detailed report if requested
            if args.save_report:
                import json
                args.output_dir.mkdir(exist_ok=True)
                report_file = args.output_dir / 'canonical_data_report.json'
                
                # Convert numpy types for JSON serialization
                def convert_numpy(obj):
                    if isinstance(obj, np.integer):
                        return int(obj)
                    elif isinstance(obj, np.floating):
                        return float(obj)
                    elif isinstance(obj, np.ndarray):
                        return obj.tolist()
                    return obj
                
                # Clean report for JSON
                clean_report = {}
                for key, value in report.items():
                    if isinstance(value, dict):
                        clean_report[key] = {k: convert_numpy(v) for k, v in value.items() if v is not None}
                    else:
                        clean_report[key] = convert_numpy(value)
                        
                with open(report_file, 'w') as f:
                    json.dump(clean_report, f, indent=2, default=str)
                    
                print(f"📄 Detailed report saved to {report_file}")
                
        return 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())