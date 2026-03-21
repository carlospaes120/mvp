#!/usr/bin/env python3
"""
windowed_metrics.py

Calculate time-windowed network metrics from JSONL case data.
Exports temporal series to CSV and optionally generates plots.
"""

import argparse
import glob
import json
import logging
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Iterator

import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Robust Louvain import
try:
    import community as community_louvain
except Exception:
    try:
        import community.community_louvain as community_louvain
    except Exception:
        community_louvain = None


def setup_logging(level: str) -> None:
    """Configure logging."""
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def load_events(file_patterns: List[str]) -> pd.DataFrame:
    """
    Load JSONL files, parse timestamps robustly, return DataFrame with 'ts' column.
    
    Args:
        file_patterns: List of file paths or glob patterns
        
    Returns:
        DataFrame with 'ts' (datetime), 'raw_event' (dict)
    """
    logging.info(f"Loading events from {len(file_patterns)} pattern(s)...")
    
    # Expand glob patterns
    all_files = []
    for pattern in file_patterns:
        expanded = glob.glob(pattern, recursive=True)
        if expanded:
            all_files.extend(expanded)
        else:
            # Try as literal path
            if Path(pattern).exists():
                all_files.append(pattern)
    
    if not all_files:
        logging.error(f"No files found matching patterns: {file_patterns}")
        sys.exit(1)
    
    logging.info(f"Found {len(all_files)} file(s) to process")
    
    # Load all JSONL
    events = []
    for fpath in all_files:
        logging.debug(f"Reading {fpath}")
        try:
            with open(fpath, 'r', encoding='utf-8') as f:
                for line_no, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                        events.append(event)
                    except json.JSONDecodeError as e:
                        logging.warning(f"JSON decode error in {fpath}:{line_no}: {e}")
        except Exception as e:
            logging.error(f"Error reading {fpath}: {e}")
    
    logging.info(f"Loaded {len(events)} raw events")
    
    if not events:
        logging.error("No valid events loaded")
        sys.exit(1)
    
    df = pd.DataFrame(events)
    df['raw_event'] = events
    
    # Parse timestamp robustly
    ts_fields = ['created_at', 'timestamp', 'ts', 'date', 'time']
    ts_col = None
    
    for field in ts_fields:
        if field in df.columns:
            logging.debug(f"Trying timestamp field: {field}")
            temp_ts = pd.to_datetime(df[field], utc=True, errors='coerce')
            valid_count = temp_ts.notna().sum()
            if valid_count > 0:
                df['ts'] = temp_ts
                ts_col = field
                logging.info(f"Parsed {valid_count}/{len(df)} timestamps from '{field}'")
                break
    
    if ts_col is None:
        logging.error("No valid timestamp field found")
        sys.exit(1)
    
    # Drop rows without valid timestamp
    before = len(df)
    df = df[df['ts'].notna()].copy()
    after = len(df)
    if before > after:
        logging.warning(f"Dropped {before - after} events with invalid timestamps")
    
    if df.empty:
        logging.error("No events with valid timestamps")
        sys.exit(1)
    
    # Sort by timestamp
    df = df.sort_values('ts').reset_index(drop=True)
    
    logging.info(f"Final dataset: {len(df)} events from {df['ts'].min()} to {df['ts'].max()}")
    
    return df


def extract_edges(df_slice: pd.DataFrame) -> pd.DataFrame:
    """
    Extract directed edges (source -> target) from event slice.
    
    Returns:
        DataFrame with columns: source, target, weight (aggregated)
    """
    edges = []
    
    mention_count = 0
    retweet_count = 0
    reply_count = 0
    
    for idx, row in df_slice.iterrows():
        event = row['raw_event']
        
        # Extract author
        author = None
        for field in ['user_username', 'username', 'user.screen_name', 'user']:
            if field in event:
                val = event[field]
                if isinstance(val, str):
                    author = val.lower().strip()
                    break
                elif isinstance(val, dict) and 'screen_name' in val:
                    author = val['screen_name'].lower().strip()
                    break
                elif isinstance(val, dict) and 'username' in val:
                    author = val['username'].lower().strip()
                    break
        
        # Nested field access for author
        if author is None:
            if 'author' in event and isinstance(event['author'], dict):
                if 'username' in event['author']:
                    author = event['author']['username'].lower().strip()
        
        if not author:
            continue
        
        targets = set()
        
        # Extract mentions
        mentioned = []
        for field in ['mentioned_usernames', 'mentions', 'entities.mentions', 'entities.user_mentions']:
            if '.' in field:
                parts = field.split('.')
                val = event
                for part in parts:
                    if isinstance(val, dict) and part in val:
                        val = val[part]
                    else:
                        val = None
                        break
            else:
                val = event.get(field)
            
            if val:
                if isinstance(val, list):
                    for item in val:
                        if isinstance(item, str):
                            mentioned.append(item)
                        elif isinstance(item, dict):
                            for uname_field in ['username', 'screen_name']:
                                if uname_field in item:
                                    mentioned.append(item[uname_field])
                                    break
                break
        
        # Fallback: regex on text
        if not mentioned:
            text = event.get('text') or event.get('full_text') or ''
            if text:
                mentioned = re.findall(r'@(\w+)', text)
        
        for m in mentioned:
            m_clean = m.lower().strip()
            if m_clean and m_clean != author:
                targets.add(m_clean)
                mention_count += 1
        
        # Extract retweet
        rt_user = None
        for field in ['retweeted_user_username', 'retweet_username']:
            if field in event:
                rt_user = event[field]
                break
        
        if not rt_user and 'retweeted_status' in event:
            rt_status = event['retweeted_status']
            if isinstance(rt_status, dict) and 'user' in rt_status:
                user_obj = rt_status['user']
                if isinstance(user_obj, dict):
                    rt_user = user_obj.get('screen_name') or user_obj.get('username')
        
        if rt_user:
            rt_clean = rt_user.lower().strip()
            if rt_clean and rt_clean != author:
                targets.add(rt_clean)
                retweet_count += 1
        
        # Extract reply
        reply_user = None
        for field in ['in_reply_to_username', 'in_reply_to_screen_name']:
            if field in event:
                reply_user = event[field]
                break
        
        if reply_user:
            reply_clean = reply_user.lower().strip()
            if reply_clean and reply_clean != author:
                targets.add(reply_clean)
                reply_count += 1
        
        # Add edges
        for target in targets:
            edges.append({'source': author, 'target': target, 'weight': 1})
    
    logging.debug(f"Extracted edges: {mention_count} mentions, {retweet_count} retweets, {reply_count} replies")
    
    if not edges:
        return pd.DataFrame(columns=['source', 'target', 'weight'])
    
    edges_df = pd.DataFrame(edges)
    # Aggregate weights
    edges_df = edges_df.groupby(['source', 'target'], as_index=False)['weight'].sum()
    
    return edges_df


def build_graph(edges_df: pd.DataFrame, min_degree: int) -> nx.DiGraph:
    """
    Build directed graph from edges, filter by min degree (in+out).
    
    Args:
        edges_df: DataFrame with source, target, weight
        min_degree: Minimum total degree (in+out) to keep nodes
        
    Returns:
        Filtered DiGraph
    """
    G = nx.DiGraph()
    
    for _, row in edges_df.iterrows():
        src, tgt, wgt = row['source'], row['target'], row['weight']
        G.add_edge(src, tgt, weight=wgt)
    
    # Remove self-loops
    self_loops = list(nx.selfloop_edges(G))
    if self_loops:
        G.remove_edges_from(self_loops)
        logging.debug(f"Removed {len(self_loops)} self-loops")
    
    # Filter by degree
    if min_degree > 0:
        degrees = {n: G.in_degree(n) + G.out_degree(n) for n in G.nodes()}
        to_remove = [n for n, d in degrees.items() if d < min_degree]
        if to_remove:
            G.remove_nodes_from(to_remove)
            logging.debug(f"Removed {len(to_remove)} nodes with degree < {min_degree}")
    
    return G


def indegree_centralization(G: nx.DiGraph) -> float:
    """
    Calculate Freeman in-degree centralization (0-1 scale).
    
    Returns:
        Centralization value or NaN if n < 3
    """
    n = G.number_of_nodes()
    if n < 3:
        return float('nan')
    
    deg_in = dict(G.in_degree())
    if not deg_in:
        return float('nan')
    
    max_deg = max(deg_in.values())
    num = sum(max_deg - d for d in deg_in.values())
    den = (n - 1) * (n - 2)
    
    return num / den if den > 0 else float('nan')


def compute_louvain_and_inter(G: nx.DiGraph) -> Tuple[Optional[Dict], float, float]:
    """
    Compute Louvain communities on undirected weighted graph, return partition,
    modularity, and inter-community edge ratio.
    
    Returns:
        (partition_dict, modularity, inter_ratio) - NaN if not applicable
    """
    if community_louvain is None:
        logging.warning("python-louvain not available, community metrics will be NaN")
        return None, float('nan'), float('nan')
    
    if G.number_of_edges() == 0:
        return None, float('nan'), float('nan')
    
    try:
        # Convert to undirected with weights
        G_und = G.to_undirected()
        
        # Run Louvain
        partition = community_louvain.best_partition(G_und, weight='weight')
        
        # Calculate modularity
        modularity = community_louvain.modularity(partition, G_und, weight='weight')
        
        # Calculate inter-community ratio on directed graph
        m = G.number_of_edges()
        inter_edges = sum(
            1 for u, v in G.edges()
            if partition.get(u) != partition.get(v)
        )
        inter_ratio = inter_edges / m if m > 0 else float('nan')
        
        return partition, modularity, inter_ratio
        
    except Exception as e:
        logging.warning(f"Louvain computation failed: {e}")
        return None, float('nan'), float('nan')


def compute_ego_metrics(G: nx.DiGraph, victim_aliases: List[str]) -> Dict[str, float]:
    """
    Compute ego network metrics for victim (1-hop neighborhood).
    
    Returns:
        Dict with ego_n, ego_m, ego_density, ego_reciprocity (NaN if victim not found)
    """
    result = {
        'ego_n': float('nan'),
        'ego_m': float('nan'),
        'ego_density': float('nan'),
        'ego_reciprocity': float('nan')
    }
    
    if not victim_aliases:
        return result
    
    # Normalize aliases
    victim_aliases = [v.lower().strip().lstrip('@') for v in victim_aliases]
    
    # Find victim node
    victim_node = None
    for alias in victim_aliases:
        if alias in G.nodes():
            victim_node = alias
            break
    
    if victim_node is None:
        return result
    
    # Build 1-hop ego network (predecessors + successors + ego)
    preds = set(G.predecessors(victim_node))
    succs = set(G.successors(victim_node))
    ego_nodes = preds | succs | {victim_node}
    
    ego_subgraph = G.subgraph(ego_nodes).copy()
    
    result['ego_n'] = ego_subgraph.number_of_nodes()
    result['ego_m'] = ego_subgraph.number_of_edges()
    
    # Density on undirected
    if result['ego_n'] > 1:
        result['ego_density'] = nx.density(ego_subgraph.to_undirected())
    
    # Reciprocity with victim
    # Count pairs where both (victim, x) and (x, victim) exist
    incident_edges = list(G.in_edges(victim_node)) + list(G.out_edges(victim_node))
    neighbors = set()
    for u, v in incident_edges:
        if u == victim_node:
            neighbors.add(v)
        else:
            neighbors.add(u)
    
    if neighbors:
        reciprocal_count = sum(
            1 for nbr in neighbors
            if G.has_edge(victim_node, nbr) and G.has_edge(nbr, victim_node)
        )
        result['ego_reciprocity'] = reciprocal_count / len(neighbors)
    
    return result


def compute_window_metrics(
    df_slice: pd.DataFrame,
    min_degree: int,
    victim_aliases: List[str],
    use_community: bool
) -> Dict[str, float]:
    """
    Compute all metrics for a single time window.
    
    Returns:
        Dict with all metric values
    """
    metrics = {
        'n_nodes': 0,
        'n_edges': 0,
        'density': float('nan'),
        'indeg_centralization': float('nan'),
        'top1_share_in': float('nan'),
        'intercommunity_ratio': float('nan'),
        'modularity': float('nan'),
        'ego_n': float('nan'),
        'ego_m': float('nan'),
        'ego_density': float('nan'),
        'ego_reciprocity': float('nan')
    }
    
    # Extract edges
    edges_df = extract_edges(df_slice)
    
    if edges_df.empty:
        return metrics
    
    # Build graph
    G = build_graph(edges_df, min_degree)
    
    if G.number_of_nodes() == 0:
        return metrics
    
    # Basic metrics
    metrics['n_nodes'] = G.number_of_nodes()
    metrics['n_edges'] = G.number_of_edges()
    
    if metrics['n_edges'] > 0:
        metrics['density'] = nx.density(G)
    
    # Centralization
    metrics['indeg_centralization'] = indegree_centralization(G)
    
    # Top-1 in-degree share
    if G.number_of_nodes() > 0:
        in_degrees = dict(G.in_degree())
        if in_degrees:
            max_in = max(in_degrees.values())
            sum_in = sum(in_degrees.values())
            if sum_in > 0:
                metrics['top1_share_in'] = max_in / sum_in
    
    # Community metrics
    if use_community and metrics['n_edges'] > 0:
        partition, modularity, inter_ratio = compute_louvain_and_inter(G)
        metrics['modularity'] = modularity
        metrics['intercommunity_ratio'] = inter_ratio
    
    # Ego metrics
    if victim_aliases:
        ego_metrics = compute_ego_metrics(G, victim_aliases)
        metrics.update(ego_metrics)
    
    return metrics


def window_iter(df: pd.DataFrame, freq: str, min_samples: int) -> Iterator[Tuple[pd.Timestamp, pd.Timestamp, pd.DataFrame]]:
    """
    Iterate over time windows.

    Yields:
        (window_start, window_end, df_slice)
    """
    # pandas 2.x uses 'h' (lowercase) for hourly frequency
    freq_pandas = 'h' if freq == 'H' else freq
    grouped = df.groupby(pd.Grouper(key='ts', freq=freq_pandas))
    
    for window_start, group in grouped:
        if len(group) < min_samples:
            continue
        
        # Calculate window end
        if freq == 'H':
            window_end = window_start + pd.Timedelta(hours=1)
        elif freq == 'D':
            window_end = window_start + pd.Timedelta(days=1)
        else:
            window_end = window_start + pd.Timedelta(freq)
        
        yield window_start, window_end, group


def plot_metrics(df: pd.DataFrame, outdir: Path, has_victim: bool) -> None:
    """
    Generate time series plots for key metrics.
    """
    plots_dir = outdir / 'plots'
    plots_dir.mkdir(parents=True, exist_ok=True)
    
    logging.info(f"Generating plots in {plots_dir}")
    
    # Configure matplotlib
    plt.style.use('seaborn-v0_8-darkgrid' if 'seaborn-v0_8-darkgrid' in plt.style.available else 'default')
    
    # Plot definitions: (column, title, filename)
    plots = [
        ('indeg_centralization', 'In-Degree Centralization Over Time', 'centralization_over_time.png'),
        ('top1_share_in', 'Top-1 In-Degree Share Over Time', 'top1_share_over_time.png'),
        ('intercommunity_ratio', 'Inter-Community Edge Ratio Over Time', 'intercommunity_over_time.png'),
        ('density', 'Network Density Over Time', 'density_over_time.png'),
    ]
    
    if has_victim:
        plots.extend([
            ('ego_density', 'Victim Ego Density Over Time', 'ego_density_over_time.png'),
            ('ego_reciprocity', 'Victim Ego Reciprocity Over Time', 'ego_reciprocity_over_time.png'),
        ])
    
    for col, title, filename in plots:
        if col not in df.columns:
            continue
        
        # Skip if all NaN
        if df[col].isna().all():
            logging.debug(f"Skipping plot for {col} (all NaN)")
            continue
        
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(df['window_start'], df[col], marker='o', linewidth=2, markersize=4)
        ax.set_xlabel('Time', fontsize=12)
        ax.set_ylabel(col.replace('_', ' ').title(), fontsize=12)
        ax.set_title(title, fontsize=14, fontweight='bold')
        ax.grid(True, alpha=0.3)
        
        # Format x-axis
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
        fig.autofmt_xdate()
        
        plt.tight_layout()
        outpath = plots_dir / filename
        plt.savefig(outpath, dpi=150, bbox_inches='tight')
        plt.close(fig)
        
        logging.debug(f"Saved {filename}")
    
    logging.info(f"Generated {len(plots)} plot(s)")


def main():
    parser = argparse.ArgumentParser(
        description='Calculate windowed network metrics from JSONL case data.',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Input
    parser.add_argument('--jsonl', nargs='+', required=True,
                        help='JSONL file path(s) or glob pattern(s)')
    parser.add_argument('--victim', type=str, default='',
                        help='Victim alias(es), comma-separated (e.g., "@victim1,@victim2")')
    parser.add_argument('--outdir', type=str, required=True,
                        help='Output directory for results')
    
    # Windowing
    parser.add_argument('--freq', type=str, default='H', choices=['H', 'D'],
                        help='Window frequency: H (hourly) or D (daily)')
    parser.add_argument('--min-samples', type=int, default=1,
                        help='Minimum events per window to process')
    
    # Graph filtering
    parser.add_argument('--min-degree', type=int, default=0,
                        help='Minimum node degree (in+out) to keep')
    parser.add_argument('--core', action='store_true',
                        help='Core filter: set --min-degree to 2')
    
    # Community
    parser.add_argument('--community', type=str, default='louvain',
                        choices=['louvain', 'none'],
                        help='Community detection algorithm')
    
    # Performance
    parser.add_argument('--btw-sample', type=int, default=0,
                        help='Betweenness sampling (0 = disabled, not used)')
    parser.add_argument('--max-windows', type=int, default=0,
                        help='Maximum windows to process (0 = unlimited)')
    
    # Output
    parser.add_argument('--plots', action='store_true',
                        help='Generate PNG plots')
    parser.add_argument('--csv-only', action='store_true',
                        help='CSV only, no plots')
    
    # Logging
    parser.add_argument('--log', type=str, default='INFO',
                        choices=['INFO', 'DEBUG', 'WARNING'],
                        help='Logging level')
    
    args = parser.parse_args()
    
    # Setup
    setup_logging(args.log)
    
    # Apply --core flag
    min_degree = args.min_degree
    if args.core:
        min_degree = max(min_degree, 2)
        logging.info(f"--core flag: using min_degree={min_degree}")
    
    # Parse victim aliases
    victim_aliases = []
    if args.victim:
        victim_aliases = [v.strip() for v in args.victim.split(',') if v.strip()]
        logging.info(f"Victim aliases: {victim_aliases}")
    
    # Output directory
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    logging.info(f"Output directory: {outdir}")
    
    # Community flag
    use_community = (args.community == 'louvain')
    if use_community and community_louvain is None:
        logging.warning("Louvain requested but python-louvain not available")
    
    # Load events
    df = load_events(args.jsonl)
    
    # Process windows
    logging.info(f"Processing windows with freq={args.freq}, min_samples={args.min_samples}")
    
    results = []
    total_windows = 0
    valid_windows = 0
    discarded_windows = 0
    
    for window_start, window_end, df_slice in window_iter(df, args.freq, args.min_samples):
        total_windows += 1
        
        if args.max_windows > 0 and valid_windows >= args.max_windows:
            logging.info(f"Reached max_windows={args.max_windows}, stopping")
            break
        
        logging.debug(f"Processing window {window_start} to {window_end} ({len(df_slice)} events)")
        
        metrics = compute_window_metrics(df_slice, min_degree, victim_aliases, use_community)
        
        # Discard if no edges
        if metrics['n_edges'] == 0:
            discarded_windows += 1
            logging.debug(f"Discarded window (no edges after filtering)")
            continue
        
        valid_windows += 1
        
        # Add window info
        metrics['window_start'] = window_start.isoformat()
        metrics['window_end'] = window_end.isoformat()
        
        results.append(metrics)
        
        if valid_windows % 100 == 0:
            logging.info(f"Processed {valid_windows} valid windows...")
    
    if not results:
        logging.error("No valid windows to export")
        sys.exit(1)
    
    # Create DataFrame
    results_df = pd.DataFrame(results)
    
    # Reorder columns
    col_order = [
        'window_start', 'window_end',
        'n_nodes', 'n_edges', 'density',
        'indeg_centralization', 'top1_share_in',
        'intercommunity_ratio', 'modularity',
        'ego_n', 'ego_m', 'ego_density', 'ego_reciprocity'
    ]
    results_df = results_df[[c for c in col_order if c in results_df.columns]]
    
    # Export CSV
    csv_path = outdir / 'series_windowed.csv'
    results_df.to_csv(csv_path, index=False)
    logging.info(f"Exported {len(results_df)} windows to {csv_path}")
    
    # Statistics
    avg_edges = results_df['n_edges'].mean()
    logging.info(f"Summary: total_windows={total_windows}, valid_windows={valid_windows}, "
                 f"discarded_windows={discarded_windows}, avg_edges={avg_edges:.1f}")
    
    # Generate plots
    if args.plots and not args.csv_only:
        # Convert window_start back to datetime for plotting
        results_df['window_start'] = pd.to_datetime(results_df['window_start'])
        plot_metrics(results_df, outdir, bool(victim_aliases))
    
    logging.info("Done!")


if __name__ == '__main__':
    main()


