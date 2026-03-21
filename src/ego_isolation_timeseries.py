#!/usr/bin/env python3
"""
ego_isolation_timeseries.py

Calcula séries temporais de isolamento da vítima usando janelas deslizantes:
1. ego_density - densidade do ego-network da vítima
2. avg_dist_to_victim - distância média até a vítima

Suporta CSV ou JSONL com timestamps e gera gráficos + CSV de saída.
"""

import argparse
import os
from pathlib import Path
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

# ============ Config via CLI ============
def parse_args():
    p = argparse.ArgumentParser(description="Séries de ego_density e avg_dist_to_victim por janelas deslizantes.")
    p.add_argument("--input", required=True, help="Caminho do CSV ou JSONL de arestas (src,dst,timestamp).")
    p.add_argument("--case_id", required=True, help="Identificador do caso, ex.: monark, karol, wagner, bueno.")
    p.add_argument("--victim", required=True, help="ID/handle do nó da vítima (tem que bater com src/dst).")
    p.add_argument("--timecol", default="timestamp", help="Nome da coluna de tempo (default: timestamp).")
    p.add_argument("--srccol", default="src", help="Nome da coluna de origem (default: src).")
    p.add_argument("--dstcol", default="dst", help="Nome da coluna de destino (default: dst).")
    p.add_argument("--window", default="1D", help="Tamanho da janela (p.ex. 1H, 6H, 1D).")
    p.add_argument("--directed", action="store_true", help="Tratar grafo como dirigido para ego_density.")
    p.add_argument("--format", default="csv", choices=["csv","jsonl"], help="Formato do arquivo de entrada.")
    p.add_argument("--outdir", default="out", help="Diretório base de saída.")
    p.add_argument("--anchor_peak", action="store_true", help="Gerar coluna t_relativa ancorada no pico de volume.")
    return p.parse_args()

# ============ Métricas ============
def build_graph(edges_df, srccol, dstcol, directed=True):
    """Constrói grafo a partir de DataFrame de arestas."""
    G = nx.DiGraph() if directed else nx.Graph()
    G.add_edges_from(edges_df[[srccol, dstcol]].itertuples(index=False, name=None))
    return G

def ego_density(G, v, directed=False):
    """
    Calcula densidade do ego-network da vítima.
    
    Args:
        G: NetworkX graph
        v: ID da vítima
        directed: Se True, usa fórmula dirigida; senão, não-dirigida
    
    Returns:
        Densidade (float) ou None se vítima não existe
    """
    if v not in G:
        return None
    
    if directed and isinstance(G, nx.DiGraph):
        # Ego-network dirigido: predecessores + sucessores
        nbrs = set(G.predecessors(v)) | set(G.successors(v))
        H = G.subgraph(nbrs).copy()
        n, m = H.number_of_nodes(), H.number_of_edges()
        return 0.0 if n <= 1 else m / (n*(n-1))
    else:
        # Ego-network não-dirigido
        H = nx.ego_graph(G.to_undirected() if isinstance(G, nx.DiGraph) else G, v, radius=1)
        H.remove_node(v)
        n, m = H.number_of_nodes(), H.number_of_edges()
        return 0.0 if n <= 1 else (2*m) / (n*(n-1))

def avg_distance_to_victim(G, v):
    """
    Calcula distância média de todos os nós até a vítima.
    Sempre usa grafo não-dirigido para estabilidade.
    
    Args:
        G: NetworkX graph
        v: ID da vítima
    
    Returns:
        Distância média (float) ou None se vítima não existe
    """
    if v not in G:
        return None
    
    # Converte para não-dirigido
    H = G.to_undirected() if isinstance(G, nx.DiGraph) else G
    
    try:
        lengths = nx.single_source_shortest_path_length(H, v)
    except nx.NetworkXError:
        return None
    
    # Remove o próprio nó da vítima
    lengths = {u:d for u,d in lengths.items() if u != v}
    
    if not lengths:
        return None
    
    return sum(lengths.values())/len(lengths)

# ============ Main ============
def main():
    args = parse_args()
    inpath = Path(args.input)
    case_dir = Path(args.outdir) / args.case_id
    case_dir.mkdir(parents=True, exist_ok=True)

    print(f"📂 Lendo dados de: {inpath}")
    print(f"🎯 Caso: {args.case_id}")
    print(f"👤 Vítima: {args.victim}")
    print(f"⏱️  Janela: {args.window}")
    print(f"📍 Saída: {case_dir}")
    print()

    # Leitura do arquivo
    if args.format == "csv":
        df = pd.read_csv(inpath)
    else:
        df = pd.read_json(inpath, lines=True)

    # Validação de colunas
    required_cols = [args.timecol, args.srccol, args.dstcol]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"❌ Colunas faltando no arquivo: {missing}\n"
                        f"   Colunas disponíveis: {list(df.columns)}")

    print(f"✅ Arquivo carregado: {len(df)} arestas")
    
    # Parse de tempo
    df[args.timecol] = pd.to_datetime(df[args.timecol], errors="coerce", utc=True)
    before = len(df)
    df = df.dropna(subset=[args.timecol])
    after = len(df)
    if before > after:
        print(f"⚠️  {before - after} linhas descartadas (timestamp inválido)")
    
    df = df.sort_values(args.timecol)
    
    print(f"📅 Período: {df[args.timecol].min()} → {df[args.timecol].max()}")

    # Criar janelas regulares
    start = df[args.timecol].min().floor("D")
    end   = df[args.timecol].max().ceil("D")
    
    # Gera intervalos de janela
    bins = pd.interval_range(start=start, end=end, freq=args.window, closed="left")
    print(f"🔢 Total de janelas: {len(bins)}")
    
    # Mapeia cada linha a uma janela
    df["__bin"] = pd.cut(df[args.timecol], bins)
    
    # Processa cada janela
    print("\n🔄 Processando janelas...")
    rows = []
    victim_found_count = 0
    
    for i, iv in enumerate(bins):
        w = df[df["__bin"] == iv]
        t = iv.left  # timestamp representativo da janela
        
        if w.empty:
            rows.append({"t": t, "ego_density": None, "avg_dist": None, "volume": 0})
            continue

        # Constrói grafo (sempre dirigido inicialmente)
        G = build_graph(w, args.srccol, args.dstcol, directed=True)
        
        # Calcula métricas
        ed = ego_density(G, args.victim, directed=args.directed)
        ad = avg_distance_to_victim(G, args.victim)
        
        if ed is not None:
            victim_found_count += 1
        
        rows.append({
            "t": t, 
            "ego_density": ed, 
            "avg_dist": ad, 
            "volume": len(w)
        })
        
        if (i + 1) % 10 == 0:
            print(f"   Processadas {i+1}/{len(bins)} janelas...")

    print(f"✅ Processamento concluído!")
    print(f"   Vítima encontrada em {victim_found_count}/{len(bins)} janelas")
    
    if victim_found_count == 0:
        print(f"\n⚠️  AVISO: Vítima '{args.victim}' não encontrada em nenhuma janela!")
        print(f"   Verifique se o ID da vítima está correto.")
        print(f"   Exemplos de nós no grafo:")
        sample_nodes = set()
        for iv in bins[:5]:  # Primeiras 5 janelas
            w = df[df["__bin"] == iv]
            if not w.empty:
                G = build_graph(w, args.srccol, args.dstcol, directed=True)
                sample_nodes.update(list(G.nodes())[:10])
                if len(sample_nodes) >= 10:
                    break
        for node in list(sample_nodes)[:10]:
            print(f"      - {node}")

    # Cria DataFrame de saída
    out = pd.DataFrame(rows).sort_values("t").reset_index(drop=True)

    # Âncora no pico (opcional): usa pico de volume
    if args.anchor_peak and (out["volume"].max() > 0):
        t_peak = out.loc[out["volume"].idxmax(), "t"]
        print(f"\n📍 Pico de volume em: {t_peak}")
        # Delta em unidades de janela
        out["t_rel_janelas"] = ((out["t"] - t_peak) / (pd.to_timedelta(args.window))).astype("int64")

    # Salva CSV
    csv_path = case_dir / f"metrics_{args.case_id}.csv"
    out.to_csv(csv_path, index=False)
    print(f"\n💾 CSV salvo: {csv_path}")

    # Gera gráficos
    print("\n📊 Gerando gráficos...")
    
    def _plot(series_col, title, ylabel, png_name):
        """Helper para plotar séries temporais."""
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Filtrar valores não-nulos para plotagem
        plot_data = out[out[series_col].notna()]
        
        if len(plot_data) == 0:
            print(f"   ⚠️  {png_name}: sem dados para plotar")
            return
        
        ax.plot(plot_data["t"], plot_data[series_col], 
               marker="o", linewidth=2, markersize=4, color='steelblue')
        ax.set_title(title, fontsize=14, fontweight='bold')
        ax.set_xlabel("Janela (t)", fontsize=12)
        ax.set_ylabel(ylabel, fontsize=12)
        ax.grid(True, alpha=0.3)
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        
        outpath = case_dir / png_name
        plt.savefig(outpath, dpi=160, bbox_inches='tight')
        plt.close()
        print(f"   ✅ {png_name}")

    _plot("ego_density",
          f"Densidade do ego-network da vítima — {args.case_id}",
          "Ego Density",
          f"ego_density_{args.case_id}.png")

    _plot("avg_dist",
          f"Distância média até a vítima — {args.case_id}",
          "Distância Média (caminho mais curto)",
          f"avg_dist_{args.case_id}.png")

    # Gráfico de volume (bônus)
    _plot("volume",
          f"Volume de arestas por janela — {args.case_id}",
          "Número de Arestas",
          f"volume_{args.case_id}.png")

    print(f"\n✅ Concluído! Resultados em: {case_dir}")

if __name__ == "__main__":
    main()






