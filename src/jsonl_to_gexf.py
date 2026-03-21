"""
Gera grafo GEXF a partir de JSONL de tweets.
Compatível com formato GraphQL (Twitter/X): usa legacy.user_id_str e id_str como IDs,
screen_name como label quando existir.
"""
import json
import re
import argparse
from collections import defaultdict
import networkx as nx


def dig(obj, *keys):
    cur = obj
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur


def get_author_id(t):
    """Autor do tweet: ID robusto (GraphQL usa legacy.user_id_str). Formato MVP: user (screen_name)."""
    uid = dig(t, "legacy", "user_id_str")
    if uid:
        return str(uid).strip()
    res = dig(t, "core", "user_results", "result")
    if isinstance(res, dict):
        uid = res.get("rest_id")
        if uid:
            return str(uid).strip()
        uid = res.get("id")  # pode ser base64
        if uid:
            return str(uid).strip()
    # Formato MVP/processado: "user" é o screen_name (usado como id quando não há legacy)
    u = t.get("user")
    if u and isinstance(u, str) and u.strip():
        return u.strip()
    return None


def get_author_label(t):
    """Label do autor: screen_name quando existir."""
    sn = dig(t, "core", "user_results", "result", "legacy", "screen_name")
    if sn:
        return str(sn).strip()
    sn = dig(t, "core", "user_results", "result", "core", "screen_name")
    if sn:
        return str(sn).strip()
    sn = dig(t, "user", "screen_name") or dig(t, "user", "username")
    if sn:
        return str(sn).strip()
    # Formato MVP: "user" é string (screen_name)
    u = t.get("user")
    if u and isinstance(u, str) and u.strip():
        return u.strip()
    return None


def get_mention_targets(t):
    """Arestas de menção: (author_id, target_id) com target_id de legacy.entities.user_mentions[*].id_str ou MVP mentions[*].id_str."""
    out = []
    # Formato MVP: "mentions" é lista de {id_str, name, username}
    ents = t.get("mentions")
    if isinstance(ents, list):
        for m in ents:
            if not isinstance(m, dict):
                continue
            tid = m.get("id_str")
            if not tid:
                tid = m.get("id")
            if tid:
                out.append((str(tid).strip(), m.get("username") or m.get("name") or str(tid)))
            else:
                un = m.get("username") or m.get("name")
                if un:
                    out.append((str(un).strip(), un))
        if out:
            return out
    # GraphQL
    ents = dig(t, "legacy", "entities", "user_mentions")
    if not isinstance(ents, list):
        return out
    for m in ents:
        if not isinstance(m, dict):
            continue
        tid = m.get("id_str")
        if not tid:
            tid = m.get("id")
        if tid:
            out.append((str(tid).strip(), m.get("screen_name") or m.get("username")))
    return out


def get_reply_target(t):
    """Alvo de reply: legacy.in_reply_to_user_id_str ou MVP in_reply_to_user (screen_name)."""
    uid = dig(t, "legacy", "in_reply_to_user_id_str")
    if not uid:
        uid = dig(t, "legacy", "in_reply_to_user_id") or dig(t, "in_reply_to_user_id_str")
    if uid:
        label = dig(t, "legacy", "in_reply_to_screen_name") or dig(t, "in_reply_to_screen_name")
        return str(uid).strip(), (str(label).strip() if label else None)
    # Formato MVP: in_reply_to_user é screen_name (string)
    reply_user = t.get("in_reply_to_user")
    if reply_user and isinstance(reply_user, str) and reply_user.strip():
        return reply_user.strip(), reply_user.strip()
    return None, None


def get_retweet_target(text):
    """Alvo de RT por regex no texto (retorna screen_name; sem id_str no RT puro)."""
    if not text:
        return None
    m = re.match(r"^\s*RT\s+@([A-Za-z0-9_]{1,15})\s*:", text.strip(), re.IGNORECASE)
    return m.group(1).strip() if m else None


def main():
    ap = argparse.ArgumentParser(
        description="Converte JSONL de tweets (1 por linha) em grafo GEXF. Compatível com formato GraphQL (GKay)."
    )
    ap.add_argument("--input", required=True, help="Arquivo JSONL (1 tweet por linha)")
    ap.add_argument("--output", required=True, help="Arquivo .gexf de saída")
    ap.add_argument("--min-weight", type=float, default=1.0, help="Filtrar arestas com peso < min-weight")
    ap.add_argument(
        "--max-lines",
        type=int,
        default=0,
        help="Máximo de linhas a processar (0 = sem limite, útil para debug)",
    )
    args = ap.parse_args()

    weights = defaultdict(float)
    node_labels = {}  # id -> label (screen_name ou id)

    lines_done = 0
    with open(args.input, "r", encoding="utf-8") as f:
        for line in f:
            if args.max_lines and lines_done >= args.max_lines:
                break
            line = line.strip()
            if not line:
                continue
            try:
                t = json.loads(line)
            except Exception:
                continue
            lines_done += 1

            author_id = get_author_id(t)
            if not author_id:
                continue

            label = get_author_label(t)
            node_labels[author_id] = label or author_id

            text = dig(t, "legacy", "full_text") or dig(t, "full_text") or dig(t, "text") or ""

            # Mentions (por id_str)
            for target_id, target_label in get_mention_targets(t):
                if target_id and target_id != author_id:
                    weights[(author_id, target_id)] += 1.0
                    if target_id not in node_labels:
                        node_labels[target_id] = target_label or target_id

            # Reply (por in_reply_to_user_id_str)
            reply_id, reply_label = get_reply_target(t)
            if reply_id and reply_id != author_id:
                weights[(author_id, reply_id)] += 1.0
                if reply_id not in node_labels:
                    node_labels[reply_id] = reply_label or reply_id

            # Retweet (por texto; target é screen_name – tratamos como nó sem id, usar screen_name como id)
            rtt = get_retweet_target(text)
            if rtt and author_id:
                # Em GraphQL não temos id do RT no texto; usamos screen_name como id do nó
                rt_id = rtt
                weights[(author_id, rt_id)] += 1.0
                if rt_id not in node_labels:
                    node_labels[rt_id] = rtt

    G = nx.DiGraph()
    for nid, label in node_labels.items():
        if not nid:
            continue
        G.add_node(nid, label=label or nid)

    for (u, v), w in weights.items():
        if w >= args.min_weight and u in G and v in G:
            G.add_edge(u, v, weight=float(w))

    if G.number_of_nodes() == 0 or G.number_of_edges() == 0:
        raise SystemExit(f"Grafo vazio: nodes={G.number_of_nodes()} edges={G.number_of_edges()}")

    nx.write_gexf(G, args.output)
    print(f"OK: {args.output}")
    print(f"nodes={G.number_of_nodes()} edges={G.number_of_edges()}")


if __name__ == "__main__":
    main()
