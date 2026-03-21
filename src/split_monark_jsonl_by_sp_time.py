#!/usr/bin/env python3
"""
Split tweets_classificados_monark.jsonl by São Paulo local date/time.
- Daily files (all days in dataset).
- 3-hour windows for 2022-02-08.
- 6-hour windows for 2022-02-09.
Timestamps in source are UTC; conversion to America/Sao_Paulo is used only for bucketing.
Uses only stdlib: json, pathlib, datetime, zoneinfo.
"""

import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

# Paths relative to repository root (parent of src/)
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
INPUT_FILE = REPO_ROOT / "data" / "raw" / "tweets_classificados_monark.jsonl"
OUTPUT_DIR = REPO_ROOT / "data" / "processed" / "monark"

# Timezone for bucketing
SP_TZ = ZoneInfo("America/Sao_Paulo")

# Twitter created_at format: "Mon Feb 14 23:24:08 +0000 2022"
TWITTER_FMT = "%a %b %d %H:%M:%S %z %Y"


def parse_utc_timestamp(created_at_str: str) -> Optional[datetime]:
    """Parse Twitter created_at or created_at_iso string as UTC; return None on failure."""
    s = created_at_str.strip()
    try:
        if "T" in s and "+" in s:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.strptime(s, TWITTER_FMT)
        if dt.tzinfo is None:
            return None
        return dt
    except (ValueError, TypeError):
        return None


def get_timestamp_field(obj: dict) -> Tuple[Optional[str], str]:
    """Return (value, field_name). Prefer created_at, else created_at_iso. Reports which was used."""
    if "created_at" in obj and obj["created_at"]:
        return (obj["created_at"], "created_at")
    if "created_at_iso" in obj and obj["created_at_iso"]:
        return (obj["created_at_iso"], "created_at_iso")
    return (None, "")


def main() -> None:
    if not OUTPUT_DIR.is_dir():
        print(f"Erro: pasta de saída não encontrada: {OUTPUT_DIR.resolve()}", file=sys.stderr)
        print("Crie a pasta 'tweets_por_dia' no diretório raiz do projeto e execute novamente.", file=sys.stderr)
        sys.exit(1)

    if not INPUT_FILE.is_file():
        print(f"Erro: arquivo de entrada não encontrado: {INPUT_FILE.resolve()}", file=sys.stderr)
        sys.exit(1)

    # Buckets: day -> list of raw JSON lines (to preserve order and exact content)
    daily: dict[str, list[str]] = {}
    feb8_3h: dict[str, list[str]] = {}
    feb9_6h: dict[str, list[str]] = {}

    timestamp_field_used: str | None = None
    total_lines = 0
    skipped = 0

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            total_lines += 1
            line = line.strip()
            if not line:
                skipped += 1
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue

            ts_val, ts_field = get_timestamp_field(obj)
            if timestamp_field_used is None and ts_field:
                timestamp_field_used = ts_field
            if not ts_val:
                skipped += 1
                continue

            dt_utc = parse_utc_timestamp(ts_val)
            if dt_utc is None:
                skipped += 1
                continue

            dt_sp = dt_utc.astimezone(SP_TZ)
            date_key = dt_sp.strftime("%Y-%m-%d")

            # Daily bucket
            if date_key not in daily:
                daily[date_key] = []
            daily[date_key].append(line)

            # 2022-02-08: 3-hour windows (0-2, 3-5, 6-8, 9-11, 12-14, 15-17, 18-20, 21-23)
            if date_key == "2022-02-08":
                slot = dt_sp.hour // 3
                start_h = slot * 3
                end_h = start_h + 2
                key = f"{start_h:02d}-00_{end_h:02d}-59"
                if key not in feb8_3h:
                    feb8_3h[key] = []
                feb8_3h[key].append(line)

            # 2022-02-09: 6-hour windows (0-5, 6-11, 12-17, 18-23)
            if date_key == "2022-02-09":
                slot = dt_sp.hour // 6
                start_h = slot * 6
                end_h = start_h + 5
                key = f"{start_h:02d}-00_{end_h:02d}-59"
                if key not in feb9_6h:
                    feb9_6h[key] = []
                feb9_6h[key].append(line)

    # Write daily files
    for date_key in sorted(daily.keys()):
        out_path = OUTPUT_DIR / f"monark_dia_{date_key}.jsonl"
        with open(out_path, "w", encoding="utf-8") as out:
            for ln in daily[date_key]:
                out.write(ln + "\n")

    # Write 3h windows for 2022-02-08 (all 8 slots; empty = don't write or write empty - we don't write empty)
    for start_h in range(0, 24, 3):
        end_h = start_h + 2
        key = f"{start_h:02d}-00_{end_h:02d}-59"
        out_path = OUTPUT_DIR / f"monark_3h_2022-02-08_{key}.jsonl"
        if key in feb8_3h and feb8_3h[key]:
            with open(out_path, "w", encoding="utf-8") as out:
                for ln in feb8_3h[key]:
                    out.write(ln + "\n")

    # Write 6h windows for 2022-02-09
    for start_h in range(0, 24, 6):
        end_h = start_h + 5
        key = f"{start_h:02d}-00_{end_h:02d}-59"
        out_path = OUTPUT_DIR / f"monark_6h_2022-02-09_{key}.jsonl"
        if key in feb9_6h and feb9_6h[key]:
            with open(out_path, "w", encoding="utf-8") as out:
                for ln in feb9_6h[key]:
                    out.write(ln + "\n")

    # Summary
    print("--- Resumo ---")
    print(f"1. Arquivo de origem: {INPUT_FILE.resolve()}")
    print(f"2. Campo de timestamp usado: {timestamp_field_used or 'nenhum (todas as linhas ignoradas)'}")
    print("3. Conversão: UTC -> America/Sao_Paulo (apenas para recorte temporal)")
    print("4. Arquivos gerados:")

    generated = []
    for date_key in sorted(daily.keys()):
        p = OUTPUT_DIR / f"monark_dia_{date_key}.jsonl"
        if p.exists():
            generated.append(str(p))
    for key in sorted(feb8_3h.keys()):
        p = OUTPUT_DIR / f"monark_3h_2022-02-08_{key}.jsonl"
        if p.exists():
            generated.append(str(p))
    for key in sorted(feb9_6h.keys()):
        p = OUTPUT_DIR / f"monark_6h_2022-02-09_{key}.jsonl"
        if p.exists():
            generated.append(str(p))
    for g in sorted(generated):
        print(f"   {g}")

    print("5. Contagem por arquivo diário:")
    for date_key in sorted(daily.keys()):
        print(f"   monark_dia_{date_key}.jsonl: {len(daily[date_key])} tweets")

    print("6. Contagem por janela de 3h (2022-02-08):")
    for start_h in range(0, 24, 3):
        end_h = start_h + 2
        key = f"{start_h:02d}-00_{end_h:02d}-59"
        count = len(feb8_3h.get(key, []))
        print(f"   monark_3h_2022-02-08_{key}.jsonl: {count} tweets")

    print("7. Contagem por janela de 6h (2022-02-09):")
    for start_h in range(0, 24, 6):
        end_h = start_h + 5
        key = f"{start_h:02d}-00_{end_h:02d}-59"
        count = len(feb9_6h.get(key, []))
        print(f"   monark_6h_2022-02-09_{key}.jsonl: {count} tweets")

    empty_3h = [f"monark_3h_2022-02-08_{start:02d}-00_{start+2:02d}-59.jsonl" for start in range(0, 24, 3) if len(feb8_3h.get(f"{start:02d}-00_{start+2:02d}-59", [])) == 0]
    empty_6h = [f"monark_6h_2022-02-09_{start:02d}-00_{start+5:02d}-59.jsonl" for start in range(0, 24, 6) if len(feb9_6h.get(f"{start:02d}-00_{start+5:02d}-59", [])) == 0]
    if empty_3h or empty_6h:
        print("\nBuckets com zero registros (arquivo não gerado):")
        for name in empty_3h + empty_6h:
            print(f"   {name}")
    if skipped:
        print(f"\nLinhas ignoradas (vazias, JSON inválido ou timestamp inválido): {skipped}")


if __name__ == "__main__":
    main()
