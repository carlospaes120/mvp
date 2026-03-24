#!/usr/bin/env python3
"""
Gera o PRE agregado "puro" do caso Monark, sem sobreposição com o bloco
crítico de 3h (2022-02-08 21:00–23:59 em America/Sao_Paulo).

Entrada:
- data/raw/tweets_classificados_monark.jsonl

Saída:
- data/processed/monark/monark_pre_2022-02-07_00-00_2022-02-08_20-59.jsonl

Comportamento:
- usa created_at, se existir; caso contrário, created_at_iso
- timestamps são parseados em UTC
- o recorte temporal é feito em America/Sao_Paulo
- opcionalmente valida ausência de overlap com o arquivo 3h já existente
- opcionalmente chama jsonl_to_gexf.py para gerar o GEXF correspondente

Uso:
    python src/build_monark_pre_aggregate.py
    python src/build_monark_pre_aggregate.py --overwrite
    python src/build_monark_pre_aggregate.py --overwrite --generate-gexf
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple
from zoneinfo import ZoneInfo


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent

INPUT_FILE = REPO_ROOT / "data" / "raw" / "tweets_classificados_monark.jsonl"
OUTPUT_DIR = REPO_ROOT / "data" / "processed" / "monark"
OUTPUT_FILE = OUTPUT_DIR / "monark_pre_2022-02-07_00-00_2022-02-08_20-59.jsonl"

THREE_H_FILE = OUTPUT_DIR / "monark_3h_2022-02-08_21-00_23-59.jsonl"
JSONL_TO_GEXF_SCRIPT = SCRIPT_DIR / "jsonl_to_gexf.py"
GEXF_OUTPUT = REPO_ROOT / "data" / "outputs" / "monark" / "monark_pre_2022-02-07_00-00_2022-02-08_20-59.gexf"

SP_TZ = ZoneInfo("America/Sao_Paulo")
TWITTER_FMT = "%a %b %d %H:%M:%S %z %Y"

# PRE puro: 07/02/2022 00:00:00 até 08/02/2022 20:59:59 em São Paulo
START_SP = datetime(2022, 2, 7, 0, 0, 0, tzinfo=SP_TZ)
END_SP = datetime(2022, 2, 8, 20, 59, 59, tzinfo=SP_TZ)


def parse_utc_timestamp(created_at_str: str) -> Optional[datetime]:
    """Parse Twitter created_at ou created_at_iso como UTC; retorna None em falha."""
    s = created_at_str.strip()
    try:
        if "T" in s and ("+" in s or s.endswith("Z")):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.strptime(s, TWITTER_FMT)
        if dt.tzinfo is None:
            return None
        return dt
    except (ValueError, TypeError):
        return None


def get_timestamp_field(obj: dict) -> Tuple[Optional[str], str]:
    """Prefer created_at; se não houver, usa created_at_iso."""
    if "created_at" in obj and obj["created_at"]:
        return obj["created_at"], "created_at"
    if "created_at_iso" in obj and obj["created_at_iso"]:
        return obj["created_at_iso"], "created_at_iso"
    return None, ""


def read_lines_in_sp_interval(path: Path, start_sp: datetime, end_sp: datetime):
    kept_lines: list[str] = []
    kept_dt_utc: list[datetime] = []
    kept_dt_sp: list[datetime] = []

    total_lines = 0
    skipped = 0
    timestamp_field_used = None

    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            total_lines += 1
            line = raw_line.strip()
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
            if start_sp <= dt_sp <= end_sp:
                kept_lines.append(line)
                kept_dt_utc.append(dt_utc)
                kept_dt_sp.append(dt_sp)

    return {
        "lines": kept_lines,
        "dt_utc": kept_dt_utc,
        "dt_sp": kept_dt_sp,
        "total_lines": total_lines,
        "skipped": skipped,
        "timestamp_field_used": timestamp_field_used,
    }


def validate_no_overlap(pre_last_sp: Optional[datetime], three_h_file: Path) -> tuple[bool, Optional[datetime], int]:
    """Verifica se o primeiro timestamp do 3h ocorre depois do último timestamp do PRE."""
    if pre_last_sp is None or not three_h_file.is_file():
        return True, None, 0

    result = read_lines_in_sp_interval(
        path=three_h_file,
        start_sp=datetime(2022, 2, 8, 21, 0, 0, tzinfo=SP_TZ),
        end_sp=datetime(2022, 2, 8, 23, 59, 59, tzinfo=SP_TZ),
    )

    if not result["dt_sp"]:
        return True, None, 0

    three_h_first_sp = min(result["dt_sp"])
    no_overlap = pre_last_sp < three_h_first_sp
    return no_overlap, three_h_first_sp, len(result["lines"])


def maybe_generate_gexf(input_jsonl: Path, output_gexf: Path) -> None:
    if not JSONL_TO_GEXF_SCRIPT.is_file():
        raise FileNotFoundError(f"Script não encontrado: {JSONL_TO_GEXF_SCRIPT}")

    output_gexf.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        str(JSONL_TO_GEXF_SCRIPT),
        "--input", str(input_jsonl),
        "--output", str(output_gexf),
    ]
    subprocess.run(cmd, check=True)


def format_dt(dt: Optional[datetime]) -> str:
    if dt is None:
        return "None"
    return dt.isoformat(sep=" ")


def main() -> None:
    parser = argparse.ArgumentParser(description="Gera o PRE agregado puro do caso Monark.")
    parser.add_argument("--overwrite", action="store_true", help="Sobrescreve o JSONL de saída, se existir.")
    parser.add_argument("--generate-gexf", action="store_true", help="Gera também o GEXF correspondente usando jsonl_to_gexf.py.")
    args = parser.parse_args()

    if not INPUT_FILE.is_file():
        print(f"Erro: arquivo de entrada não encontrado: {INPUT_FILE.resolve()}", file=sys.stderr)
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if OUTPUT_FILE.exists() and not args.overwrite:
        print(f"Erro: arquivo de saída já existe: {OUTPUT_FILE.resolve()}", file=sys.stderr)
        print("Use --overwrite para sobrescrever.", file=sys.stderr)
        sys.exit(1)

    result = read_lines_in_sp_interval(INPUT_FILE, START_SP, END_SP)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        for line in result["lines"]:
            out.write(line + "\n")

    pre_first_utc = min(result["dt_utc"]) if result["dt_utc"] else None
    pre_last_utc = max(result["dt_utc"]) if result["dt_utc"] else None
    pre_first_sp = min(result["dt_sp"]) if result["dt_sp"] else None
    pre_last_sp = max(result["dt_sp"]) if result["dt_sp"] else None

    no_overlap, three_h_first_sp, three_h_count = validate_no_overlap(pre_last_sp, THREE_H_FILE)

    print("--- Resumo ---")
    print(f"Arquivo de origem: {INPUT_FILE.resolve()}")
    print(f"Campo de timestamp usado: {result['timestamp_field_used'] or 'nenhum'}")
    print("Conversão: UTC -> America/Sao_Paulo (apenas para recorte temporal)")
    print(f"Janela PRE pura (SP): {format_dt(START_SP)} -> {format_dt(END_SP)}")
    print(f"Arquivo gerado: {OUTPUT_FILE.resolve()}")
    print(f"Tweets no novo PRE: {len(result['lines'])}")
    print(f"Primeiro timestamp no PRE (UTC): {format_dt(pre_first_utc)}")
    print(f"Último  timestamp no PRE (UTC): {format_dt(pre_last_utc)}")
    print(f"Primeiro timestamp no PRE (SP):  {format_dt(pre_first_sp)}")
    print(f"Último  timestamp no PRE (SP):  {format_dt(pre_last_sp)}")

    if THREE_H_FILE.is_file():
        print(f"Arquivo 3h de referência: {THREE_H_FILE.resolve()}")
        print(f"Tweets no arquivo 3h: {three_h_count}")
        print(f"Primeiro timestamp no 3h (SP): {format_dt(three_h_first_sp)}")
        print(f"Validação overlap PRE vs 3h: {'OK (sem sobreposição)' if no_overlap else 'FALHA (há sobreposição)'}")
    else:
        print("Validação de overlap: arquivo 3h de referência não encontrado; checagem pulada.")

    if result["skipped"]:
        print(f"Linhas ignoradas (vazias, JSON inválido ou timestamp inválido): {result['skipped']}")

    if args.generate_gexf:
        maybe_generate_gexf(OUTPUT_FILE, GEXF_OUTPUT)
        print(f"GEXF gerado: {GEXF_OUTPUT.resolve()}")


if __name__ == "__main__":
    main()
