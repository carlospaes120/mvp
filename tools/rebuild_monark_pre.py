from pathlib import Path
import json
import shutil
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

PROCESSED_DIR = ROOT / "data" / "processed" / "monark"
OUTPUTS_DIR = ROOT / "data" / "outputs" / "monark"
REPORTS_DIR = ROOT / "reports" / "h1_monark"

ARCHIVE_ROOT = ROOT / "data" / "archive" / "monark_daily_legacy"
ARCHIVE_PROCESSED = ARCHIVE_ROOT / "processed"
ARCHIVE_OUTPUTS = ARCHIVE_ROOT / "outputs"
ARCHIVE_REPORTS = ARCHIVE_ROOT / "reports"

for d in [ARCHIVE_PROCESSED, ARCHIVE_OUTPUTS, ARCHIVE_REPORTS]:
    d.mkdir(parents=True, exist_ok=True)

PRE_FILENAME = "monark_pre_2022-02-07_00-00_2022-02-08_23-59.jsonl"
PRE_PATH = PROCESSED_DIR / PRE_FILENAME

SOURCE_DAILIES = [
    PROCESSED_DIR / "monark_dia_2022-02-07.jsonl",
    PROCESSED_DIR / "monark_dia_2022-02-08.jsonl",
]

ARCHIVE_DAILIES = True  # troque para False se quiser só testar primeiro

def read_jsonl(path: Path) -> pd.DataFrame:
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return pd.DataFrame(rows)

def write_jsonl(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for rec in df.to_dict(orient="records"):
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

def collect_ids_from_jsonl_files(paths: list[Path]) -> set[str]:
    ids = set()
    for path in paths:
        df = read_jsonl(path)
        if "tweet_id" in df.columns:
            ids.update(df["tweet_id"].dropna().astype(str).tolist())
    return ids

def hashable_dedup_subset(df: pd.DataFrame) -> list[str]:
    """
    Escolhe um subconjunto de colunas seguras para deduplicação.
    Ignora colunas com listas/dicts (não-hashables para factorize).
    """
    subset = []
    for col in df.columns:
        if col == "_created_at_dt":
            continue
        series = df[col]
        # ignora colunas totalmente vazias
        non_na = series.dropna()
        if non_na.empty:
            continue
        # se houver listas/dicts, não usar na chave
        if non_na.map(lambda x: isinstance(x, (list, dict))).any():
            continue
        subset.append(col)
    return subset

def main():
    for p in SOURCE_DAILIES:
        if not p.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {p}")

    parts = []
    for p in SOURCE_DAILIES:
        df = read_jsonl(p)
        if "created_at" in df.columns:
            df["_created_at_dt"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
        else:
            df["_created_at_dt"] = pd.NaT
        parts.append(df)

    pre_df = pd.concat(parts, ignore_index=True)

    # ordena e remove duplicatas
    if "_created_at_dt" in pre_df.columns:
        pre_df = pre_df.sort_values("_created_at_dt", na_position="last")

    before = len(pre_df)
    if "tweet_id" in pre_df.columns:
        pre_df["tweet_id"] = pre_df["tweet_id"].astype(str)
        pre_df = pre_df.drop_duplicates(subset=["tweet_id"], keep="first")
    else:
        subset = hashable_dedup_subset(pre_df)
        if subset:
            pre_df = pre_df.drop_duplicates(subset=subset, keep="first")
        else:
            # Fallback extremo: nenhuma coluna hashable disponível
            pre_df = pre_df.reset_index(drop=True)
    after = len(pre_df)

    # salva o novo PRE
    out_df = pre_df.drop(columns=["_created_at_dt"], errors="ignore")
    write_jsonl(out_df, PRE_PATH)

    print(f"PRE criado em: {PRE_PATH}")
    print(f"Linhas antes da deduplicação: {before}")
    print(f"Linhas depois da deduplicação: {after}")

    # checagem de sobreposição com janelas 3h/6h
    fine_files = sorted(PROCESSED_DIR.glob("monark_3h_*.jsonl")) + sorted(PROCESSED_DIR.glob("monark_6h_*.jsonl"))
    fine_ids = collect_ids_from_jsonl_files(fine_files)

    pre_ids = set()
    if "tweet_id" in out_df.columns:
        pre_ids = set(out_df["tweet_id"].dropna().astype(str).tolist())

    overlap = pre_ids.intersection(fine_ids)
    print(f"Arquivos finos encontrados: {len(fine_files)}")
    print(f"Overlap de tweet_id entre PRE e 3h/6h: {len(overlap)}")

    if len(overlap) > 0:
        print("ATENÇÃO: existe sobreposição. Verifique timezone/janelamento antes de arquivar os diários.")
    else:
        print("OK: não foi detectada sobreposição por tweet_id entre PRE e 3h/6h.")

    if ARCHIVE_DAILIES:
        moved = []

        for p in sorted(PROCESSED_DIR.glob("monark_dia_*.jsonl")):
            target = ARCHIVE_PROCESSED / p.name
            shutil.move(str(p), str(target))
            moved.append(str(target))

        for p in sorted(OUTPUTS_DIR.glob("monark_dia_*.gexf")):
            target = ARCHIVE_OUTPUTS / p.name
            shutil.move(str(p), str(target))
            moved.append(str(target))

        for p in sorted(OUTPUTS_DIR.glob("monark_dia_*.csv")):
            target = ARCHIVE_OUTPUTS / p.name
            shutil.move(str(p), str(target))
            moved.append(str(target))

        for p in sorted(REPORTS_DIR.glob("monark_dia_*.gexf")):
            target = ARCHIVE_REPORTS / p.name
            shutil.move(str(p), str(target))
            moved.append(str(target))

        for p in sorted(REPORTS_DIR.glob("monark_dia_*.csv")):
            target = ARCHIVE_REPORTS / p.name
            shutil.move(str(p), str(target))
            moved.append(str(target))

        print("\nArquivos diários arquivados:")
        for item in moved:
            print(" -", item)

    print("\nPróximo passo:")
    print(
        "python src/jsonl_to_gexf.py "
        f"--input {PRE_PATH.as_posix()} "
        f"--output {(OUTPUTS_DIR / PRE_FILENAME.replace('.jsonl', '.gexf')).as_posix()}"
    )

if __name__ == "__main__":
    main()