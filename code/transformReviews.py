#!/usr/bin/env python3
"""
Batch transform multilingual review CSVs into clean, consistent data.

Features:
- Per-file encoding detection (fallback to UTF-8)
- Delimiter sniffing (comma, semicolon, tab, pipe)
- Column normalization to a canonical schema
- Date parsing to ISO 8601 (YYYY-MM-DD)
- Numeric coercion for playtime and helpfulness
- Optional language detection
- Output: cleaned CSVs + optional consolidated Excel workbook

Requires: pandas, python-dateutil, openpyxl (for Excel), chardet (optional), langdetect (optional)
"""

import os
import io
import sys
import csv
import glob
import time
import argparse
import warnings
from typing import Optional, Dict, List, Tuple

import pandas as pd
from dateutil import parser as dateparser

# Optional libraries
try:
    import chardet
    HAS_CHARDET = True
except ImportError:
    HAS_CHARDET = False

try:
    from langdetect import detect, DetectorFactory
    DetectorFactory.seed = 0  # make language detection deterministic
    HAS_LANGDETECT = True
except ImportError:
    HAS_LANGDETECT = False

warnings.simplefilter("ignore", category=UserWarning)

CANONICAL_COLUMNS = [
    "user", "playtime", "post_date", "helpfulness",
    "review", "recommend", "early_access_review"
]

COLUMN_ALIASES = {
    "username": "user",
    "player": "user",
    "hours": "playtime",
    "time_played": "playtime",
    "date": "post_date",
    "posted_on": "post_date",
    "helpful": "helpfulness",
    "helpfulness_score": "helpfulness",
    "text": "review",
    "content": "review",
    "recommended": "recommend",
    "recommendation": "recommend",
    "ea_review": "early_access_review",
    "early_access": "early_access_review",
}


def parse_args():
    p = argparse.ArgumentParser(description="Clean and standardize review CSVs in batch.")
    p.add_argument("--input", "-i", default="./raw_csvs", help="Input folder containing raw CSV files.")
    p.add_argument("--output", "-o", default="./cleaned_csvs", help="Output folder for cleaned CSV files.")
    p.add_argument("--pattern", "-p", default="*.csv", help="Glob pattern for input files.")
    p.add_argument("--excel", "-x", default=None, help="Path to consolidated Excel workbook to write (optional).")
    p.add_argument("--default-encoding", default="utf-8", help="Fallback encoding if detection is uncertain.")
    p.add_argument("--detect-encoding", action="store_true", help="Enable chardet-based encoding detection.")
    p.add_argument("--encoding-threshold", type=float, default=0.50, help="Confidence threshold for encoding detection.")
    p.add_argument("--language", action="store_true", help="Add language column using langdetect (optional).")
    p.add_argument("--min-playtime", type=float, default=None, help="Filter: keep rows with playtime >= value (hours).")
    p.add_argument("--min-helpfulness", type=int, default=None, help="Filter: keep rows with helpfulness >= value.")
    p.add_argument("--drop-empty-reviews", action="store_true", help="Drop rows with empty review text.")
    p.add_argument("--quiet", action="store_true", help="Reduce console output.")
    return p.parse_args()


def ensure_folder(path: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def detect_encoding(file_path: str, default_encoding: str, threshold: float) -> str:
    if not HAS_CHARDET:
        return default_encoding
    try:
        with open(file_path, "rb") as f:
            raw = f.read(200_000)
        result = chardet.detect(raw)
        enc = (result.get("encoding") or default_encoding)
        conf = (result.get("confidence") or 0.0)
        if conf < threshold:
            return default_encoding
        enc_norm = enc.lower().replace("-", "")
        return "utf-8" if "utf8" in enc_norm else enc
    except Exception:
        return default_encoding


def sniff_delimiter(file_path: str, encoding: str) -> str:
    sample_lines = []
    try:
        with io.open(file_path, "r", encoding=encoding, errors="replace") as f:
            for _ in range(500):
                line = f.readline()
                if not line:
                    break
                sample_lines.append(line)
    except Exception:
        pass
    sample = "".join(sample_lines)
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(sample, delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except Exception:
        return ","


def read_csv_flexible(file_path: str, encoding: str, delimiter: Optional[str]) -> pd.DataFrame:
    sep = delimiter or sniff_delimiter(file_path, encoding)
    try:
        return pd.read_csv(
            file_path,
            encoding=encoding,
            sep=sep,
            dtype=str,
            on_bad_lines="skip",
            engine="python",
        )
    except Exception:
        # Fallback to default UTF-8 + comma
        return pd.read_csv(
            file_path,
            encoding="utf-8",
            sep=",",
            dtype=str,
            on_bad_lines="skip",
            engine="python",
        )


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    new_cols = []
    for c in df.columns:
        c_norm = (c or "").strip().lower()
        c_norm = COLUMN_ALIASES.get(c_norm, c_norm)
        new_cols.append(c_norm)
    df.columns = new_cols

    # Add missing canonical columns as NA
    for col in CANONICAL_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    # Reorder to canonical order
    return df[CANONICAL_COLUMNS]


def parse_date_safe(val: Optional[str]) -> Optional[pd.Timestamp]:
    if val is None or pd.isna(val):
        return pd.NaT
    s = str(val).strip()
    if s == "" or s == "########":
        return pd.NaT
    # Try common explicit formats first
    for fmt in ("%m/%d/%Y", "%d-%b-%y", "%d-%b-%Y", "%Y-%m-%d", "%m/%d/%y", "%d/%m/%Y", "%d/%m/%y"):
        try:
            return pd.to_datetime(s, format=fmt, errors="coerce")
        except Exception:
            pass
    # Fallback: dateutil parse
    try:
        dt = dateparser.parse(s, fuzzy=True, dayfirst=False, yearfirst=False)
        return pd.to_datetime(dt)
    except Exception:
        return pd.NaT


def standardize_dates(df: pd.DataFrame) -> pd.DataFrame:
    if "post_date" in df.columns:
        ts = df["post_date"].apply(parse_date_safe)
        df["post_date"] = ts.dt.strftime("%Y-%m-%d").where(~ts.isna(), pd.NA)
    return df


def coerce_numeric(df: pd.DataFrame) -> pd.DataFrame:
    if "playtime" in df.columns:
        s = df["playtime"].astype(str).str.replace(",", "", regex=False).str.strip()
        df["playtime"] = pd.to_numeric(s, errors="coerce")
    if "helpfulness" in df.columns:
        s = df["helpfulness"].astype(str).str.replace(",", "", regex=False).str.strip()
        df["helpfulness"] = pd.to_numeric(s, errors="coerce", downcast="integer")
    return df


def trim_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip().where(~df[col].isna(), pd.NA)
    return df


def add_language(df: pd.DataFrame) -> pd.DataFrame:
    if not HAS_LANGDETECT:
        df["lang"] = pd.NA
        return df

    def detect_lang(text: Optional[str]) -> Optional[str]:
        if text is None or pd.isna(text):
            return pd.NA
        t = str(text).strip()
        if not t:
            return pd.NA
        try:
            return detect(t)
        except Exception:
            return pd.NA

    df["lang"] = df["review"].apply(detect_lang)
    return df


def clean_recommend(df: pd.DataFrame) -> pd.DataFrame:
    if "recommend" in df.columns:
        df["recommend"] = (
            df["recommend"].astype(str).str.strip()
            .str.replace(r"^\s*recommended\s*$", "Recommended", regex=True)
        )
    return df


def filter_rows(df: pd.DataFrame, min_play: Optional[float], min_help: Optional[int], drop_empty_reviews: bool) -> pd.DataFrame:
    if min_play is not None and "playtime" in df.columns:
        df = df[df["playtime"].fillna(-1) >= float(min_play)]
    if min_help is not None and "helpfulness" in df.columns:
        df = df[df["helpfulness"].fillna(-1) >= int(min_help)]
    if drop_empty_reviews and "review" in df.columns:
        df = df[~df["review"].isna() & (df["review"].astype(str).str.strip() != "")]
    return df


def safe_sheet_name(name: str) -> str:
    invalid_chars = ['\\', '/', '*', '?', ':', '[', ']']
    for ch in invalid_chars:
        name = name.replace(ch, "_")
    return name[:31] if len(name) > 31 else name


def write_clean_csv(df: pd.DataFrame, src_path: str, out_folder: str):
    ensure_folder(out_folder)
    base = os.path.basename(src_path)
    out_path = os.path.join(out_folder, base)
    df.to_csv(out_path, index=False, encoding="utf-8")


def export_excel(dfs: List[Tuple[str, pd.DataFrame]], excel_path: str):
    if excel_path is None or not dfs:
        return
    ensure_folder(os.path.dirname(os.path.abspath(excel_path)) or ".")
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        for fname, df in dfs:
            sheet = safe_sheet_name(os.path.splitext(os.path.basename(fname))[0])
            df.to_excel(writer, sheet_name=sheet, index=False)


def process_file(file_path: str, args) -> Tuple[str, pd.DataFrame]:
    enc = detect_encoding(file_path, args.default_encoding, args.encoding_threshold) if args.detect_encoding else args.default_encoding
    df = read_csv_flexible(file_path, enc, delimiter=None)
    df = normalize_columns(df)
    df = trim_whitespace(df)
    df = standardize_dates(df)
    df = coerce_numeric(df)
    df = clean_recommend(df)
    if args.language:
        df = add_language(df)
    df = filter_rows(df, args.min_playtime, args.min_helpfulness, args.drop_empty_reviews)
    return file_path, df


def main():
    args = parse_args()

    pattern = os.path.join(args.input, args.pattern)
    files = sorted(glob.glob(pattern))
    if not args.quiet:
        print(f"Found {len(files)} CSV files in {os.path.abspath(args.input)}")

    cleaned_pairs = []
    start = time.time()

    for i, fp in enumerate(files, 1):
        try:
            fname, df = process_file(fp, args)
            write_clean_csv(df, fname, args.output)
            cleaned_pairs.append((fname, df))
            if not args.quiet:
                print(f"[{i}/{len(files)}] Cleaned: {os.path.basename(fp)}  rows={len(df)}")
        except Exception as e:
            print(f"ERROR processing {fp}: {e}", file=sys.stderr)

    export_excel(cleaned_pairs, args.excel)

    elapsed = time.time() - start
    if not args.quiet:
        print(f"Done. Cleaned {len(cleaned_pairs)} files in {elapsed:.1f}s")
        print(f"Output CSVs: {os.path.abspath(args.output)}")
        if args.excel:
            print(f"Excel: {os.path.abspath(args.excel)}")


if __name__ == "__main__":
    main()