"""
Download IEEE-CIS Fraud Detection and PaySim datasets.
Strategy: Kaggle API first (if creds available), HuggingFace Hub fallback.
Idempotent: skips files already present.
"""

import hashlib
import json
import os
import sys
import zipfile
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data" / "raw"
CHECKSUMS_FILE = BASE_DIR / "data" / "schemas" / "download_checksums.json"
IEEE_CIS_DIR = DATA_DIR / "ieee_cis"
PAYSIM_DIR = DATA_DIR / "paysim"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def count_csv_rows(path: Path) -> int:
    count = 0
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for _ in f:
            count += 1
    return max(count - 1, 0)


def download_ieee_cis_kaggle() -> bool:
    # Kaggle CLI ≥2.x prefers KAGGLE_API_TOKEN; fall back to username+key pair
    api_token = os.environ.get("KAGGLE_API_TOKEN")
    username = os.environ.get("KAGGLE_USERNAME")
    key = os.environ.get("KAGGLE_KEY")
    if not api_token and not (username and key):
        print("No Kaggle credentials — skipping Kaggle download")
        return False
    env = dict(os.environ)
    if api_token:
        env["KAGGLE_API_TOKEN"] = api_token
    else:
        env.update({"KAGGLE_USERNAME": username, "KAGGLE_KEY": key,
                    "KAGGLE_API_TOKEN": key})
    try:
        import subprocess
        IEEE_CIS_DIR.mkdir(parents=True, exist_ok=True)
        result = subprocess.run(
            ["kaggle", "competitions", "download", "-c", "ieee-fraud-detection", "-p", str(IEEE_CIS_DIR)],
            env=env, capture_output=True, text=True,
        )
        if result.returncode != 0:
            print(f"Kaggle download failed: {result.stderr.strip()}")
            print("  → Accept competition rules at https://www.kaggle.com/competitions/ieee-fraud-detection/data")
            return False
        for zip_file in IEEE_CIS_DIR.glob("*.zip"):
            with zipfile.ZipFile(zip_file, "r") as z:
                z.extractall(IEEE_CIS_DIR)
            zip_file.unlink()
        print("IEEE-CIS downloaded from Kaggle")
        return True
    except Exception as e:
        print(f"Kaggle error: {e}")
        return False


def download_ieee_cis_huggingface() -> bool:
    try:
        from huggingface_hub import hf_hub_download
        IEEE_CIS_DIR.mkdir(parents=True, exist_ok=True)
        token = os.environ.get("HF_TOKEN")
        for filename in ["train_transaction.csv", "train_identity.csv"]:
            dest = IEEE_CIS_DIR / filename
            if dest.exists():
                print(f"Skipping {filename} — already present")
                continue
            # These repos mirror the competition CSVs; update if they go offline
            repos = [
                "vbinh/ieee-cis-fraud-detection",
                "daishen/ieee-cis-fraud-detection",
            ]
            for repo_id in repos:
                try:
                    hf_hub_download(
                        repo_id=repo_id, filename=filename,
                        repo_type="dataset", local_dir=str(IEEE_CIS_DIR), token=token,
                    )
                    print(f"{filename} downloaded from {repo_id}")
                    break
                except Exception as e:
                    print(f"Failed {repo_id}/{filename}: {e}")
        return (IEEE_CIS_DIR / "train_transaction.csv").exists()
    except ImportError:
        print("huggingface_hub not installed. Run: pip install huggingface-hub")
        return False
    except Exception as e:
        print(f"HuggingFace error: {e}")
        return False


def download_paysim() -> bool:
    try:
        from huggingface_hub import hf_hub_download, list_repo_files
        PAYSIM_DIR.mkdir(parents=True, exist_ok=True)
        dest = PAYSIM_DIR / "PS_log.csv"
        if dest.exists():
            print("Skipping PaySim — already present")
            return True
        token = os.environ.get("HF_TOKEN")
        repos = [
            ("theman10/paysim", "paysim.csv"),  # confirmed working
            ("ealtman2019/ibm-transactions-for-anti-money-laundering-aml", None),
        ]
        for repo_id, filename in repos:
            try:
                if filename is None:
                    files = list(list_repo_files(repo_id, repo_type="dataset", token=token))
                    csv_files = [f for f in files if f.endswith(".csv")]
                    if not csv_files:
                        continue
                    filename = csv_files[0]
                downloaded = hf_hub_download(
                    repo_id=repo_id, filename=filename,
                    repo_type="dataset", local_dir=str(PAYSIM_DIR), token=token,
                )
                import shutil
                shutil.copy(downloaded, dest)
                print(f"PaySim downloaded from {repo_id}")
                return True
            except Exception as e:
                print(f"Failed {repo_id}: {e}")
        print("Could not download PaySim from any source")
        return False
    except ImportError:
        print("huggingface_hub not installed")
        return False


def validate_and_update_checksums():
    manifest = {}
    if CHECKSUMS_FILE.exists():
        with open(CHECKSUMS_FILE) as f:
            manifest = json.load(f)

    files = [
        (IEEE_CIS_DIR / "train_transaction.csv", "ieee_cis/train_transaction.csv"),
        (IEEE_CIS_DIR / "train_identity.csv", "ieee_cis/train_identity.csv"),
        (PAYSIM_DIR / "PS_log.csv", "paysim/PS_log.csv"),
    ]

    print("\nValidating downloads:")
    for path, key in files:
        if not path.exists():
            print(f"  MISSING: {key}")
            continue
        size_mb = path.stat().st_size / (1024 * 1024)
        sha = sha256_file(path)
        rows = count_csv_rows(path)
        manifest[key] = {"sha256": sha, "rows": rows, "size_mb": round(size_mb, 1), "source": "downloaded"}
        print(f"  OK: {key} — {rows:,} rows, {size_mb:.1f} MB")

    with open(CHECKSUMS_FILE, "w") as f:
        json.dump(manifest, f, indent=2)


def main():
    print("Sentinel Fraud Platform — Dataset Downloader\n")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not (IEEE_CIS_DIR / "train_transaction.csv").exists():
        if not download_ieee_cis_kaggle():
            download_ieee_cis_huggingface()
    else:
        print("IEEE-CIS already downloaded")

    download_paysim()
    validate_and_update_checksums()
    print("\nDone. Run 'make phase-03' to begin ingestion.")


if __name__ == "__main__":
    main()
