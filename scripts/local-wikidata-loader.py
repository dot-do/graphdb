#!/usr/bin/env python3
"""
Local Wikidata Loader using WikiDataSets

Downloads pre-processed Wikidata subsets (humans, films, companies, countries, animals)
from graphs.telecom-paris.fr and uploads to R2.

Usage:
    pip install wikidatasets
    python scripts/local-wikidata-loader.py [subset]

Subsets: humans, films, companies, countries, animals, all
"""

import os
import sys
import json
import subprocess
from pathlib import Path

# Try to import wikidatasets
try:
    from wikidatasets import WikidataSubset
except ImportError:
    print("Installing wikidatasets...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "wikidatasets", "-q"])
    from wikidatasets import WikidataSubset

TEMP_DIR = Path("/tmp/wikidata-loader")
R2_BUCKET = "graphdb-lakehouse-prod"
NAMESPACE = "https://wikidata.org/"
CHUNK_SIZE = 250_000

# ULID generation
ENCODING = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
import time
import random

def generate_ulid():
    now = int(time.time() * 1000)
    ulid = ""
    for _ in range(10):
        ulid = ENCODING[now % 32] + ulid
        now //= 32
    for _ in range(16):
        ulid += ENCODING[random.randint(0, 31)]
    return ulid[:26]

def encode_graphcol(triples, namespace):
    """Simple JSON-based GraphCol encoding"""
    data = {
        "version": 1,
        "namespace": namespace,
        "triples": triples
    }
    return json.dumps(data).encode()

def upload_to_r2(local_path, r2_path):
    """Upload file to R2 using wrangler"""
    subprocess.run([
        "npx", "wrangler", "r2", "object", "put",
        f"{R2_BUCKET}/{r2_path}",
        f"--file={local_path}",
        "--content-type=application/octet-stream"
    ], capture_output=True, cwd=str(Path(__file__).parent.parent))

def load_subset(subset_name: str):
    """Load a WikiDataSets subset"""
    print(f"\n📥 Loading Wikidata {subset_name} subset...")

    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    # Download subset
    print(f"  Downloading from graphs.telecom-paris.fr...")
    ds = WikidataSubset(subset_name, download_dir=str(TEMP_DIR))

    # Get edges (subject, relation, object triples)
    print(f"  Processing edges...")
    edges = ds.get_edges()

    # Get entity labels
    labels = ds.get_labels() if hasattr(ds, 'get_labels') else {}

    tx_id = generate_ulid()
    ts = int(time.time() * 1000)

    triples = []
    chunk_index = 0
    total_triples = 0
    total_bytes = 0

    def flush_chunk():
        nonlocal chunk_index, total_bytes, triples
        if not triples:
            return

        chunk_id = f"chunk_{chunk_index:06d}"
        chunk_path = f"datasets/wikidata/{subset_name}/chunks/{chunk_id}.graphcol"
        local_path = TEMP_DIR / f"{chunk_id}.graphcol"

        encoded = encode_graphcol(triples, NAMESPACE)
        local_path.write_bytes(encoded)
        total_bytes += len(encoded)

        print(f"  📤 Uploading {chunk_id}: {len(triples):,} triples, {len(encoded)/1024/1024:.2f}MB...")
        upload_to_r2(str(local_path), chunk_path)
        local_path.unlink()

        chunk_index += 1
        triples = []

    # Process edges
    for i, (s, r, o) in enumerate(edges):
        # Create entity IDs
        subj = f"https://wikidata.org/entity/Q{s}" if isinstance(s, int) else s
        obj = f"https://wikidata.org/entity/Q{o}" if isinstance(o, int) else o
        pred = f"P{r}" if isinstance(r, int) else r

        triples.append({
            "s": subj,
            "p": pred,
            "o": {"t": 10, "v": obj},  # REF type
            "ts": ts,
            "tx": tx_id
        })
        total_triples += 1

        if len(triples) >= CHUNK_SIZE:
            flush_chunk()

        if (i + 1) % 100_000 == 0:
            print(f"    📊 {i+1:,} edges processed")

    # Add labels as string triples
    for entity_id, label in labels.items() if labels else []:
        subj = f"https://wikidata.org/entity/Q{entity_id}" if isinstance(entity_id, int) else entity_id
        triples.append({
            "s": subj,
            "p": "label",
            "o": {"t": 5, "v": label},  # STRING type
            "ts": ts,
            "tx": tx_id
        })
        total_triples += 1

        if len(triples) >= CHUNK_SIZE:
            flush_chunk()

    # Final flush
    flush_chunk()

    # Upload manifest
    manifest = {
        "version": 1,
        "namespace": NAMESPACE,
        "dataset": f"wikidata/{subset_name}",
        "stats": {
            "totalTriples": total_triples,
            "totalChunks": chunk_index,
            "totalSizeBytes": total_bytes,
        },
        "createdAt": time.strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    manifest_path = TEMP_DIR / "index.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    upload_to_r2(str(manifest_path), f"datasets/wikidata/{subset_name}/index.json")
    manifest_path.unlink()

    print(f"\n✅ Completed {subset_name}:")
    print(f"   Triples: {total_triples:,}")
    print(f"   Chunks: {chunk_index}")
    print(f"   Size: {total_bytes/1024/1024:.2f}MB")

    return {"triples": total_triples, "chunks": chunk_index, "bytes": total_bytes}

def main():
    subset = sys.argv[1] if len(sys.argv) > 1 else "all"

    print("📚 Wikidata Local Loader (WikiDataSets)")
    print("=" * 40)

    subsets = ["humans", "films", "companies", "countries", "animals"]

    if subset == "all":
        results = {}
        for s in subsets:
            try:
                results[s] = load_subset(s)
            except Exception as e:
                print(f"  ❌ Failed to load {s}: {e}")

        print("\n📈 Summary")
        print("=" * 40)
        total = {"triples": 0, "chunks": 0, "bytes": 0}
        for name, stats in results.items():
            print(f"{name}: {stats['triples']:,} triples, {stats['chunks']} chunks")
            total["triples"] += stats["triples"]
            total["chunks"] += stats["chunks"]
            total["bytes"] += stats["bytes"]
        print("-" * 40)
        print(f"Total: {total['triples']:,} triples, {total['chunks']} chunks, {total['bytes']/1024/1024:.2f}MB")

    elif subset in subsets:
        load_subset(subset)

    else:
        print(f"Unknown subset: {subset}")
        print(f"Available: {', '.join(subsets)}, all")
        sys.exit(1)

if __name__ == "__main__":
    main()
