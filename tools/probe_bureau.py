#!/usr/bin/env python3
"""Какие продукты есть за день и сколько жалоб по каждому (чтобы проверить имена для контура бюро). В базу не пишет.
  python3 tools/probe_bureau.py --ds 2026-08-17
"""
import argparse, pathlib, sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "ingestion"))
import cfpb_http, load_cfpb


def find_buckets(obj, path=""):
    if isinstance(obj, dict):
        if isinstance(obj.get("buckets"), list):
            yield path, obj["buckets"]
        for k, v in obj.items():
            yield from find_buckets(v, path + "/" + k)


ap = argparse.ArgumentParser()
ap.add_argument("--ds", required=True)
a = ap.parse_args()
r = cfpb_http.get(load_cfpb.BASE, {"size": 0, "date_received_min": a.ds, "date_received_max": a.ds}, 30, 3)
data = r.json()
print("всего за день:", data["hits"]["total"]["value"])
for path, buckets in find_buckets(data.get("aggregations", {})):
    if "product" in path.lower() and "sub" not in path.lower():
        print(path)
        for b in buckets:
            print(f"  {b.get('key')!r}: {b.get('doc_count')}")
print("\nСписок BUREAU_PRODUCTS в ingestion/bureau_pipeline.py должен совпадать с нужными ключами ровно.")
