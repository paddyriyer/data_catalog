#!/usr/bin/env python3
"""Validate catalogue metadata output."""
import json, os, sys

BASE = os.path.join(os.path.dirname(__file__), "..", "data", "catalogue_metadata")
P, F = 0, 0

def check(name, cond, detail=""):
    global P, F
    if cond: P += 1; print(f"  ✅ {name}")
    else: F += 1; print(f"  ❌ {name} — {detail}")

def main():
    print("\n" + "="*50)
    print("  DATA CATALOGUE TESTS")
    print("="*50)
    
    # Master catalogue
    mc = json.load(open(os.path.join(BASE, "master_catalogue.json")))
    check("Master catalogue exists", len(mc) > 0)
    check("15 tables profiled", len(mc) == 15, f"Got {len(mc)}")
    check("All have quality scores", all("quality_score" in t for t in mc))
    check("All have columns", all(len(t["columns"]) > 0 for t in mc))
    check("All have lineage", all("lineage" in t for t in mc))
    check("All have tags", all(len(t["tags"]) > 0 for t in mc))
    
    # Quality report
    qr = json.load(open(os.path.join(BASE, "quality_report.json")))
    check("Quality report exists", "avg_quality_score" in qr)
    check("Avg quality >= 90", qr["avg_quality_score"] >= 90, f"Got {qr['avg_quality_score']}")
    check("Total rows = 103,443", qr["total_rows"] == 103443, f"Got {qr['total_rows']}")
    
    # Glossary
    gl = json.load(open(os.path.join(BASE, "business_glossary.json")))
    check("Glossary has terms", len(gl) > 0, f"Got {len(gl)}")
    check("Glossary >= 10 terms", len(gl) >= 10, f"Got {len(gl)}")
    
    # Lineage
    lm = json.load(open(os.path.join(BASE, "lineage_map.json")))
    check("Lineage map exists", len(lm) > 0)
    check("Lineage >= 10 tables", len(lm) >= 10, f"Got {len(lm)}")
    
    # Individual profiles
    profiles = [f for f in os.listdir(BASE) if f.endswith("_profile.json")]
    check("Individual profiles exist", len(profiles) >= 10, f"Got {len(profiles)}")
    
    # PII classification
    pii_count = sum(1 for t in mc for c in t["columns"] if c["pii_classification"] in ("PII","SPII"))
    check("PII/SPII columns detected", pii_count > 0, f"Got {pii_count}")
    
    print(f"\n{'='*50}")
    print(f"  {P} passed, {F} failed")
    print(f"{'='*50}\n")
    return 0 if F == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
