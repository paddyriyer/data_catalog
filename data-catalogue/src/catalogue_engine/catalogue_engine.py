#!/usr/bin/env python3
"""
Data Catalogue Engine — Horizon Bank Holdings
===============================================
Auto-profiles all tables in the MDM Lakehouse, generating:
- Column-level metadata (type, nullability, cardinality, min/max, samples)
- Table-level statistics (row count, size, freshness)
- PII classification (PII, SPII, Confidential, Public)
- Data quality scores per column and table
- Lineage mappings (Bronze → Silver → MDM → Gold)
- Business glossary terms
- Column-level distribution profiles

Usage: python catalogue_engine.py --data-dir ../../data
"""
import csv, os, json, sys, hashlib, re, argparse, math
from datetime import datetime
from collections import Counter, defaultdict

# ─── PII Classification Rules ───
PII_PATTERNS = {
    "PII": {
        "columns": ["first_name","last_name","email","phone","address_line1","date_of_birth",
                     "CUST_NAME","ADDR1","EMAIL","PHONE","PersonEmail","Phone","MailingStreet",
                     "FULL_NAME","EMAIL_ADDR","PHONE_NUM","STREET_ADDR","FirstName","LastName"],
        "patterns": [r"email", r"phone", r"addr", r"name", r"birth", r"dob"],
    },
    "SPII": {
        "columns": ["ssn_hash","fico_score","annual_income","FICO","CREDIT_SCORE","risk_tier",
                     "Annual_Revenue__c","RISK_RATING","probability_of_default","loss_given_default"],
        "patterns": [r"ssn", r"fico", r"income", r"credit_score", r"risk"],
    },
    "CONFIDENTIAL": {
        "columns": ["account_number","account_id","customer_id","balance","credit_limit","apr",
                     "amount","loss_amount","risk_score","CIF_NUM","AccountId","PARTY_ID",
                     "composite_score","expected_loss"],
        "patterns": [r"account", r"balance", r"limit", r"amount", r"score"],
    },
}

# ─── Business Glossary ───
GLOSSARY = {
    "customer_id": {"term": "Customer Identifier", "definition": "Unique golden record identifier for a customer entity, MDM-assigned after deduplication across source systems.", "domain": "MDM", "steward": "Data Governance Team"},
    "fico_score": {"term": "FICO Score", "definition": "Fair Isaac Corporation credit score (300-850) indicating creditworthiness. Sourced from Core Banking as authoritative system.", "domain": "Credit Risk", "steward": "Risk Analytics"},
    "segment": {"term": "Customer Segment", "definition": "Wealth-based segmentation: mass_market (<$75K), mass_affluent ($75K-$150K), affluent ($150K-$300K), high_net_worth ($300K-$750K), ultra_hnw (>$750K).", "domain": "Marketing", "steward": "Customer Analytics"},
    "risk_tier": {"term": "Credit Risk Tier", "definition": "Risk classification based on FICO: super_prime (750+), prime (700-749), near_prime (650-699), subprime (580-649), deep_subprime (<580).", "domain": "Credit Risk", "steward": "Risk Analytics"},
    "balance": {"term": "Account Balance", "definition": "Current outstanding balance on the account. For credit cards: amount owed. For loans: remaining principal. For deposits: available funds.", "domain": "Finance", "steward": "Finance Operations"},
    "transaction_id": {"term": "Transaction Identifier", "definition": "Unique identifier for each financial transaction. Immutable once created.", "domain": "Payments", "steward": "Payment Operations"},
    "mcc_code": {"term": "Merchant Category Code", "definition": "ISO 18245 four-digit code classifying the merchant's business type for card transactions.", "domain": "Payments", "steward": "Merchant Services"},
    "days_past_due": {"term": "Days Past Due (DPD)", "definition": "Number of days a payment is overdue. Key delinquency indicator: 0=current, 30/60/90/120+ trigger escalating actions.", "domain": "Collections", "steward": "Collections Team"},
    "probability_of_default": {"term": "Probability of Default (PD)", "definition": "Statistical likelihood (0-1) that a borrower will default within 12 months. Basel II/III regulatory metric.", "domain": "Credit Risk", "steward": "Risk Analytics"},
    "composite_score": {"term": "MDM Composite Match Score", "definition": "Weighted similarity score (0-1) across name, email, phone, address, and cross-system dimensions. ≥0.92=auto_merge, 0.75-0.92=review, <0.75=no_match.", "domain": "MDM", "steward": "Data Governance Team"},
    "rewards_earned": {"term": "Rewards Earned", "definition": "Dollar value of rewards points/cashback earned on a transaction, calculated by product-specific reward rates.", "domain": "Loyalty", "steward": "Loyalty Program"},
    "acquisition_channel": {"term": "Acquisition Channel", "definition": "Marketing channel through which the customer was originally acquired: branch, web, mobile_app, phone, mail, partner_referral, social_media.", "domain": "Marketing", "steward": "Customer Analytics"},
    "digital_enrolled": {"term": "Digital Banking Enrollment", "definition": "Boolean flag indicating whether the customer has activated digital banking (web or mobile app access).", "domain": "Digital", "steward": "Digital Banking"},
    "fraud_flag": {"term": "Fraud Flag", "definition": "Boolean indicator set by the real-time fraud detection engine when a transaction triggers ML model or rules-based alerts.", "domain": "Fraud", "steward": "Fraud Operations"},
    "alert_type": {"term": "Fraud Alert Type", "definition": "Classification of the fraud/AML alert: velocity_spike, geographic_anomaly, large_purchase, account_takeover_attempt, structuring_pattern, etc.", "domain": "Fraud", "steward": "Fraud Operations"},
    "partner_id": {"term": "Partner Identifier", "definition": "Unique identifier for co-brand partners, merchant networks, and digital partners in the rewards ecosystem.", "domain": "Partnerships", "steward": "Partnership Team"},
    "interchange_revenue": {"term": "Interchange Revenue", "definition": "Fee earned per card transaction, paid by the merchant's bank to Horizon Bank. Typically 1.5-3.5% of transaction value.", "domain": "Finance", "steward": "Finance Operations"},
}

# ─── Lineage Definitions ───
LINEAGE = {
    "dim_customer": {
        "layer": "gold",
        "upstream": [
            {"table": "core_banking_customers", "layer": "bronze", "join": "SSN_HASH → ssn_hash", "transform": "Uppercase name split, phone normalize"},
            {"table": "salesforce_accounts", "layer": "bronze", "join": "PersonEmail → email", "transform": "CRM fields mapped"},
            {"table": "fiserv_parties", "layer": "bronze", "join": "EMAIL_ADDR → email", "transform": "Name parsing, risk mapping"},
            {"table": "mdm_match_pairs", "layer": "mdm", "join": "customer_id_1/2 → customer_id", "transform": "Survivorship rules applied"},
        ],
        "downstream": ["dim_account", "fact_transactions", "fact_loan_payments", "fact_credit_risk", "digital_events", "fraud_alerts"],
        "refresh": "Every 4 hours (CDC from Core Banking)",
        "sla": "< 4 hours from source change",
    },
    "dim_account": {
        "layer": "gold",
        "upstream": [{"table": "dim_customer", "layer": "gold", "join": "customer_id", "transform": "FK reference"},
                     {"table": "dim_product", "layer": "gold", "join": "product_id", "transform": "Product enrichment"}],
        "downstream": ["fact_transactions", "fact_loan_payments", "fact_credit_risk"],
        "refresh": "Every 4 hours",
        "sla": "< 4 hours",
    },
    "fact_transactions": {
        "layer": "gold",
        "upstream": [{"table": "dim_account", "layer": "gold", "join": "account_id", "transform": "Account enrichment"},
                     {"table": "dim_customer", "layer": "gold", "join": "customer_id", "transform": "Customer enrichment"}],
        "downstream": ["fraud_alerts", "partner_performance"],
        "refresh": "Near real-time (streaming)",
        "sla": "< 15 minutes",
    },
    "fact_credit_risk": {
        "layer": "gold",
        "upstream": [{"table": "dim_customer", "layer": "gold", "join": "customer_id", "transform": "FICO, risk tier"},
                     {"table": "dim_account", "layer": "gold", "join": "account_id (aggregated)", "transform": "Balance aggregation"}],
        "downstream": [],
        "refresh": "Daily snapshot",
        "sla": "< 6 hours (overnight batch)",
    },
    "fraud_alerts": {
        "layer": "gold",
        "upstream": [{"table": "fact_transactions", "layer": "gold", "join": "transaction_id", "transform": "ML model scoring"},
                     {"table": "dim_account", "layer": "gold", "join": "account_id", "transform": "Account context"}],
        "downstream": [],
        "refresh": "Real-time (event-driven)",
        "sla": "< 500ms from transaction",
    },
    "digital_events": {
        "layer": "clickstream",
        "upstream": [{"table": "dim_customer", "layer": "gold", "join": "customer_id", "transform": "Session attribution"}],
        "downstream": [],
        "refresh": "Streaming (Kinesis)",
        "sla": "< 5 minutes",
    },
    "core_banking_customers": {
        "layer": "bronze",
        "upstream": [{"table": "Oracle Core Banking DB", "layer": "source", "join": "JDBC CDC", "transform": "None (raw extract)"}],
        "downstream": ["dim_customer"],
        "refresh": "Every 4 hours (incremental CDC)",
        "sla": "< 30 minutes extraction",
    },
    "salesforce_accounts": {
        "layer": "bronze",
        "upstream": [{"table": "Salesforce CRM", "layer": "source", "join": "Bulk API v2", "transform": "None (raw extract)"}],
        "downstream": ["dim_customer"],
        "refresh": "Every 2 hours + real-time CDC",
        "sla": "< 15 minutes",
    },
    "fiserv_parties": {
        "layer": "bronze",
        "upstream": [{"table": "Fiserv SFTP", "layer": "source", "join": "File drop", "transform": "None (raw CSV)"}],
        "downstream": ["dim_customer"],
        "refresh": "Daily at 02:00 UTC",
        "sla": "< 1 hour after file drop",
    },
    "mdm_match_pairs": {
        "layer": "mdm",
        "upstream": [{"table": "core_banking_customers", "layer": "bronze", "join": "Fuzzy match", "transform": "Jaro-Winkler scoring"},
                     {"table": "salesforce_accounts", "layer": "bronze", "join": "Fuzzy match", "transform": "Jaro-Winkler scoring"},
                     {"table": "fiserv_parties", "layer": "bronze", "join": "Fuzzy match", "transform": "Jaro-Winkler scoring"}],
        "downstream": ["dim_customer"],
        "refresh": "After Silver refresh",
        "sla": "< 1 hour after Silver completes",
    },
}

# ─── Profiling Engine ───
def classify_pii(col_name):
    """Classify column PII level."""
    cn = col_name.lower()
    for level, rules in PII_PATTERNS.items():
        if col_name in rules["columns"]:
            return level
        for pat in rules["patterns"]:
            if re.search(pat, cn):
                return level
    return "PUBLIC"

def infer_type(values):
    """Infer semantic data type from sample values."""
    non_empty = [v for v in values if v and v.strip()]
    if not non_empty:
        return "unknown"
    
    # Check boolean
    bools = {"true","false","True","False","TRUE","FALSE","1","0","yes","no"}
    if all(v in bools for v in non_empty[:50]):
        return "boolean"
    
    # Check date patterns
    date_pats = [r"^\d{4}-\d{2}-\d{2}$", r"^\d{4}-\d{2}-\d{2}T"]
    for pat in date_pats:
        if all(re.match(pat, v) for v in non_empty[:20]):
            return "datetime" if "T" in non_empty[0] else "date"
    
    # Check numeric
    try:
        nums = [float(v) for v in non_empty[:100]]
        if all(float(v) == int(float(v)) for v in non_empty[:100] if v):
            return "integer"
        return "decimal"
    except (ValueError, OverflowError):
        pass
    
    # Check if looks like ID (by value patterns)
    if all(re.match(r'^[A-Z]{2,5}[-_]\d+', v) for v in non_empty[:10]):
        return "identifier"
    
    # Check email
    if any("@" in v for v in non_empty[:10]):
        return "email"
    
    # Check phone
    if any(v.startswith("+") for v in non_empty[:10]):
        return "phone"
    
    return "string"

def profile_column(col_name, values, total_rows):
    """Generate full column profile."""
    non_empty = [v for v in values if v and v.strip()]
    null_count = total_rows - len(non_empty)
    unique_vals = set(non_empty)
    cardinality = len(unique_vals)
    
    dtype = infer_type(values)
    pii = classify_pii(col_name)
    
    profile = {
        "column_name": col_name,
        "data_type": dtype,
        "pii_classification": pii,
        "total_count": total_rows,
        "null_count": null_count,
        "null_rate": round(null_count / total_rows * 100, 2) if total_rows > 0 else 0,
        "distinct_count": cardinality,
        "cardinality_ratio": round(cardinality / total_rows * 100, 2) if total_rows > 0 else 0,
        "is_unique": cardinality == len(non_empty),
    }
    
    # Numeric stats
    if dtype in ("integer", "decimal"):
        try:
            nums = [float(v) for v in non_empty]
            profile["min"] = min(nums)
            profile["max"] = max(nums)
            profile["mean"] = round(sum(nums) / len(nums), 2)
            profile["median"] = sorted(nums)[len(nums)//2]
            variance = sum((x - profile["mean"])**2 for x in nums) / len(nums)
            profile["std_dev"] = round(math.sqrt(variance), 2)
        except:
            pass
    
    # Top values (for non-PII)
    if pii == "PUBLIC" or dtype in ("boolean",):
        counter = Counter(non_empty)
        profile["top_values"] = [{"value": v, "count": c, "pct": round(c/len(non_empty)*100,1)} for v, c in counter.most_common(8)]
    elif pii in ("PII", "SPII"):
        profile["top_values"] = [{"value": "***MASKED***", "count": len(non_empty), "pct": 100}]
    
    # Sample values (masked for PII)
    if pii in ("PII", "SPII"):
        profile["sample_values"] = ["***MASKED***"] * min(3, len(non_empty))
    else:
        profile["sample_values"] = list(unique_vals)[:5]
    
    # String length stats
    if dtype in ("string", "email", "phone", "identifier"):
        lengths = [len(v) for v in non_empty]
        if lengths:
            profile["min_length"] = min(lengths)
            profile["max_length"] = max(lengths)
            profile["avg_length"] = round(sum(lengths) / len(lengths), 1)
    
    # Quality score (0-100)
    qs = 100
    if null_count > 0:
        qs -= min(null_count / total_rows * 50, 25)  # Penalty for nulls
    if dtype == "unknown":
        qs -= 10
    profile["quality_score"] = round(qs, 1)
    
    # Glossary lookup
    if col_name in GLOSSARY:
        profile["glossary"] = GLOSSARY[col_name]
    
    return profile

def profile_table(table_name, file_path, layer):
    """Profile an entire table."""
    with open(file_path, "r") as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames
        rows = list(reader)
    
    total_rows = len(rows)
    file_size = os.path.getsize(file_path)
    
    # Profile each column
    col_profiles = []
    for col in columns:
        values = [row.get(col, "") for row in rows]
        col_profiles.append(profile_column(col, values, total_rows))
    
    # Table-level quality score
    avg_quality = sum(cp["quality_score"] for cp in col_profiles) / len(col_profiles) if col_profiles else 0
    
    # PII summary
    pii_summary = Counter(cp["pii_classification"] for cp in col_profiles)
    
    # Lineage
    lineage_info = LINEAGE.get(table_name, {})
    
    table_profile = {
        "table_name": table_name,
        "layer": layer,
        "file_path": file_path,
        "total_rows": total_rows,
        "total_columns": len(columns),
        "file_size_bytes": file_size,
        "file_size_human": f"{file_size/1024:.1f} KB" if file_size < 1048576 else f"{file_size/1048576:.1f} MB",
        "profiled_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "quality_score": round(avg_quality, 1),
        "pii_summary": dict(pii_summary),
        "columns": col_profiles,
        "lineage": lineage_info,
        "owner": lineage_info.get("steward", "Data Engineering"),
        "refresh_frequency": lineage_info.get("refresh", "Unknown"),
        "sla": lineage_info.get("sla", "Unknown"),
        "tags": _auto_tag(table_name, layer, col_profiles),
    }
    
    return table_profile

def _auto_tag(table_name, layer, cols):
    """Auto-generate tags based on content."""
    tags = [layer]
    if any(cp["pii_classification"] in ("PII","SPII") for cp in cols):
        tags.append("contains_pii")
    if "customer" in table_name:
        tags.extend(["customer", "entity"])
    if "transaction" in table_name or "payment" in table_name:
        tags.extend(["financial", "transactional"])
    if "fraud" in table_name:
        tags.extend(["fraud", "compliance", "aml"])
    if "risk" in table_name:
        tags.extend(["risk", "regulatory"])
    if table_name.startswith("dim_"):
        tags.append("dimension")
    if table_name.startswith("fact_"):
        tags.append("fact")
    if "partner" in table_name:
        tags.append("partnership")
    if "digital" in table_name or "click" in table_name:
        tags.append("digital")
    if "mdm" in table_name or "match" in table_name:
        tags.extend(["mdm", "data_quality"])
    return list(set(tags))

def generate_quality_report(all_profiles):
    """Generate aggregate quality report."""
    total_tables = len(all_profiles)
    total_rows = sum(p["total_rows"] for p in all_profiles)
    total_cols = sum(p["total_columns"] for p in all_profiles)
    total_size = sum(p["file_size_bytes"] for p in all_profiles)
    avg_quality = sum(p["quality_score"] for p in all_profiles) / total_tables
    
    pii_agg = Counter()
    for p in all_profiles:
        pii_agg.update(p["pii_summary"])
    
    by_layer = defaultdict(list)
    for p in all_profiles:
        by_layer[p["layer"]].append(p)
    
    layer_stats = {}
    for layer, profiles in by_layer.items():
        layer_stats[layer] = {
            "tables": len(profiles),
            "rows": sum(p["total_rows"] for p in profiles),
            "avg_quality": round(sum(p["quality_score"] for p in profiles) / len(profiles), 1),
        }
    
    return {
        "report_date": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "company": "Horizon Bank Holdings",
        "total_tables": total_tables,
        "total_rows": total_rows,
        "total_columns": total_cols,
        "total_size_bytes": total_size,
        "total_size_human": f"{total_size/1048576:.1f} MB",
        "avg_quality_score": round(avg_quality, 1),
        "pii_column_distribution": dict(pii_agg),
        "layer_statistics": layer_stats,
        "tables_by_quality": sorted([{"table": p["table_name"], "score": p["quality_score"], "layer": p["layer"]} for p in all_profiles], key=lambda x: x["score"]),
    }

def generate_glossary_export(all_profiles):
    """Export all glossary terms found across tables."""
    terms = {}
    for p in all_profiles:
        for col in p["columns"]:
            if "glossary" in col:
                key = col["column_name"]
                if key not in terms:
                    terms[key] = {
                        **col["glossary"],
                        "found_in": [],
                        "pii_classification": col["pii_classification"],
                    }
                terms[key]["found_in"].append(p["table_name"])
    return dict(sorted(terms.items()))

# ─── Main ───
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=os.path.join(os.path.dirname(__file__), "..", "..", "data"))
    args = parser.parse_args()
    
    data_dir = os.path.abspath(args.data_dir)
    out_dir = os.path.join(os.path.dirname(__file__), "..", "..", "data", "catalogue_metadata")
    os.makedirs(out_dir, exist_ok=True)
    
    print(f"\n{'='*60}")
    print(f"  DATA CATALOGUE ENGINE — HORIZON BANK HOLDINGS")
    print(f"{'='*60}\n")
    
    # Discover tables
    table_map = {
        "bronze": ["core_banking_customers", "salesforce_accounts", "fiserv_parties"],
        "gold": ["dim_customer", "dim_account", "dim_product", "dim_date", "fact_transactions", "fact_loan_payments", "fact_credit_risk"],
        "clickstream": ["digital_events"],
        "fraud": ["fraud_alerts"],
        "partners": ["partner_performance"],
        "realtime": ["hourly_metrics"],
        "mdm": ["mdm_match_pairs"],
    }
    
    all_profiles = []
    
    for layer, tables in table_map.items():
        for table_name in tables:
            fname = f"{table_name}.csv"
            fpath = os.path.join(data_dir, layer, fname)
            if not os.path.exists(fpath):
                print(f"  ⚠ Missing: {fpath}")
                continue
            
            print(f"  Profiling {table_name} ({layer})...")
            profile = profile_table(table_name, fpath, layer)
            all_profiles.append(profile)
            
            # Write individual table profile
            with open(os.path.join(out_dir, f"{table_name}_profile.json"), "w") as f:
                json.dump(profile, f, indent=2, default=str)
    
    # Write master catalogue
    with open(os.path.join(out_dir, "master_catalogue.json"), "w") as f:
        json.dump(all_profiles, f, indent=2, default=str)
    
    # Quality report
    report = generate_quality_report(all_profiles)
    with open(os.path.join(out_dir, "quality_report.json"), "w") as f:
        json.dump(report, f, indent=2, default=str)
    
    # Glossary
    glossary = generate_glossary_export(all_profiles)
    with open(os.path.join(out_dir, "business_glossary.json"), "w") as f:
        json.dump(glossary, f, indent=2, default=str)
    
    # Lineage export
    with open(os.path.join(out_dir, "lineage_map.json"), "w") as f:
        json.dump(LINEAGE, f, indent=2, default=str)
    
    # Summary
    print(f"\n{'='*60}")
    print(f"  CATALOGUE COMPLETE")
    print(f"  Tables profiled: {len(all_profiles)}")
    print(f"  Total rows: {report['total_rows']:,}")
    print(f"  Total columns: {report['total_columns']}")
    print(f"  Avg quality score: {report['avg_quality_score']}%")
    print(f"  Glossary terms: {len(glossary)}")
    print(f"  Output: {out_dir}")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
