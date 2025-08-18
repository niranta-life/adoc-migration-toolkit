import csv
import json
import logging
import os
import re
import zipfile
from collections import defaultdict
from pathlib import Path
from ..shared import globals


TBL_REGEX = re.compile(r'([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)', re.IGNORECASE)


def _resolve_dir(category: str) -> Path:
    base = globals.GLOBAL_OUTPUT_DIR if globals.GLOBAL_OUTPUT_DIR else Path.cwd()
    return base / category


def _extract_policy_tables_from_zips(directory: Path) -> dict:
    """Scan ZIPs in directory and extract db.schema.table references from Custom SQL policies.

    Only inspects files named like data_quality_policy*.json inside the archives.
    Returns: dict[policy_name] -> set of full table refs (db.schema.table)
    """
    policies = defaultdict(set)
    if not directory.exists() or not directory.is_dir():
        return policies

    for fname in os.listdir(directory):
        if not fname.lower().endswith('.zip'):
            continue
        zip_path = directory / fname
        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                for member in z.namelist():
                    if member.startswith('data_quality_policy') and member.endswith('.json'):
                        with z.open(member) as f:
                            try:
                                j = json.load(f)
                            except Exception:
                                continue
                            # Normalize to list of dicts
                            records = [j] if isinstance(j, dict) else [r for r in j if isinstance(r, dict)]
                            for rec in records:
                                policy_name = str(rec.get('name', '')).strip()
                                if not policy_name:
                                    continue
                                csc = rec.get('customSqlConfig')
                                sqls = []
                                if isinstance(csc, dict):
                                    expr = csc.get('sqlExpression')
                                    if expr:
                                        sqls.append(expr)
                                elif isinstance(csc, list):
                                    for it in csc:
                                        if isinstance(it, dict) and it.get('sqlExpression'):
                                            sqls.append(it.get('sqlExpression'))
                                for sql in sqls:
                                    for db, schema, tbl in TBL_REGEX.findall(sql):
                                        policies[policy_name].add(f"{db}.{schema}.{tbl}")
        except zipfile.BadZipFile:
            continue
        except Exception:
            continue
    return policies


def _build_schema_table_map(full_table_set: set) -> dict:
    """Map 'schema.table' -> list of 'db.schema.table' for matching across DB names."""
    mapping = defaultdict(list)
    for full in full_table_set:
        parts = full.split('.')
        if len(parts) == 3:
            db, schema, tbl = parts
            mapping[f"{schema}.{tbl}"].append(full)
    return mapping


def check_for_custom_sql_required_before_migration(client, logger: logging.Logger, quiet_mode: bool = False, verbose_mode: bool = False):
    """Compare Custom SQL table references between exported and import-ready policy ZIPs.

    Reference logic adapted from pfizer_new_Scripts/diff/diff.py
    - Scans <output-dir>/policy-export and <output-dir>/policy-import for ZIPs
    - Extracts db.schema.table references from Custom SQL configs
    - Produces a CSV diff showing source tables, sink tables, mapping and unmapped
    Output: <output-dir>/policy-export/policy_table_diff_with_mappings.csv
    """
    source_dir = _resolve_dir('policy-export')
    sink_dir = _resolve_dir('policy-import')

    if not quiet_mode:
        print("\nScanning ZIPs for Custom SQL table references...")
        print(f"Source (policy-export): {source_dir}")
        print(f"Sink   (policy-import): {sink_dir}")

    src_tables = _extract_policy_tables_from_zips(source_dir)
    sink_tables = _extract_policy_tables_from_zips(sink_dir)

    all_policies = sorted(set(src_tables) | set(sink_tables))

    # Prepare output path
    out_dir = source_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / 'policy_table_diff_with_mappings.csv'

    with open(report_path, 'w', newline='', encoding='utf-8') as out:
        writer = csv.writer(out)
        writer.writerow([
            'Policy_Name',
            'Source_Tables',
            'Sink_Tables',
            'Source_to_Sink_Mapping',
            "Databases which don't have mapping",
        ])

        for policy in all_policies:
            source_set = src_tables.get(policy, set())
            sink_set = sink_tables.get(policy, set())

            # Sorted lists for output readability
            src_list = sorted(source_set)
            sink_list = sorted(sink_set)

            # Mapping: match on schema.table regardless of DB name
            src_schema_map = _build_schema_table_map(source_set)
            sink_schema_map = _build_schema_table_map(sink_set)
            mappings = []
            unmapped_dbs = set()

            for schema_table, src_full_list in src_schema_map.items():
                if schema_table in sink_schema_map:
                    sink_full_list = sink_schema_map[schema_table]
                    for src_full in src_full_list:
                        mappings.append(f"{src_full} -> {', '.join(sink_full_list)}")
                else:
                    # No match for this schema.table, mark as unmapped
                    for src_full in src_full_list:
                        mappings.append(f"{src_full} -> No match")
                        unmapped_dbs.add(src_full)

            writer.writerow([
                policy,
                ", ".join(src_list),
                ", ".join(sink_list),
                " | ".join(mappings) if mappings else "",
                ", ".join(sorted(unmapped_dbs)) if unmapped_dbs else "",
            ])

    if not quiet_mode:
        print(f"✅ Done! Output saved to {report_path}")

    return str(report_path)


