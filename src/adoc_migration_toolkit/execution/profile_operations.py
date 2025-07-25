import csv
import logging
import time
from pathlib import Path
from ..shared import globals
import requests

from .utils import get_source_to_target_asset_id_map

BATCH_SIZE = 3               # Number of assets to run profiling per batch
POLL_INTERVAL = 5            # Seconds to wait between polling
MAX_RETRIES = 3             # Number of polling attempts before timeout



# === PAYLOAD TO TRIGGER PROFILING ===
trigger_payload = {
    "data": {
        "profilingType": "FULL"
    }
}

def trigger_profiling(client, asset_id):
    """Send POST request to start profiling, then poll for status."""
    try:
        print("Trigger profiling for asset {}".format(asset_id))
        response = client.make_api_call(
            endpoint=f"/catalog-server/api/assets/{asset_id}/profile",
            method='POST',
            json_payload=trigger_payload,
            use_target_auth=True,
            use_target_tenant=True,
            dont_parse_reponse=True,
        )

        if response.status_code == 200:
            print(f"▶️ Triggered profiling for asset {asset_id}")
            # Poll for status up to 5 times, 5 seconds each
            if is_profiling_successful(client, asset_id):
                return True
            else:
                print(f"❌ Profiling did not complete successfully for asset {asset_id} after triggering.")
                return False
        else:
            print(f"❌ Failed to trigger profiling for asset {asset_id} | Status: {response.status_code}")
            print(f"Response: {response.text}")
            return False
    except Exception as e:
        print(f"🔥 Error triggering profiling for asset {asset_id}: {e}")
        return False

def is_profiling_successful(client, asset_id):
    """Poll the profiling status until SUCCESS or give up after max retries."""
    attempts = 0
    max_attempts = 5
    poll_interval = 5  # seconds
    while attempts < max_attempts:
        try:
            print(f"Check profiling status for asset {asset_id} (attempt {attempts+1}/{max_attempts})")
            response = client.make_api_call(
                endpoint=f"/catalog-server/api/assets/{asset_id}/profile",
                method='GET',
                use_target_auth=True,
                use_target_tenant=True,
                dont_parse_reponse=True,
            )

            if response.status_code == 200:
                result = response.json()
                status = result.get("data", {}).get("status", "").upper()
                if status == "SUCCESS":
                    print(f"✅ Profiling SUCCESSFUL for asset {asset_id}")
                    return True
                elif status in ["FAILED", "CANCELLED", "ERROR"]:
                    print(f"❌ Profiling FAILED for asset {asset_id} | Status: {status}")
                    return False
                elif status in ["RUNNING", "INPROGRESS", "IN PROGRESS"]:
                    print(f"⏳ Profiling in progress for asset {asset_id} (status: {status})")
                else:
                    print(f"⚠️ Unknown profiling status '{status}' for asset {asset_id}")
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                print(f"⚠️ Asset {asset_id} not found (404)")
                return False
            print(f"⚠️ Exception while polling profiling status for asset {asset_id}: {e}")
        except Exception as e:
            print(f"⚠️ Exception while polling profiling status for asset {asset_id}: {e}")

        attempts += 1
        time.sleep(poll_interval)

    print(f"⏰ Timeout waiting for profiling on asset {asset_id}")
    return False

def process_batch(client, asset_id):
    print(f"Processing asset {asset_id}")
    if is_profiling_successful(client, asset_id):
        return True
    else:
        return trigger_profiling(client, asset_id)


def trigger_profile_action(client, logger: logging.Logger, profile_assets_config_csv_path, quiet_mode: bool = False, verbose_mode: bool = False):
    """Trigger profiling action."""
    print("Running Profiling Action")
    asset_ids = []
    asset_id_name_map = {}
    with open(profile_assets_config_csv_path, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            asset_id = row.get("assetId", "").strip()
            asset_name = row.get("assetUid", "UNKNOWN").strip()
            if asset_id.isdigit():
                # Convert asset_id to int for consistency
                asset_ids.append(asset_id)
                asset_id_name_map[asset_id] = asset_name

    failed_assets = []
    for assetId in asset_ids:
        try:
            if not process_batch(client, assetId):
                failed_assets.append(assetId)
        except Exception as e:
            print(f"Error processing asset {assetId}: {e}")
            failed_assets.append(assetId)
            continue

    print(f"\n🏁 Profiling completed, follwoing assets failed to trigger profiling: {failed_assets}")


def check_for_profiling_required_before_migration(client, logger: logging.Logger, policy_types: str, run_profile: bool = False, quiet_mode: bool = False, verbose_mode: bool = False):
    if globals.GLOBAL_OUTPUT_DIR:
        input_file = globals.GLOBAL_OUTPUT_DIR / "policy-export" / "policies-all-export.csv"
    else:
        input_file = "policies-all-export.csv"

    if not input_file.exists():
        logger.error(f"Input file {input_file} does not exist, Please run 'policy-xfr' first to generate the policies-all-export.csv file")
        print(f"   Expected location: {globals.GLOBAL_OUTPUT_DIR}/policy-export/policies-all-export.csv")
        return None
    # Read CSV data
    asset_data = []
    # Support comma-separated policy types
    policy_types_list = [ptype.strip() for ptype in policy_types.split(',')] if policy_types else []
    print(f"Policy types: {policy_types_list}")
    with open(input_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip header
        for row in reader:
            if len(row) >= 7:
                if str(row[1]).strip() in policy_types_list:
                    table_asset_id = str(row[3])
                    asset_data.append(table_asset_id)


    if not asset_data:
        print("❌ No valid asset data found in CSV file...")
        logger.warning("No valid asset data found in CSV file...")
        return None
    print(f"Asset data: {asset_data}")
    assets_mapped_csv_file = str(globals.GLOBAL_OUTPUT_DIR / "asset-import" / "asset-merged-all.csv")
    assets_mapping = get_source_to_target_asset_id_map(assets_mapped_csv_file, logger)
    un_profiled_assets = []
    for each_asset_id in asset_data:
        if each_asset_id not in assets_mapping:
            print(f"Source Asset {each_asset_id} not found in target assets_mapping, skipping...")
            continue
        target_info = assets_mapping[each_asset_id]
        target_table_asset = int(target_info["target_id"])
        try:
            count_response = client.make_api_call(
                    endpoint=f"/catalog-server/api/assets/{target_table_asset}/profiles",
                    method='GET',
                    use_target_auth=True,
                    use_target_tenant=True,
                )
            if not count_response.get("profileRequests"):
                un_profiled_assets.append({
                    "assetId": target_table_asset,
                    "assetUid": target_info["target_uid"]
                })
        except Exception as e:
            print(f"Error getting profiles for asset {target_table_asset}: {e}")
            un_profiled_assets.append({
                "assetId": target_table_asset,
                "assetUid": target_info["target_uid"]
            })
            continue


    if un_profiled_assets:
        print(f"Assets required to be profiled on target : {un_profiled_assets}")
        print(f"Tenant: {client.tenant}")
        # Write asset_data to profile-assets.csv in asset-import directory
        if globals.GLOBAL_OUTPUT_DIR:
            profile_assets_csv = globals.GLOBAL_OUTPUT_DIR / "asset-import" / "profile-assets.csv"
        else:
            profile_assets_csv = Path("asset-import/profile-assets.csv")
        profile_assets_csv.parent.mkdir(parents=True, exist_ok=True)
        # Remove duplicates and write
        with open(profile_assets_csv, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['assetId', 'assetUid']
            writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(un_profiled_assets)
        print(f"Wrote asset IDs to {profile_assets_csv}")
    else:
        print("All assets are profiled on target")

    if run_profile:
        trigger_profile_action(client, logger, profile_assets_csv, quiet_mode, verbose_mode)
    pass
