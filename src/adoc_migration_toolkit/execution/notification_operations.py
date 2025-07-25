import csv
import logging
from datetime import datetime

import requests

from ..shared.file_utils import get_output_file_path

def fetch_all_rule_notification_group_ids(client, logger: logging.Logger, source_assembly_ids, quiet_mode: bool = False, verbose_mode: bool = False):
    """
    Fetch all unique configuredNotificationGroupIds from rules API,
    and track which rules reference them.
    """
    page = 0
    size = 20
    unique_ids = set()
    group_id_to_rules = {}
    skipped_rules_log = []

    print("🔍 Starting to fetch rules and extract notification group IDs...")

    while True:
        params = {
            "page": page,
            "size": size,
            "withLatestExecution": "true",
            "sortBy": "startedAt:DESC",
            "ruleStatus": "ENABLED,ACTIVE",
            f"assemblyIds": source_assembly_ids
        }
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        try:
            print(f"/catalog-server/api/rules?{query_string}")
            response = client.make_api_call(
                endpoint=f"/catalog-server/api/rules?{query_string}",
                method='GET'
            )
            data = response

            rules = data.get("rules", [])
            if not rules:
                print(f"⚠️ No rules found on page: {page}")
                break

            for idx, item in enumerate(rules):
                rule_obj = item.get("rule")
                if not rule_obj:
                    skipped_rules_log.append(f"[Page {page} - Rule {idx}] ⛔ Missing 'rule' object")
                    continue

                rule_id = rule_obj.get("id", "Unknown")
                rule_name = rule_obj.get("name", "Unnamed")

                notif_channel = rule_obj.get("notificationChannels")
                if not notif_channel:
                    skipped_rules_log.append(f"[Rule ID: {rule_id}, Name: {rule_name}] ⚠️ Missing 'notificationChannels'")
                    continue

                group_ids = notif_channel.get("configuredNotificationGroupIds")
                if not group_ids:
                    skipped_rules_log.append(f"[Rule ID: {rule_id}, Name: {rule_name}] ⚠️ No 'configuredNotificationGroupIds'")
                    continue

                unique_ids.update(group_ids)
                for gid in group_ids:
                    group_id_to_rules.setdefault(gid, []).append((rule_id, rule_name))

            meta = data.get("meta", {})
            total_count = meta.get("count", 0)
            total_pages = (total_count + size - 1) // size

            if page >= total_pages - 1:
                break
            page += 1

        except requests.RequestException as e:
            msg = f"[Page {page}] ❌ Exception occurred while fetching rules: {e}"
            print(msg)
            skipped_rules_log.append(msg)
            break

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = f"skipped_rules_{timestamp}.log"
    with open(log_filename, "w", encoding="utf-8") as log_file:
        log_file.write("\n".join(skipped_rules_log))

    print(f"\n✅ Fetched {len(unique_ids)} unique configuredNotificationGroupIds.")
    print(f"📝 Skipped rules written to: {log_filename}")

    return unique_ids, group_id_to_rules


def fetch_all_notification_groups(client, logger: logging.Logger, source_context_id, source_assembly_ids, quiet_mode: bool = False, verbose_mode: bool = False):
    """Fetch all source notification groups with pagination."""
    page = 1
    size = 20
    all_groups = []

    while True:
        params = {
            "page": page,
            "size": size
        }
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        try:
            response = client.make_api_call(
                endpoint=f"/api/notifications/api/v1/{source_context_id}/notifications/channels/groups?{query_string}",
                method='GET'
            )
            data = response

            channels = data.get("channels", [])
            if not channels:
                break

            all_groups.extend(channels)

            meta = data.get("meta", {})
            total = meta.get("total", 0)
            if page * size >= total:
                break

            page += 1

        except requests.RequestException as e:
            print(f"Error fetching source notification groups: {e}")
            break

    return all_groups


def fetch_all_target_notification_groups(client, logger: logging.Logger, target_context_id, quiet_mode: bool = False, verbose_mode: bool = False):
    """Fetch all target notification groups with pagination."""
    page = 1
    size = 20
    all_groups = []

    while True:
        params = {
            "page": page,
            "size": size
        }
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])

        try:
            response = client.make_api_call(
                endpoint=f"/api/notifications/api/v1/{target_context_id}/notifications/channels/groups?{query_string}",
                method='GET',
                use_target_auth=True,
                use_target_tenant=True
            )
            data = response

            channels = data.get("channels", [])
            if not channels:
                break

            all_groups.extend(channels)

            meta = data.get("meta", {})
            total = meta.get("total", 0)
            if page * size >= total:
                break

            page += 1

        except requests.RequestException as e:
            print(f"❌ Error fetching target notification groups: {e}")
            break

    return all_groups


def write_notification_data_to_csv(notification_ids, notification_groups, output_file="notification_groups.csv"):
    """Write source-matched notification group info to CSV."""
    matched_groups = []

    for group in notification_groups:
        group_id = group.get("id")
        if not isinstance(group_id, int) or group_id not in notification_ids:
            continue

        name = group.get("name", "N/A")
        channels = group.get("channels", [])
        types = ', '.join(set(ch.get("type", "N/A") for ch in channels if isinstance(ch, dict)))

        matched_groups.append({
            "Notification ID": group_id,
            "Notification Name": name,
            "Notification Type": types
        })

    with open(output_file, mode="w", newline='', encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["Notification ID", "Notification Name", "Notification Type"])
        writer.writeheader()
        writer.writerows(matched_groups)

    print(f"✅ Notification group info written to: {output_file}")


def generate_comparison_csv(source_groups, target_groups, filename):
    """Compare source vs target notification groups by NAME and write to CSV."""
    print("🔄 Generating notification group comparison report...")

    target_group_names = {
        group.get("name", "").strip().lower() for group in target_groups
    }

    comparison_data = []

    for group in source_groups:
        group_id = group.get("id")
        name = group.get("name", "N/A")
        channels = group.get("channels", [])
        types = ', '.join(set(ch.get("type", "N/A") for ch in channels if isinstance(ch, dict)))

        name_key = name.strip().lower()
        exists_in_target = name_key in target_group_names

        comparison_data.append({
            "Notification ID": group_id,
            "Notification Name": name,
            "Notification Type": types,
            "Present in Target": "Yes" if exists_in_target else "No",
            "Comment": "" if exists_in_target else "Missing in target environment"
        })


    with open(filename, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ["Notification ID", "Notification Name", "Notification Type", "Present in Target", "Comment"]
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(comparison_data)

    print(f"✅ Comparison report generated: {filename}")

def precheck_on_notifications(client, logger: logging.Logger, source_context_id: str, target_context_id: str, source_assembly_ids: str, quiet_mode: bool = False, verbose_mode: bool = False) -> bool:
    print(f"🔄 Fetching configuredNotificationGroupIds from rules API... {source_assembly_ids}")
    configured_ids, group_id_to_rules = fetch_all_rule_notification_group_ids(client, logger, source_assembly_ids, source_context_id, target_context_id)
    print(f"✅ Fetched {len(configured_ids)} unique Notification Group IDs.")

    print("🔄 Fetching all notification group definitions from source...")
    all_notification_groups = fetch_all_notification_groups(client, logger, source_context_id, source_assembly_ids, quiet_mode, verbose_mode)
    print(f"✅ Retrieved {len(all_notification_groups)} total source notification group definitions.")

    # Filter notification groups to only those linked to rules
    notification_groups = [
        group for group in all_notification_groups
        if isinstance(group.get("id"), int) and group.get("id") in configured_ids
    ]
    print(f"✅ Filtered {len(notification_groups)} notification groups associated with filtered rules.")


    print("📄 Writing final source CSV output...")
    write_notification_data_to_csv(configured_ids, notification_groups)

    matched_group_ids = {
        group.get("id") for group in notification_groups if isinstance(group.get("id"), int)
    }

    unmatched_ids = configured_ids - matched_group_ids

    if unmatched_ids:
        print(f"\n⚠️ {len(unmatched_ids)} Notification Group ID(s) from rules not found in source groups.")
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        missing_log_file = f"missing_group_id_usage_{timestamp}.log"
        with open(missing_log_file, "w") as f:
            for missing_id in unmatched_ids:
                rules_using = group_id_to_rules.get(missing_id, [])
                id_log = f"🔍 Missing Group ID: {missing_id} — Referenced By {len(rules_using)} Rule(s):"
                print(id_log)
                f.write(id_log + "\n")
                for rid, rname in rules_using:
                    rule_line = f"   - Rule ID: {rid} | Rule Name: {rname}"
                    print(rule_line)
                    f.write(rule_line + "\n")
                f.write("\n")
        print(f"📝 Missing Group ID references saved to: {missing_log_file}")
    else:
        print("\n✅ All Notification Group IDs matched successfully in source.")

    print("\n🔄 Fetching notification groups from target environment...")
    target_notification_groups = fetch_all_target_notification_groups(client, logger, target_context_id, quiet_mode, verbose_mode)
    print(f"✅ Retrieved {len(target_notification_groups)} target notification group definitions.")

    # Generate default output file if not provided

    output_file = get_output_file_path(csv_file= '', default_filename = "notification_group_comparison.csv", category="notifications-check")

    print("📄 Generating source vs target notification comparison...")
    generate_comparison_csv(notification_groups, target_notification_groups, output_file)
