import time
from proxmoxer import ProxmoxAPI
import urllib3

# Disable SSL warnings for self-signed certs
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION ---
PBS_HOST = "localhost"
PBS_PORT = 8007
PBS_USER = "root@pam"       # Or your API user
PBS_TOKEN_ID = "verify"    # Token ID
PBS_TOKEN_SECRET = "f8f416b2-5ffb-43bb-94be-158862f67cc7" # Token Secret
MAX_CONCURRENT_JOBS = 10  # Maximum running verify jobs allowed
POLL_INTERVAL = 10        # Seconds to wait before checking again

NODE_NAME = "localhost"

def get_running_verify_jobs(pbs):
    """Returns the count of currently running verification tasks."""
    try:
        active_tasks = pbs.nodes(NODE_NAME).tasks.get(running=1)
        verify_tasks = [
            t for t in active_tasks
            if 'verify' in t.get('worker_type', '')
        ]
        return len(verify_tasks)
    except Exception as e:
        print(f"      Error checking tasks: {e}")
        # If we can't check, assume full to be safe
        return MAX_CONCURRENT_JOBS

def main():
    print(f"Connecting to {PBS_HOST}...")
    try:
        pbs = ProxmoxAPI(
            PBS_HOST,
            user=PBS_USER,
            token_name=PBS_TOKEN_ID,
            token_value=PBS_TOKEN_SECRET,
            port=PBS_PORT,
            verify_ssl=False,
            service='PBS',
            timeout=60
        )
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    # Get Datastores
    try:
        datastores = pbs.config.datastore.get()
    except Exception as e:
        print(f"Could not fetch datastores: {e}")
        return

    all_failed_snapshots = []

    # 1. SCAN PHASE
    print("\nScanning all datastores for failures...")
    for ds in datastores:
        store_name = ds['name']
        try:
            snapshots = pbs.admin.datastore(store_name).snapshots.get()
            for snap in snapshots:
                verification = snap.get('verification')
                if verification and verification.get('state') == 'failed':
                    snap['store_name'] = store_name
                    all_failed_snapshots.append(snap)
        except Exception as e:
            print(f"   Error reading snapshots in {store_name}: {e}")

    total_failures = len(all_failed_snapshots)
    if total_failures == 0:
        print("No failed backups found. System healthy.")
        return

    print(f"Found {total_failures} failed backups. Queue Limit: {MAX_CONCURRENT_JOBS}")

    # 2. EXECUTION PHASE
    for index, snap in enumerate(all_failed_snapshots, 1):
        store_name = snap['store_name']
        b_type = snap['backup-type']
        b_id   = snap['backup-id']
        b_time = snap['backup-time']

        # --- CONCURRENCY CHECK ---
        while True:
            running_count = get_running_verify_jobs(pbs)

            if running_count < MAX_CONCURRENT_JOBS:
                break

            print(f"      Throttling active... ({running_count}/{MAX_CONCURRENT_JOBS} jobs running). Waiting {POLL_INTERVAL}s...")
            time.sleep(POLL_INTERVAL)
        # -------------------------

        print(f"[{index}/{total_failures}] Starting: {b_type}/{b_id} ({b_time})")

        try:
            pbs.admin.datastore(store_name).verify.post(
                **{
                    'backup-type': b_type,
                    'backup-id': b_id,
                    'backup-time': b_time,
                    'ignore-verified': False
                }
            )
            # Wait 2s to let the new job register in the API task list
            time.sleep(2)
        except Exception as e:
            print(f"      Trigger failed: {e}")

    print("\nAll jobs queued successfully.")

if __name__ == "__main__":
    main()
