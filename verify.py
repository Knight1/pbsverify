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
            timeout=60 # Increased timeout for large lists
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

    for ds in datastores:
        store_name = ds['name']
        print(f"\nChecking Datastore: {store_name}")

        try:
            snapshots = pbs.admin.datastore(store_name).snapshots.get()
        except Exception as e:
            print(f"   Error reading snapshots: {e}")
            continue

        failed_backups = []

        # 1. Collect all failed snapshots first
        for snap in snapshots:
            verification = snap.get('verification')
            if verification and verification.get('state') == 'failed':
                failed_backups.append(snap)

        if not failed_backups:
            print(f"   All verified backups in '{store_name}' are healthy.")
            continue

        print(f"   Found {len(failed_backups)} failed backups. Queueing re-verification jobs...")

        # 2. Trigger a precise job for EACH failed snapshot
        for snap in failed_backups:
            b_type = snap['backup-type']
            b_id   = snap['backup-id']
            b_time = snap['backup-time'] # Integer timestamp required for precision

            print(f"      Queueing: {b_type}/{b_id} (Time: {b_time})")

            try:
                # We target the specific snapshot using backup-type, backup-id, and backup-time
                # ignore-verified=False forces it to overwrite the 'failed' status
                upid = pbs.admin.datastore(store_name).verify.post(
                    **{
                        'backup-type': b_type,
                        'backup-id': b_id,
                        'backup-time': b_time,
                        'ignore-verified': False
                    }
                )
            except Exception as e:
                print(f"      Failed to trigger task: {e}")

if __name__ == "__main__":
    main()
