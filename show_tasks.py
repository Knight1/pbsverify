from proxmoxer import ProxmoxAPI
import urllib3
import json

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION ---
PBS_HOST = "localhost"
PBS_PORT = 8007
PBS_USER = "root@pam"       # Or your API user
PBS_TOKEN_ID = "verify"    # Token ID
PBS_TOKEN_SECRET = "" # Token Secret

pbs = ProxmoxAPI(PBS_HOST, user=PBS_USER, token_name=PBS_TOKEN_ID, token_value=PBS_TOKEN_SECRET, port=PBS_PORT, verify_ssl=False, service='PBS')

print("--- 1. Checking Node Name ---")
try:
    nodes = pbs.nodes.get()
    print(f"API sees these nodes: {[n['node'] for n in nodes]}")
    real_node_name = nodes[0]['node']
except Exception as e:
    print(f"Could not list nodes (Permission Error?): {e}")
    real_node_name = "localhost" # Fallback

print(f"\n--- 2. Checking Tasks on '{real_node_name}' ---")
try:
    # Get ALL running tasks
    tasks = pbs.nodes(real_node_name).tasks.get(running=1)
    print(f"Raw Task Count: {len(tasks)}")

    if len(tasks) > 0:
        print("First 3 tasks found:")
        print(json.dumps(tasks[:3], indent=2))

        # Check for verification workers specifically
        verify_tasks = [t for t in tasks if 'verify' in t.get('worker_type', '')]
        print(f"\nVerify Jobs running: {len(verify_tasks)}")
    else:
        print("No running tasks found. (If jobs are running in GUI, this is a permission issue!)")

except Exception as e:
    print(f"Error fetching tasks: {e}")
