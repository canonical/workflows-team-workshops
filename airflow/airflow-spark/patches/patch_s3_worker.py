"""Patch rendered worker pod template to inject S3 connection env var into BOTH init and main containers."""
import os
import pathlib
import sys

TEMPLATE_FILE = "/opt/airflow/pod_templates/worker_pod_template.yaml"

env_name = os.environ.get("CONN_ENV_NAME", "")
conn_uri = os.environ.get("S3_CONN_URI", "")

if not env_name or not conn_uri:
    print("    ERROR: CONN_ENV_NAME and S3_CONN_URI env vars required")
    sys.exit(1)

f = pathlib.Path(TEMPLATE_FILE)
content = f.read_text()

inject = f"        - name: {env_name}\n          value: '{conn_uri}'\n"

# Inject into init container (before first volumeMounts)
if env_name not in content.split("containers:")[0]:
    marker = "      volumeMounts:\n        - name: airflow-logs"
    idx = content.find(marker)
    if idx >= 0:
        content = content[:idx] + inject + content[idx:]
        print("    S3 conn injected into init container")
    else:
        print("    WARNING: Could not find init container injection point")

# Inject into main container (before AIRFLOW__CORE__EXECUTOR or another env var in containers section)
containers_section = content.split("containers:")[1] if "containers:" in content else ""
if env_name not in containers_section:
    # Find a good injection point in the main container's env section
    # Look for AIRFLOW_CONN_GIT_DEFAULT and add after it
    git_marker = "        - name: AIRFLOW_CONN_GIT_DEFAULT\n          value: '{\"conn_type\": \"git\"}'\n"
    if git_marker in content:
        content = content.replace(git_marker, git_marker + inject)
        print("    S3 conn injected into main container (after GIT_DEFAULT)")
    else:
        # Fallback: inject before AIRFLOW__CORE__EXECUTOR
        executor_marker = "        - name: AIRFLOW__CORE__EXECUTOR"
        idx = content.find(executor_marker)
        if idx >= 0:
            content = content[:idx] + inject + content[idx:]
            print("    S3 conn injected into main container (before EXECUTOR)")
        else:
            print("    WARNING: Could not find main container injection point")

f.write_text(content)
print("    Done")
