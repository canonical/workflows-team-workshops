"""Patch executor pod template: add extra_env to init container for DAG bundle sync."""
import os
import pathlib
import sys

TEMPLATE_PATH = "/var/lib/juju/agents/unit-airflow-kubernetes-executor-k8s-0/charm/src/templates/pod_template.yaml.j2"

f = pathlib.Path(TEMPLATE_PATH)
content = f.read_text()

# Check if already patched
if "extra_env" in content.split("initContainers")[1].split("containers")[0]:
    print("    Already patched - skipping")
    sys.exit(0)

# Find the init container env block and add extra_env vars
old = """      env:
        # Defaults to /opt/airflow as per Airflow standards, can be overridden if
        # the Airflow Coordinator charm sets the value.
        - name: AIRFLOW_HOME
          value: {{ airflow_home | default('/opt/airflow') }}
        # Defaults to /opt/airflow/dag_bundles as per Airflow standards, can be overridden
        # if the Airflow Coordinator charm sets the value.
        - name: AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_STORAGE_PATH
          value: {{ dag_bundle_storage_path | default('/opt/airflow/dag_bundles') }}
      volumeMounts:"""

new = """      env:
        - name: AIRFLOW_HOME
          value: {{ airflow_home | default('/opt/airflow') }}
        - name: AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_STORAGE_PATH
          value: {{ dag_bundle_storage_path | default('/opt/airflow/dag_bundles') }}
        - name: AIRFLOW_CONN_GIT_DEFAULT
          value: '{"conn_type": "git"}'
{% for env_var in extra_env %}
        - name: {{ env_var.name }}
          valueFrom:
            secretKeyRef:
              name: {{ secret_name }}
              key: {{ env_var.secret_key }}
{% endfor %}
      volumeMounts:"""

if old in content:
    content = content.replace(old, new)
    f.write_text(content)
    print("    Init container env vars patched")
else:
    print("    WARNING: Could not find expected init container env block")
    sys.exit(1)
