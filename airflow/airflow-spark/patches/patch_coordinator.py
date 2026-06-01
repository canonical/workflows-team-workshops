"""Patch coordinator charm: fix KeyError tls_ca_chain in connection_manager.py."""
import pathlib
import sys

CHARM_PATH = "/var/lib/juju/agents/unit-airflow-coordinator-k8s-0/charm/src/connection_manager.py"

f = pathlib.Path(CHARM_PATH)
content = f.read_text()
old = 'if isinstance(normalized_data["tls_ca_chain"], str):'
new = (
    'if "tls_ca_chain" not in normalized_data:\n'
    '            normalized_data["tls_ca_chain"] = []\n'
    '        if isinstance(normalized_data["tls_ca_chain"], str):'
)
if old in content:
    content = content.replace(old, new)
    f.write_text(content)
    print("    Patch applied successfully")
else:
    print("    Already patched or code changed - skipping")
