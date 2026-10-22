"""Unit tests for the proxy-staging and kubectl-prefix helpers of
`pocket_coffea/executors/executors_kubernetes.py` (no cluster needed)."""
import os

from pocket_coffea.executors.executors_kubernetes import _kubectl_prefix, _stage_proxy


def test_stage_proxy_copies_to_shared_dir(tmp_path):
    src = tmp_path / "x509up_u1234"
    src.write_text("proxy-bytes")
    shared = tmp_path / "work" / "proxies"

    staged = _stage_proxy(str(src), str(shared))

    assert staged == str(shared / "x509up_u1234")
    assert open(staged).read() == "proxy-bytes"
    # staging again onto itself is a no-op, not an error
    assert _stage_proxy(staged, str(shared)) == staged


def test_kubectl_prefix():
    assert _kubectl_prefix({}) == "kubectl"
    assert _kubectl_prefix({"namespace": "cms"}) == "kubectl -n cms"
    assert _kubectl_prefix({"kubernetes-context": "ctx", "namespace": "cms"}) == \
        "kubectl --context ctx -n cms"
