"""Unit tests for the kubernetes manual-jobs executor
(`pocket_coffea/executors/executors_kubernetes.py`).

Exercises manifest/job-script generation and resubmission with the kubectl
call and the rucio-backed site-rewrite tools monkey-patched out
(`executors_kubernetes._run_cmd` / `_get_site_rewrite_tools`), so the tests
need neither a kubernetes cluster, kubectl, nor rucio — mirroring the
dependency-free style of `test_manual_jobs_splitting.py`.
"""
import os
import shutil
import subprocess
import time

import cloudpickle
import pytest
import yaml

from pocket_coffea.executors import executors_kubernetes
from pocket_coffea.executors.executors_kubernetes import (
    ExecutorFactoryKubernetesJobs,
    _normalize_memory,
    _sanitize_k8s_name,
)


class FakeConfigurator:
    """Minimal stand-in for utils.configurator.Configurator: just what
    prepare_jobs/submit_jobs touch. cloudpickle serializes it by value."""

    def __init__(self):
        self.do_postprocessing = True
        self.columns = {}
        self.workflow_options = {}
        self.filesets = None

    def clone(self):
        return FakeConfigurator()

    def set_filesets_manually(self, filesets):
        self.filesets = filesets


def _make_filesets():
    return {
        "TT_2018": {
            "files": [f"root://xrootd.site//store/tt_{i}.root" for i in range(4)],
            "metadata": {"sample": "TT", "nevents": "400"},
        },
        "DY_2018": {
            "files": [f"root://xrootd.site//store/dy_{i}.root" for i in range(2)],
            "metadata": {"sample": "DY", "nevents": "200"},
        },
    }


def _run_options(**overrides):
    opts = {
        "ignore-grid-certificate": True,
        "worker-image": "registry.example.org/pocketcoffea:latest",
        "chunksize": 100,
        "scaleout": 3,
        "cores-per-worker": 1,
        "mem-per-worker": "2GiB",
        "job-name": "job",
        "dry-run": True,
    }
    opts.update(overrides)
    return opts


def _submit(tmp_path, run_options):
    outputdir = str(tmp_path / "output")
    os.makedirs(outputdir, exist_ok=True)
    factory = ExecutorFactoryKubernetesJobs(run_options, outputdir=outputdir)
    factory.submit(FakeConfigurator(), _make_filesets(), outputdir)
    return factory


@pytest.fixture
def kubectl_calls(monkeypatch):
    calls = []

    def fake_run_cmd(cmd):
        calls.append(cmd)
        return 0

    monkeypatch.setattr(executors_kubernetes, "_run_cmd", fake_run_cmd)
    return calls


@pytest.fixture
def fake_site_tools(monkeypatch):
    """Replace the lazy rucio/site-rewrite loader with in-memory fakes that
    mimic the condor-style log scanning of executors_lxplus.find_input_failure_jobs."""

    def find_input_failure_jobs(logs_dir):
        failures, logs = {}, {}
        if not os.path.isdir(logs_dir):
            return failures, logs
        for fname in sorted(os.listdir(logs_dir)):
            stem, ext = os.path.splitext(fname)
            if ext not in (".err", ".out"):
                continue
            parts = stem.split(".")
            if len(parts) < 2:
                continue
            job = "job_" + parts[-1]
            path = os.path.join(logs_dir, fname)
            with open(path, errors="replace") as f:
                if "xrootd error" in f.read().lower():
                    failures.setdefault(job, set()).add("XRootD error")
                    logs.setdefault(job, []).append(path)
        return failures, logs

    tools = {
        "get_xrootd_sites_map": lambda: {"T2_OTHER": "root://other.site/"},
        "get_rucio_client": lambda: None,
        "find_other_file": lambda fl, sitemap, blocklist=None, rucio_client=None:
            "root://other.site//store/" + fl.split("/store/")[-1],
        "rewrite_fileset_blocklist": lambda fs, sitemap, blocklist, rucio_client=None: fs,
        "rewrite_fileset_to_redirector": lambda fs: fs,
        "find_input_failure_jobs": find_input_failure_jobs,
        "GLOBAL_XROOTD_REDIRECTOR": "root://xrootd-cms.infn.it//",
    }
    monkeypatch.setattr(executors_kubernetes, "_get_site_rewrite_tools", lambda: tools)
    return tools


# ----------------------- helpers -----------------------

def test_normalize_memory():
    assert _normalize_memory("2GiB", "1Gi") == "2Gi"
    assert _normalize_memory("2GB", "1Gi") == "2Gi"
    assert _normalize_memory("500MiB", "1Gi") == "500Mi"
    assert _normalize_memory("500MB", "1Gi") == "500Mi"
    assert _normalize_memory(4, "1Gi") == "4Gi"
    assert _normalize_memory(None, "1Gi") == "1Gi"
    # valid kubernetes quantities pass through unchanged
    assert _normalize_memory("3Gi", "1Gi") == "3Gi"
    assert _normalize_memory("4G", "1Gi") == "4G"
    assert _normalize_memory("500M", "1Gi") == "500M"
    with pytest.raises(Exception, match="not a valid kubernetes quantity"):
        _normalize_memory("lots", "1Gi")


def test_sanitize_k8s_name():
    assert _sanitize_k8s_name("My_Job.v2") == "my-job-v2"
    assert _sanitize_k8s_name("---") == "pocket-coffea"
    assert len(_sanitize_k8s_name("x" * 100)) <= 63


# ----------------------- submission -----------------------

def test_dry_run_writes_files_without_kubectl(tmp_path, kubectl_calls):
    factory = _submit(tmp_path, _run_options())
    jobs_dir = factory.jobs_dir

    assert kubectl_calls == []
    for fname in ("job.sh", "jobs_all.yaml", "jobs_config.yaml",
                  "chunksizes.txt", "inner_run_options.yaml"):
        assert os.path.isfile(os.path.join(jobs_dir, fname)), fname

    jobs_config = yaml.safe_load(open(f"{jobs_dir}/jobs_config.yaml"))
    assert jobs_config["split_by_category"] is False
    n_jobs = len(jobs_config["jobs_list"])
    for i in range(n_jobs):
        assert os.path.isfile(f"{jobs_dir}/config_job_{i}.pkl")
        assert os.path.isfile(f"{jobs_dir}/job_{i}.yaml")
        assert os.path.isfile(f"{jobs_dir}/job_{i}.idle")
    # one chunksize line per job
    with open(f"{jobs_dir}/chunksizes.txt") as f:
        assert len(f.read().splitlines()) == n_jobs


def test_indexed_job_manifest(tmp_path, kubectl_calls):
    factory = _submit(tmp_path, _run_options(scaleout=2))
    jobs_dir = factory.jobs_dir
    n_jobs = len(yaml.safe_load(open(f"{jobs_dir}/jobs_config.yaml"))["jobs_list"])

    manifest = yaml.safe_load(open(f"{jobs_dir}/jobs_all.yaml"))
    assert manifest["apiVersion"] == "batch/v1"
    assert manifest["kind"] == "Job"
    assert manifest["spec"]["completions"] == n_jobs
    assert manifest["spec"]["completionMode"] == "Indexed"
    assert manifest["spec"]["parallelism"] == min(2, n_jobs)
    # per-index failure isolation: one failed chunk must not kill the others
    assert manifest["spec"]["backoffLimitPerIndex"] == 0
    assert manifest["spec"]["maxFailedIndexes"] == n_jobs
    assert "backoffLimit" not in manifest["spec"]
    container = manifest["spec"]["template"]["spec"]["containers"][0]
    assert container["image"] == "registry.example.org/pocketcoffea:latest"
    assert container["resources"]["requests"] == {"cpu": "1", "memory": "2Gi"}
    assert container["command"][-1].endswith("job.sh")
    assert manifest["spec"]["template"]["spec"]["restartPolicy"] == "Never"
    # no namespace / pvc unless configured
    assert "namespace" not in manifest["metadata"]
    assert "volumes" not in manifest["spec"]["template"]["spec"]


def test_per_job_manifest_has_explicit_job_id(tmp_path, kubectl_calls):
    factory = _submit(tmp_path, _run_options())
    jobs_dir = factory.jobs_dir

    manifest = yaml.safe_load(open(f"{jobs_dir}/job_1.yaml"))
    assert "completions" not in manifest["spec"]
    assert manifest["spec"]["backoffLimit"] == 0
    env = manifest["spec"]["template"]["spec"]["containers"][0]["env"]
    assert {"name": "POCKET_JOB_ID", "value": "1"} in env
    assert manifest["metadata"]["name"].endswith("-1")


def test_job_script_contents(tmp_path, kubectl_calls):
    factory = _submit(tmp_path, _run_options())
    script = open(f"{factory.jobs_dir}/job.sh").read()

    assert 'JOBID=${POCKET_JOB_ID:-$JOB_COMPLETION_INDEX}' in script
    assert "chunksizes.txt" in script
    assert "--custom-run-options" in script
    assert "--executor iterative" in script
    assert "pocket-coffea run" in script
    assert "set_status running" in script
    assert "set_status done" in script
    # condor-style log names (timestamp as ClusterId) behind a writability guard
    assert 'if [ -w "$JOBDIR/logs" ]; then' in script
    assert "job_$LOGTAG.$JOBID.err" in script
    # single core -> iterative; proxy ignored -> no X509 export
    assert "X509_USER_PROXY" not in script


def test_job_script_bash_syntax(tmp_path, kubectl_calls):
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash not available")
    for opts in (_run_options(), _run_options(**{"split-by-category": True,
                                                 "job-name": "jobsplit"})):
        factory = _submit(tmp_path, opts)
        res = subprocess.run([bash, "-n", f"{factory.jobs_dir}/job.sh"],
                             capture_output=True, text=True)
        assert res.returncode == 0, res.stderr


def test_multicore_uses_futures(tmp_path, kubectl_calls):
    factory = _submit(tmp_path, _run_options(**{"cores-per-worker": 4}))
    script = open(f"{factory.jobs_dir}/job.sh").read()
    assert "--executor futures --scaleout 4" in script


def test_split_by_category_splits_in_pod_dir(tmp_path, kubectl_calls):
    factory = _submit(tmp_path, _run_options(**{"split-by-category": True}))
    jobs_dir = factory.jobs_dir
    script = open(f"{jobs_dir}/job.sh").read()

    # split must run in the pod-local output dir, not the shared output dir
    assert "\n    cd output\n" in script
    assert "split-output output_all.coffea -b category" in script
    # each produced piece is pushed to the shared dir with a per-job suffix
    assert "_job_$JOBID.coffea" in script
    # failure of split-output must fail the job, not silently continue
    assert "split-output failed" in script
    assert yaml.safe_load(open(f"{jobs_dir}/jobs_config.yaml"))["split_by_category"] is True


def test_namespace_pvc_and_submission(tmp_path, kubectl_calls):
    opts = _run_options(**{
        "dry-run": False,
        "namespace": "cms-analysis",
        "kubernetes-context": "my-ctx",
        "pvc-name": "shared-volume",
        "pvc-mount-path": "/work",
    })
    factory = _submit(tmp_path, opts)
    jobs_dir = factory.jobs_dir

    manifest = yaml.safe_load(open(f"{jobs_dir}/jobs_all.yaml"))
    assert manifest["metadata"]["namespace"] == "cms-analysis"
    pod = manifest["spec"]["template"]["spec"]
    assert pod["volumes"][0]["persistentVolumeClaim"]["claimName"] == "shared-volume"
    assert pod["containers"][0]["volumeMounts"][0]["mountPath"] == "/work"

    # stale Job cleared first, then the manifest applied
    assert len(kubectl_calls) == 2
    assert "kubectl --context my-ctx -n cms-analysis delete job" in kubectl_calls[0]
    assert kubectl_calls[0].endswith("--ignore-not-found")
    assert "kubectl --context my-ctx -n cms-analysis apply -f" in kubectl_calls[1]
    assert kubectl_calls[1].endswith("jobs_all.yaml")


def test_failed_apply_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(executors_kubernetes, "_run_cmd", lambda cmd: 1)
    with pytest.raises(Exception, match="kubectl command failed"):
        _submit(tmp_path, _run_options(**{"dry-run": False}))


def test_pvc_without_mount_path_raises(tmp_path, kubectl_calls):
    with pytest.raises(Exception, match="pvc-mount-path"):
        _submit(tmp_path, _run_options(**{"pvc-name": "shared-volume"}))


def test_missing_worker_image_raises(tmp_path, kubectl_calls):
    with pytest.raises(Exception, match="worker-image"):
        _submit(tmp_path, _run_options(**{"worker-image": None}))


# ----------------------- resubmission -----------------------

def _prepare_failed_job(tmp_path, kubectl_calls, err_text=None):
    """Dry-run submission, then mark job_0 failed (optionally with a log)."""
    factory = _submit(tmp_path, _run_options())
    jobs_dir = factory.jobs_dir
    n_jobs = len(yaml.safe_load(open(f"{jobs_dir}/jobs_config.yaml"))["jobs_list"])
    assert n_jobs >= 2

    for i in range(n_jobs):
        os.remove(f"{jobs_dir}/job_{i}.idle")
    open(f"{jobs_dir}/job_0.failed", "w").close()
    open(f"{jobs_dir}/job_1.done", "w").close()
    if err_text is not None:
        ts = int(time.time())
        with open(f"{jobs_dir}/logs/job_{ts}.0.err", "w") as f:
            f.write(err_text)
    return factory, jobs_dir


def test_recreate_jobs_auto(tmp_path, kubectl_calls, fake_site_tools):
    factory, jobs_dir = _prepare_failed_job(tmp_path, kubectl_calls)

    opts = _run_options(**{"dry-run": False, "recreate-jobs": "auto"})
    outputdir = str(tmp_path / "output")
    factory2 = ExecutorFactoryKubernetesJobs(opts, outputdir=outputdir)
    factory2.submit(FakeConfigurator(), _make_filesets(), outputdir)

    # job_0 deleted + re-applied; nothing else resubmitted
    deletes = [c for c in kubectl_calls if " delete job " in c]
    applies = [c for c in kubectl_calls if " apply -f " in c]
    assert len(deletes) == 1 and deletes[0].endswith("--ignore-not-found")
    assert len(applies) == 1 and applies[0].endswith("job_0.yaml")
    assert not os.path.exists(f"{jobs_dir}/job_0.failed")
    assert os.path.isfile(f"{jobs_dir}/job_0.idle")


def test_recreate_rewrites_xrootd_failed_inputs(tmp_path, kubectl_calls, fake_site_tools):
    factory, jobs_dir = _prepare_failed_job(
        tmp_path, kubectl_calls,
        err_text="OSError: XRootD error: [ERROR] Server responded with an error\n")

    opts = _run_options(**{"dry-run": False, "recreate-jobs": "auto"})
    outputdir = str(tmp_path / "output")
    factory2 = ExecutorFactoryKubernetesJobs(opts, outputdir=outputdir)
    factory2.submit(FakeConfigurator(), _make_filesets(), outputdir)

    # the failed job's pickled config must now carry the rewritten file urls
    config = cloudpickle.load(open(f"{jobs_dir}/config_job_0.pkl", "rb"))
    files = [fl for ds in config.filesets.values() for fl in ds["files"]]
    assert files and all(fl.startswith("root://other.site//store/") for fl in files)
    # the analysed log was archived so the next scan sees only fresh attempts
    assert os.listdir(f"{jobs_dir}/logs/processed")
    # and the job was resubmitted
    assert any(c.endswith("job_0.yaml") for c in kubectl_calls if " apply -f " in c)
