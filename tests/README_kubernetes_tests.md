# Kubernetes executor — unit-test commands (local cores only)

Commands for running the first-iteration Kubernetes-executor unit tests on a
laptop/desktop. **Everything on this page runs on the local machine's cores and
submits NOTHING**: `kubectl` and the rucio site-rewrite tools are
monkey-patched out in the tests, and dry-run mode never invokes `kubectl`.
Before any *real* submission (not covered here), always pin the context
explicitly (`kubernetes-context` run option / `kubectl --context <local>`) —
never rely on the kubeconfig default, which may point at a shared remote
cluster (e.g. NRP/Nautilus).

## 1. One-time environment

The tests need coffea 0.7 and a few extras (no grid proxy, no XRootD, no
kubernetes client):

```bash
python3 -m venv ~/pocotest-venv
~/pocotest-venv/bin/pip install "coffea==0.7.29" pytest rich pyyaml cloudpickle omegaconf click law parsl
```

## 2. Running the unit tests

From the repository root (branch `kubernetes-executor-test`):

```bash
# kubernetes executor suites (20 tests)
PYTHONPATH=$PWD ~/pocotest-venv/bin/python -m pytest \
    tests/test_kubernetes_jobs.py tests/test_kubernetes_proxy_staging.py -v
```

```bash
# full manual-jobs stack the executor builds on (44 tests, ~2 s)
PYTHONPATH=$PWD ~/pocotest-venv/bin/python -m pytest \
    tests/test_kubernetes_jobs.py tests/test_kubernetes_proxy_staging.py \
    tests/test_manual_jobs_splitting.py tests/test_recreate_jobs_blocklist.py -q
```

```bash
# a single test
PYTHONPATH=$PWD ~/pocotest-venv/bin/python -m pytest \
    tests/test_kubernetes_jobs.py::test_indexed_job_manifest -v
```

What they cover:

| Suite | Coverage |
|---|---|
| `test_kubernetes_jobs.py` | Indexed-Job manifest (completions/parallelism, `backoffLimitPerIndex`, PVC mounts, namespace/context, `gpus-per-worker`), `job.sh` generation (incl. `bash -n` syntax check), per-job chunksizes, split-by-category, dry-run, failed-`kubectl apply` handling, `--recreate-jobs auto` incl. the XRootD input-rewrite path |
| `test_kubernetes_proxy_staging.py` | proxy staging to a worker-visible dir, kubectl prefix building |
| `test_manual_jobs_splitting.py` | fileset splitting (uniform / per-sample / per-sample chunksize) shared with the condor executors |
| `test_recreate_jobs_blocklist.py` | site-rewrite helpers used on resubmission |

**Dummy inputs**: the tests use in-memory filesets with fake `root://…` URLs —
the files are never opened at submission level (splitting uses only the
`nevents` metadata), so no actual ROOT file is required for these tests. A
dummy NanoAOD ROOT file only becomes necessary for a future end-to-end test
where a pod actually *processes* events; that test intentionally does not
exist yet (see README "first iteration" section).

## 3. Dry-run inspection (framework-generated manifests, still no submission)

The executor's `dry-run: true` writes everything a submission would use, for
inspection:

```bash
pocket-coffea run --cfg <config>.py -o k8s_dry_test -e k8sjobs@kubernetes \
    -ro my_k8s_options.yaml      # with dry-run: true in the yaml
```

(NOTE: `--dry-run false` on the CLI does NOT disable it — the runner stores the
truthy string `"false"`; set it in the yaml.)

Produced in `<out>/<job-name>/`, worth eyeballing before any real launch:

- `jobs_all.yaml` — the Indexed `batch/v1` Job (one pod per chunk-group)
- `job_{i}.yaml` — single-job manifests for resubmission
- `job.sh` — the pod payload script (`bash -n job.sh` to re-check syntax)
- `chunksizes.txt`, `inner_run_options.yaml`, `config_job_{i}.pkl`, `jobs_config.yaml`
- `job_{i}.idle` — the monitoring flag files, all idle before submission

## 4. Monitoring surface (for when real launches are authorized — NOT run by the tests)

Job state lives in two places, both covered by the unit tests at generation level:

```bash
# flag files on the shared jobs dir: .idle -> .running -> .done | .failed
ls <out>/<job-name>/ | grep -E 'idle|running|done|failed'

# mirrored pod logs (condor-style naming; drives --recreate-jobs log scanning)
ls <out>/<job-name>/logs/        # job_<timestamp>.<index>.{out,err}

# live cluster view — ALWAYS with an explicit local context
kubectl --context <local-context> get pods -l app=pocket-coffea
kubectl --context <local-context> logs job/pocket-coffea-<job-name>
```

`check-jobs` does **not** support this executor (it expects condor `.sub`
files and the condor event log); resubmission goes through
`pocket-coffea run ... --recreate-jobs auto`.
