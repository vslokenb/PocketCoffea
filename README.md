```bash
    ____             __        __  ______      ________          
   / __ \____  _____/ /_____  / /_/ ____/___  / __/ __/__  ____ _
  / /_/ / __ \/ ___/ //_/ _ \/ __/ /   / __ \/ /_/ /_/ _ \/ __ `/
 / ____/ /_/ / /__/ ,< /  __/ /_/ /___/ /_/ / __/ __/  __/ /_/ / 
/_/    \____/\___/_/|_|\___/\__/\____/\____/_/ /_/  \___/\__,_/  
```
<!--[![Actions Status][actions-badge]][actions-link] -->
[![Documentation Status][rtd-badge]][rtd-link]
[![PyPI version][pypi-version]][pypi-link]
[![PyPI platforms][pypi-platforms]][pypi-link]
[![GitHub Discussion][github-discussions-badge]][github-discussions-link]

<!-- prettier-ignore-start -->
[actions-badge]:            https://github.com/PocketCoffea/PocketCoffea/workflows/CI/badge.svg
[actions-link]:             https://github.com/PocketCoffea/PocketCoffea/actions
[github-discussions-badge]: https://img.shields.io/static/v1?label=Discussions&message=Ask&color=blue&logo=github
[github-discussions-link]:  https://github.com/PocketCoffea/PocketCoffea/discussions
[pypi-link]:                https://pypi.org/project/pocket-coffea/
[pypi-platforms]:           https://img.shields.io/pypi/pyversions/pocket-coffea
[pypi-version]:             https://img.shields.io/pypi/v/pocket-coffea
[rtd-badge]:                https://readthedocs.org/projects/pocketcoffea/badge/?version=latest
[rtd-link]:                 https://pocketcoffea.readthedocs.io/en/latest/?badge=latest

<!-- prettier-ignore-end -->
PocketCoffea is a slim configuration framework for CMS NanoAOD analysess based on [Coffea](https://github.com/CoffeaTeam/coffea/). 

The goal of the framework is to define an HEP analysis in a declarative way where possible (with a well defined
configuration files), and with python code where customization is needed (by subclassing the base PocketCoffea processor).

PocketCoffea defines a customizable structure to process NanoAOD events and define weights, categories, histograms. This
is done thans to a `BaseProcessor` class which defines a `workflow` of operations to go from Raw NanoAOD to histograms.
The user can customize the process from the confguration file or by redefining well-defined steps in the workflow.

## Documentation

- All the documentaton material is hosted at: https://pocketcoffea.readthedocs.io

- A set of tutorials is available at: https://github.com/PocketCoffea/Tutorials

- Complete analyses examples
  - Z->ee **[analysis example page](https://pocketcoffea.readthedocs.io/en/latest/analysis_example.html)** 
  
- [Join](https://mattermost.web.cern.ch/cms-exp/channels/pocketcoffea---qa) the Q&A CMS Mattermost channel for technical questions

## Kubernetes executors — FIRST ITERATION (testing)

**This branch (`kubernetes-executor-test`) carries the first iteration of the new
Kubernetes executors. It is a test, not yet a supported production path.**

Two new executor strings, following the existing dask/condor site conventions
(full documentation in `docs/running.md`, section "Kubernetes executors"):

- `dask@kubernetes` — Dask cluster through the dask-kubernetes operator
  (`pip install pocket_coffea[kubernetes]`; operator CRDs must be installed in
  the cluster).
- `k8sjobs@kubernetes` — manual-jobs executor: one Indexed `batch/v1` Job for
  the whole fileset splitting (pod completion index = condor `$(ProcId)`),
  per-job manifests for `--recreate-jobs`, `kubectl apply` submission. Needs
  only `kubectl` on the submit host. Per-index failure isolation requires
  **Kubernetes >= 1.29**.

Intended use case: running the usual grouped analysis passes
(`pocket-coffea run --cfg <config> -o <out> --executor k8sjobs@kubernetes -ro <options>.yaml`)
on a Kubernetes cluster/analysis facility, with the jobs dir, output dir and
voms proxy on a shared mount visible to the pods at the same absolute paths
(`pvc-name`/`pvc-mount-path`), and optional GPUs per pod (`gpus-per-worker`).

Initial test cases (what has been exercised so far):

1. Unit tests, no cluster needed: `pytest tests/test_kubernetes_jobs.py
   tests/test_kubernetes_proxy_staging.py` — manifest structure (Indexed Job,
   per-index backoff, PVC mounts, namespace/context, GPU limits), `job.sh`
   generation (`bash -n` checked, per-job chunksizes, flag files),
   split-by-category, dry-run, failed-submission handling, `--recreate-jobs`
   resubmission including the XRootD input-rewrite path, proxy staging.
2. `--executor k8sjobs@kubernetes` with `dry-run: true` — writes
   `jobs_all.yaml`, `job_{i}.yaml`, `job.sh`, `chunksizes.txt`,
   `inner_run_options.yaml` for inspection without submitting.
3. Not yet exercised: real submissions at scale on a cluster — that is the
   point of this test branch.

## Testing 
At the moment this repository is mirrored to the CERN GitLab:  https://gitlab.cern.ch/cms-analysis/general/PocketCoffea.

Tests are running on the GitLab [CDCI](https://gitlab.cern.ch/cms-analysis/general/PocketCoffea/-/pipelines) for every commit. Docker images are created for the latest, stable branchs, and for tags, and are pushed to `/cvmfs` with the [`unpacked`](https://gitlab.cern.ch/unpacked/sync) CERN service. 
