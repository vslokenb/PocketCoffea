'''
Executors for running PocketCoffea on a generic Kubernetes cluster.

Two modes are provided, mirroring the dask/condor split used at the other sites:

- ``dask@kubernetes``: spins up a Dask cluster on Kubernetes through the
  dask-kubernetes operator (https://kubernetes.dask.org) and streams the
  processing through it, like ``dask@lxplus``/``dask@lpc`` do with HTCondor.
  Requires the ``dask-kubernetes`` package (``pip install pocket_coffea[kubernetes]``)
  and the Dask operator installed in the cluster.

- ``k8sjobs@kubernetes``: the manual-jobs executor (one self-contained job per
  chunk-group, like ``condor@lxplus``/``condor@lpc``) targeting Kubernetes
  ``batch/v1`` Jobs. It writes one Indexed Job manifest covering all splits plus
  a per-job manifest for resubmission, and submits with ``kubectl apply``.
  Only ``kubectl`` is required on the submit host, no python kubernetes client.
  Per-index failure isolation uses ``backoffLimitPerIndex``, which requires
  Kubernetes >= 1.29 (where the feature is on by default).

Shared-storage contract for ``k8sjobs``: the jobs dir, the output dir and the
voms proxy copy are read and written by the pods THROUGH THE SAME ABSOLUTE
PATHS as on the submit host. Mount the volume that holds them (e.g. a PVC with
``pvc-name``/``pvc-mount-path``, or whatever your facility auto-mounts) so those
paths resolve inside the pod. Outputs under ``/eos/...`` are instead pushed
with xrdcp like at LPC (see ``eos-prefix``). The software environment comes
from ``worker-image`` (always invoked as ``pocket-coffea run``): a registry
image, since pods cannot see the submit host's python environment.
'''
import os
import re
import subprocess
from copy import deepcopy
from coffea import processor as coffea_processor
from .executors_base import ExecutorFactoryABC
from .executors_base import IterativeExecutorFactory, FuturesExecutorFactory
from .executors_manual_jobs import ExecutorFactoryManualABC, write_inner_run_options, INNER_RUN_OPTIONS_FILENAME
import cloudpickle
import yaml


def _run_cmd(cmd):
    '''Run a shell command, streaming its output. Factored out so tests can
    monkey-patch it instead of mocking subprocess (see CLAUDE.md testing
    conventions for helpers that talk to external services).'''
    print(f">> {cmd}")
    return subprocess.call(cmd, shell=True)


def _get_site_rewrite_tools():
    '''Lazy import of the rucio-backed input-recovery helpers, factored out so
    tests can monkey-patch it and the module stays importable without rucio.'''
    from pocket_coffea.utils.rucio import get_xrootd_sites_map, get_rucio_client
    from pocket_coffea.utils.site_rewrite import (
        find_other_file,
        rewrite_fileset_blocklist,
        rewrite_fileset_to_redirector,
        GLOBAL_XROOTD_REDIRECTOR,
    )
    from pocket_coffea.executors.executors_lxplus import find_input_failure_jobs
    return {
        "get_xrootd_sites_map": get_xrootd_sites_map,
        "get_rucio_client": get_rucio_client,
        "find_other_file": find_other_file,
        "rewrite_fileset_blocklist": rewrite_fileset_blocklist,
        "rewrite_fileset_to_redirector": rewrite_fileset_to_redirector,
        "find_input_failure_jobs": find_input_failure_jobs,
        "GLOBAL_XROOTD_REDIRECTOR": GLOBAL_XROOTD_REDIRECTOR,
    }


def _sanitize_k8s_name(name):
    '''Make a string usable as a kubernetes resource name (DNS-1123 label):
    lowercase alphanumerics and dashes, max 63 chars, must start/end alphanumeric.'''
    name = re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-")
    return name[:63].rstrip("-") or "pocket-coffea"


# Kubernetes resource quantity: plain number or number + one of these suffixes.
_K8S_QUANTITY_RE = re.compile(r"^\d+(\.\d+)?(Ki|Mi|Gi|Ti|Pi|Ei|m|k|M|G|T|P|E)?$")


def _normalize_memory(value, default):
    '''Normalize a memory run-option to a kubernetes quantity.

    Valid k8s quantities ("2Gi", "4G", "500Mi") pass through unchanged; the
    GB/GiB/MB/MiB spellings used by the other executors map to Gi/Mi; a plain
    number is interpreted as GB like at the other sites.'''
    if value is None:
        return default
    mem = str(value).strip()
    if not mem:
        return default
    upper = mem.upper()
    for suffix, k8s in (("GIB", "Gi"), ("MIB", "Mi"), ("TIB", "Ti"),
                        ("GB", "Gi"), ("MB", "Mi"), ("TB", "Ti")):
        if upper.endswith(suffix):
            return mem[: -len(suffix)].strip() + k8s
    if _K8S_QUANTITY_RE.match(mem):
        if re.match(r"^\d+(\.\d+)?$", mem):
            # plain number: follow the other executors and interpret it as GB
            return f"{mem}Gi"
        return mem
    raise Exception(f"mem-per-worker value {value!r} is not a valid kubernetes quantity")


class DaskExecutorFactory(ExecutorFactoryABC):
    '''Dask cluster on Kubernetes via the dask-kubernetes operator.'''

    def __init__(self, run_options, outputdir, **kwargs):
        self.outputdir = outputdir
        super().__init__(run_options, **kwargs)

    def setup(self):
        self.setup_proxyfile()
        # Setup dask general options from parameters/dask_env.py
        import dask.config
        from distributed import Client
        from pocket_coffea.parameters.dask_env import setup_dask
        setup_dask(dask.config)

        try:
            from dask_kubernetes.operator import KubeCluster
        except ImportError as exc:
            raise Exception(
                "Executor dask@kubernetes requires the dask-kubernetes package "
                "and the Dask operator installed in the cluster "
                "(https://kubernetes.dask.org/en/latest/operator.html). "
                "Install the client with `pip install pocket_coffea[kubernetes]` "
                "or `pip install dask-kubernetes`. "
                f"Import error: {exc}"
            ) from exc

        worker_image = self.run_options.get("worker-image", None)
        if not worker_image:
            raise Exception(
                "dask@kubernetes needs a `worker-image` run option: a registry "
                "image (not a cvmfs-unpacked path) with the same software "
                "environment as the submit host."
            )

        cores_per_worker = int(self.run_options.get("cores-per-worker", 1))
        memory = _normalize_memory(self.run_options.get("mem-per-worker", "2GiB"), "2Gi")
        scaleout = int(self.run_options.get("scaleout", 1))

        # Environment for the workers. The voms proxy is only usable if its
        # path is readable inside the worker pods (shared mount) -- same
        # contract as the k8sjobs executor below.
        env = {
            "XRD_RUNFORKHANDLER": "1",
            "MALLOC_TRIM_THRESHOLD_": "0",
        }
        if not self.run_options.get("ignore-grid-certificate", False):
            env["X509_USER_PROXY"] = self.x509_path

        cluster_kwargs = {
            "name": _sanitize_k8s_name(self.run_options.get("cluster-name", "pocket-coffea-dask")),
            "image": worker_image,
            "n_workers": scaleout,
            "resources": {
                "requests": {"cpu": str(cores_per_worker), "memory": memory},
                "limits": {"memory": memory},
            },
            "env": env,
        }
        namespace = self.run_options.get("namespace", None)
        if namespace:
            cluster_kwargs["namespace"] = namespace

        print(f">> Creating dask-kubernetes cluster {cluster_kwargs['name']} "
              f"(image={worker_image}, workers={scaleout})")
        self.dask_cluster = KubeCluster(**cluster_kwargs)

        if self.run_options.get("adaptive", False):
            self.dask_cluster.adapt(minimum=1, maximum=scaleout)

        self.dask_client = Client(self.dask_cluster)
        print(">> Waiting for the first worker...")
        self.dask_client.wait_for_workers(1)
        dashboard_link = getattr(self.dask_cluster, "dashboard_link", None)
        if dashboard_link:
            print(f">> You can connect to the Dask viewer at {dashboard_link}")

    def get(self):
        return coffea_processor.dask_executor(**self.customized_args())

    def customized_args(self):
        args = super().customized_args()
        args["client"] = self.dask_client
        args["treereduction"] = self.run_options.get("tree-reduction", True)
        args["retries"] = self.run_options.get("retries", 1)
        return args

    def close(self):
        self.dask_client.close()
        self.dask_cluster.close()


# --------------------------------------------------------------------
# Manual jobs executor: one kubernetes Job per chunk-group

class ExecutorFactoryKubernetesJobs(ExecutorFactoryManualABC):
    '''Manual-jobs executor submitting kubernetes ``batch/v1`` Jobs.

    The whole submission is a single Indexed Job (``jobs_all.yaml``) whose pod
    index plays the role of HTCondor's ``$(ProcId)``; per-job manifests
    (``job_{i}.yaml``) with an explicit ``POCKET_JOB_ID`` env var are written for
    resubmission through ``--recreate-jobs``. Job state uses the same
    ``job_{i}.idle/.running/.done/.failed`` flag files as the condor executors,
    and the pods mirror their stdout/stderr to the shared
    ``logs/job_<timestamp>.<i>.{out,err}`` (condor-style naming, the timestamp
    playing the ClusterId's role of separating attempts) so the shared
    log-scanning machinery of check-jobs / --recreate-jobs works unchanged.
    '''

    def get(self):
        pass

    def _kubectl(self, verb_and_args, raise_on_error=False):
        '''Build and run a kubectl command honoring the namespace/context run options.'''
        cmd = "kubectl"
        context = self.run_options.get("kubernetes-context", None)
        if context:
            cmd += f" --context {context}"
        namespace = self.run_options.get("namespace", None)
        if namespace:
            cmd += f" -n {namespace}"
        rc = _run_cmd(f"{cmd} {verb_and_args}")
        if rc != 0 and raise_on_error:
            raise Exception(f"kubectl command failed with exit code {rc}: {cmd} {verb_and_args}")
        return rc

    def k8s_job_name(self, index=None):
        base = _sanitize_k8s_name(f"pocket-coffea-{self.job_name}")
        if index is None:
            return base
        # leave room for the "-NNN" suffix within the 63-char limit
        return f"{base[:58].rstrip('-')}-{index}"

    def prepare_jobs(self, splits):
        config_files = []
        jobs_config = {
            "job_name": self.job_name,
            "job_dir": os.path.abspath(self.jobs_dir),
            "output_dir": os.path.abspath(self.outputdir),
            "split_by_category": self.run_options.get("split-by-category", False),
            "config_pkl_total": f"{os.path.abspath(self.outputdir)}/configurator.pkl",
            "jobs_list": {}
        }
        # Disabling the postprocessing
        self.config.do_postprocessing = False
        # Splitting the filesets creating a new configuration for each and pickling it
        for i, split in enumerate(splits):
            partial_config = self.config.clone()
            partial_config.set_filesets_manually(split)
            cloudpickle.dump(partial_config, open(f"{self.jobs_dir}/config_job_{i}.pkl", "wb"))
            config_files.append(f"{self.jobs_dir}/config_job_{i}.pkl")
            jobs_config["jobs_list"][f"job_{i}"] = {
                "filesets": split,
                "config_file": f"{self.jobs_dir}/config_job_{i}.pkl",
                "output_file": f"{os.path.abspath(self.outputdir)}/output_job_{i}.coffea",
            }

        yaml.dump(jobs_config, open(f"{self.jobs_dir}/jobs_config.yaml", "w"))
        return config_files

    def _job_manifest(self, n_jobs, job_index=None):
        '''Build the ``batch/v1`` Job manifest as a dict.

        With ``job_index=None`` an Indexed Job covering all ``n_jobs`` splits is
        produced; with an explicit index, a single-completion Job for that split
        (used for resubmission).
        '''
        abs_jobdir_path = os.path.abspath(self.jobs_dir)

        cores_per_worker = int(self.run_options.get("cores-per-worker", 1))
        memory = _normalize_memory(self.run_options.get("mem-per-worker", "2GiB"), "2Gi")

        container = {
            "name": "pocket-coffea",
            "image": self.run_options["worker-image"],
            "command": ["bash", f"{abs_jobdir_path}/job.sh"],
            "resources": {
                "requests": {"cpu": str(cores_per_worker), "memory": memory},
                "limits": {"memory": memory},
            },
        }
        if job_index is not None:
            container["env"] = [{"name": "POCKET_JOB_ID", "value": str(job_index)}]

        pod_spec = {
            "restartPolicy": "Never",
            "containers": [container],
        }
        service_account = self.run_options.get("service-account", None)
        if service_account:
            pod_spec["serviceAccountName"] = service_account
        node_selector = self.run_options.get("node-selector", None)
        if node_selector:
            pod_spec["nodeSelector"] = dict(node_selector)

        pvc_name = self.run_options.get("pvc-name", None)
        if pvc_name:
            mount_path = self.run_options.get("pvc-mount-path", None)
            if not mount_path:
                raise Exception(
                    "k8sjobs@kubernetes: `pvc-name` is set but `pvc-mount-path` is not. "
                    "The PVC must be mounted at the path under which the jobs dir and "
                    "output dir live, so the pods see them at the same absolute paths "
                    "as the submit host."
                )
            pod_spec["volumes"] = [
                {"name": "pocket-coffea-data",
                 "persistentVolumeClaim": {"claimName": pvc_name}}
            ]
            container["volumeMounts"] = [
                {"name": "pocket-coffea-data", "mountPath": mount_path}
            ]

        backoff_limit = int(self.run_options.get("backoff-limit", 0))
        job_spec = {
            "template": {
                "metadata": {"labels": {"app": "pocket-coffea",
                                        "pocket-coffea/job-name": _sanitize_k8s_name(self.job_name)}},
                "spec": pod_spec,
            },
        }
        if job_index is None:
            job_spec["completions"] = n_jobs
            job_spec["completionMode"] = "Indexed"
            parallelism = self.run_options.get("parallelism", None)
            if parallelism is None:
                parallelism = self.run_options.get("scaleout", n_jobs)
            job_spec["parallelism"] = min(int(parallelism), n_jobs)
            # Per-index failure isolation (k8s >= 1.29): one failing chunk must
            # not abort the whole Job and kill the other indexes mid-processing,
            # which is what a Job-level backoffLimit would do. Failed indexes
            # are then resubmitted individually with --recreate-jobs, like the
            # condor executors.
            job_spec["backoffLimitPerIndex"] = backoff_limit
            job_spec["maxFailedIndexes"] = n_jobs
        else:
            job_spec["backoffLimit"] = backoff_limit
        ttl = self.run_options.get("ttl-seconds-after-finished", None)
        if ttl is not None:
            job_spec["ttlSecondsAfterFinished"] = int(ttl)
        deadline = self.run_options.get("job-deadline", None)
        if deadline is not None:
            job_spec["activeDeadlineSeconds"] = int(deadline)

        manifest = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": self.k8s_job_name(job_index),
                "labels": {"app": "pocket-coffea",
                           "pocket-coffea/job-name": _sanitize_k8s_name(self.job_name)},
            },
            "spec": job_spec,
        }
        namespace = self.run_options.get("namespace", None)
        if namespace:
            manifest["metadata"]["namespace"] = namespace
        return manifest

    def submit_jobs(self, jobs_config):
        '''Prepare the job script and manifests and submit the jobs to the cluster.'''
        if not self.run_options.get("worker-image", None):
            raise Exception(
                "k8sjobs@kubernetes needs a `worker-image` run option: a registry "
                "image (not a cvmfs-unpacked path) with the same software "
                "environment as the submit host."
            )

        abs_output_path = os.path.abspath(self.outputdir)
        abs_jobdir_path = os.path.abspath(self.jobs_dir)
        os.makedirs(f"{self.jobs_dir}/logs", exist_ok=True)

        # Ship the proxy inside the jobs dir so pods reach it through the
        # shared mount (the analog of condor's transfer_input_files).
        proxy_line = ""
        if not self.run_options.get("ignore-grid-certificate", False):
            proxy_in_jobdir = f"{abs_jobdir_path}/{os.path.basename(self.x509_path)}"
            if os.path.abspath(self.x509_path) != proxy_in_jobdir:
                os.system(f"cp {self.x509_path} {proxy_in_jobdir}")
            proxy_line = f'export X509_USER_PROXY={proxy_in_jobdir}'

        env_extras_list = [
            'export XRD_RUNFORKHANDLER=1',
            'export MALLOC_TRIM_THRESHOLD_=0',
        ]
        if proxy_line:
            env_extras_list.append(proxy_line)
        if self.run_options.get("custom-setup-commands", None):
            env_extras_list += list(self.run_options["custom-setup-commands"])
        env_extras = "\n".join(env_extras_list)

        copy_command = "cp"
        eos_prefix = self.run_options.get("eos-prefix", None)
        if eos_prefix and abs_output_path.startswith("/eos/"):
            abs_output_path = eos_prefix + abs_output_path
        if eos_prefix and abs_output_path.startswith(eos_prefix):
            copy_command = "xrdcp -f"

        # Handle columns
        columncommand = ""
        if len(self.config.columns) > 0 and "dump_columns_as_arrays_per_chunk" in self.config.workflow_options:
            column_out_dir = self.config.workflow_options["dump_columns_as_arrays_per_chunk"]
            if not os.path.isabs(column_out_dir) and not (eos_prefix and column_out_dir.startswith(eos_prefix)):
                columncommand = f'[ -d "{column_out_dir}" ] && run_with_retries "{copy_command} -r {column_out_dir} {abs_output_path}"'

        # Pods only have the worker image's environment: always use the
        # installed CLI entrypoint (a submit-host runner.py path would not
        # exist inside the container).
        runnercmd = "pocket-coffea run"

        if self.run_options.get("split-by-category", False):
            # Split in the pod-local output dir, then push each piece to the
            # shared output dir (mirrors the lxplus local-split pattern).
            splitcommands = f'''
    cd output
    split-output output_all.coffea -b category -o output.coffea
    if [ $? -ne 0 ]; then
        echo 'split-output failed'
        set_status failed
        exit 1
    fi
    rm output_all.coffea
    for f in *.coffea; do
        run_with_retries "{copy_command} $f {abs_output_path}/${{f%.coffea}}_job_$JOBID.coffea"
    done
    cd ..
'''
        else:
            splitcommands = f'run_with_retries "{copy_command} output/output_all.coffea {abs_output_path}/output_job_$JOBID.coffea"'

        # Dump the whitelisted inner run-options so user overrides (e.g.
        # skip-bad-files) reach the inner `pocket-coffea run` inside the pod.
        write_inner_run_options(self.jobs_dir, self.run_options)

        # Resolve per-job chunksize. Accepts a scalar (legacy) or a per-sample dict.
        chunksize_cfg = self.run_options['chunksize']
        self._validate_chunksize_keys(chunksize_cfg, self.filesets)
        per_job_chunksize = [self._resolve_chunksize_for_job(chunksize_cfg, split)
                             for split in self._splits]
        # Indexed pods share one template, so per-job chunksizes are resolved at
        # runtime from a line-indexed file (the analog of `queue chunksize from (...)`).
        with open(f"{self.jobs_dir}/chunksizes.txt", "w") as f:
            for cs in per_job_chunksize:
                f.write(f"{cs}\n")

        script = f"""#!/bin/bash
{env_extras}

JOBDIR={abs_jobdir_path}
# The pod index of the Indexed Job plays the role of condor's ProcId;
# resubmitted single jobs carry an explicit POCKET_JOB_ID instead.
JOBID=${{POCKET_JOB_ID:-$JOB_COMPLETION_INDEX}}
if [ -z "$JOBID" ]; then
    echo "Neither POCKET_JOB_ID nor JOB_COMPLETION_INDEX is set!"
    exit 1
fi

# Mirror stdout/stderr to the shared logs dir so check-jobs/--recreate-jobs can
# analyse them like the condor logs, while `kubectl logs` keeps working. Condor
# log naming job_<ClusterId>.<ProcId>.{{out,err}} is kept, with a timestamp as
# the ClusterId, so the shared log-scanning helpers work unchanged. Guarded:
# an unwritable logs dir must not kill the job via a dead tee pipe.
if [ -w "$JOBDIR/logs" ]; then
    LOGTAG=$(date +%s)
    exec > >(tee -a $JOBDIR/logs/job_$LOGTAG.$JOBID.out) 2> >(tee -a $JOBDIR/logs/job_$LOGTAG.$JOBID.err >&2)
fi

# Status-file bookkeeping (.running/.done/.failed/.idle) on the shared mount;
# guarded so a missing/read-only mount degrades gracefully.
set_status() {{  # $1 = status extension to create (running|done|failed)
    [ -w "$JOBDIR" ] || return 0
    rm -f $JOBDIR/job_$JOBID.running $JOBDIR/job_$JOBID.done $JOBDIR/job_$JOBID.failed
    touch $JOBDIR/job_$JOBID.$1
}}

run_with_retries() {{
    local cmd="$*"
    for i in {{1..10}}; do
        eval "$cmd" && return 0
        sleep 10
    done
    echo "$cmd failed after 10 attempts."
    set_status failed
    exit 1
}}

[ -w "$JOBDIR" ] && rm -f $JOBDIR/job_$JOBID.idle

echo "Starting job $JOBID"
set_status running

# Work in a pod-local scratch dir; inputs are read from the shared $JOBDIR.
WORKDIR=$(mktemp -d)
cd $WORKDIR
echo "Job CWD: $(pwd)"

CHUNKSIZE=$(sed -n "$((JOBID+1))p" $JOBDIR/chunksizes.txt)

{runnercmd} --cfg $JOBDIR/config_job_$JOBID.pkl -o output EXECUTOR --chunksize $CHUNKSIZE --custom-run-options $JOBDIR/{INNER_RUN_OPTIONS_FILENAME}
RUNNER_RC=$?
# Do things only if the job is successful
if [ $RUNNER_RC -eq 0 ]; then
    echo 'Job successful'
    {splitcommands}
    {columncommand}

    set_status done
else
    echo 'Job failed'
    set_status failed
fi
echo 'Done'
exit $RUNNER_RC
"""

        cores_per_worker = int(self.run_options.get("cores-per-worker", 1))
        if cores_per_worker > 1:
            script = script.replace("EXECUTOR", f"--executor futures --scaleout {cores_per_worker}")
        else:
            script = script.replace("EXECUTOR", "--executor iterative")

        with open(f"{self.jobs_dir}/job.sh", "w") as f:
            f.write(script)

        n_jobs = len(jobs_config)
        with open(f"{self.jobs_dir}/jobs_all.yaml", "w") as f:
            yaml.safe_dump(self._job_manifest(n_jobs), f, default_flow_style=False, sort_keys=False)
        # Per-job manifests for resubmission
        for i in range(n_jobs):
            with open(f"{self.jobs_dir}/job_{i}.yaml", "w") as f:
                yaml.safe_dump(self._job_manifest(n_jobs, job_index=i), f,
                               default_flow_style=False, sort_keys=False)
            # Let's also create a .idle file to indicate that the job is in idle
            with open(f"{self.jobs_dir}/job_{i}.idle", "w") as f:
                f.write("")

        dry_run = self.run_options.get("dry-run", False)
        if dry_run:
            print(f"Dry run, not submitting jobs. You can find all files: {abs_jobdir_path}")
            return
        else:
            print("Submitting jobs")
            # A Job object with this name may linger from a previous run (Jobs
            # are immutable once created, so a plain re-apply would either fail
            # or silently no-op on a completed Job): clear it first.
            self._kubectl(f"delete job {self.k8s_job_name()} --ignore-not-found")
            self._kubectl(f"apply -f {abs_jobdir_path}/jobs_all.yaml", raise_on_error=True)

    def recreate_jobs(self, jobs_to_recreate):
        # Read the jobs config
        if not os.path.exists(f"{self.jobs_dir}/jobs_config.yaml"):
            print("No jobs_config.yaml found. Exiting.")
            exit(1)
        with open(f"{self.jobs_dir}/jobs_config.yaml") as f:
            jobs_config = yaml.safe_load(f)

        # Refresh the inner run-options so recreate-time overrides (e.g.
        # --skip-bad-files) reach the resubmitted pods (job.sh reads the yaml
        # from the shared jobs dir, so no script patching is needed).
        write_inner_run_options(self.jobs_dir, self.run_options)

        failedjobs = [f.replace(".failed", "") for f in os.listdir(self.jobs_dir) if f.endswith(".failed")]
        runningjobs = [f.replace(".running", "") for f in os.listdir(self.jobs_dir) if f.endswith(".running")]
        idlejobs = [f.replace(".idle", "") for f in os.listdir(self.jobs_dir) if f.endswith(".idle")]

        if jobs_to_recreate == "auto":
            jobs_to_recreate = failedjobs + runningjobs + idlejobs
            if len(jobs_to_recreate) == 0:
                print(f"Could not automatically find *.failed/*.running/*.idle files in {self.jobs_dir}. All jobs probably succeeded! Exiting.")
                exit()
            jobs_to_recreate = ','.join(jobs_to_recreate)

        jobs_to_redo = []
        for j in jobs_to_recreate.split(","):
            if not j.startswith("job_"):
                jobs_to_redo.append(f"job_{j}")
            else:
                jobs_to_redo.append(j)
        print(f"Recreating jobs: {jobs_to_redo}")

        if self.run_options.get("recreate-queue", None):
            print("[recreate-jobs] WARNING: --recreate-queue is HTCondor-specific and is "
                  "ignored by k8sjobs@kubernetes; use the job-deadline run option instead.")

        # Parse blocklist-sites: accept comma-separated string or list, mirroring lxplus
        blocklist_raw = self.run_options.get("blocklist-sites", None) or []
        if isinstance(blocklist_raw, str):
            blocklist_sites = {s for s in blocklist_raw.split(",") if s}
        else:
            blocklist_sites = set(blocklist_raw)
        if blocklist_sites:
            print(f"Blocklisting sites at recreate time: {sorted(blocklist_sites)}")
        use_redirector = bool(self.run_options.get("use-redirector", False))

        # Scan the shared logs for input-file failures (XRootD error, corrupt
        # input data, ...) with the same machinery as the condor executors; the
        # tools are loaded lazily so plain resubmissions need no rucio.
        inputfailures, inputfailurelogs = {}, {}
        tools = None
        if use_redirector or blocklist_sites or os.path.isdir(f"{self.jobs_dir}/logs"):
            tools = _get_site_rewrite_tools()
            inputfailures, inputfailurelogs = tools["find_input_failure_jobs"](f"{self.jobs_dir}/logs")

        sitemap = None
        rucio_client = None
        if not use_redirector and (blocklist_sites or any(j in inputfailures for j in jobs_to_redo)):
            sitemap = tools["get_xrootd_sites_map"]()
            try:
                rucio_client = tools["get_rucio_client"]()
            except Exception as e:
                print(f"WARNING: could not open a rucio client ({e}); replica lookups will fail.")

        for job in jobs_to_redo:
            if job not in jobs_config["jobs_list"]:
                print(f"Job {job} not found in the list of jobs")
                continue

            current_fileset = jobs_config["jobs_list"][job]["filesets"]
            new_fileset = deepcopy(current_fileset)
            modified = False

            if use_redirector:
                new_fileset = tools["rewrite_fileset_to_redirector"](new_fileset)
                modified = True
            else:
                # Input-failure reactive rewrite (per-file alternative site)
                if job in inputfailures:
                    reasons = ", ".join(sorted(inputfailures[job]))
                    print(f"Replacing input files in {job} since it failed due to: {reasons}.")
                    for sample, dct in new_fileset.items():
                        dct['files'] = [
                            tools["find_other_file"](fl, sitemap, blocklist=blocklist_sites,
                                                     rucio_client=rucio_client)
                            for fl in dct['files']
                        ]
                    modified = True

                # Blocklist-driven rewrite: applied to every job, composes with the above
                if blocklist_sites:
                    new_fileset = tools["rewrite_fileset_blocklist"](new_fileset, sitemap,
                                                                    blocklist_sites,
                                                                    rucio_client=rucio_client)
                    modified = True

            if modified:
                # Update files in the configurator
                config = cloudpickle.load(open(f"{self.jobs_dir}/config_{job}.pkl", "rb"))
                config.set_filesets_manually(new_fileset)
                cloudpickle.dump(config, open(f"{self.jobs_dir}/config_{job}.pkl", "wb"))

            # Archive the analysed logs of this job so the next scan only sees
            # fresh attempts (mirrors the condor executors).
            if job in inputfailurelogs:
                backupdir = f"{self.jobs_dir}/logs/processed"
                os.makedirs(backupdir, exist_ok=True)
                for logpath in inputfailurelogs[job]:
                    os.system(f"mv {logpath} {backupdir}/")

            # Resubmit the job
            job_index = int(job.split("_")[-1])
            dry_run = self.run_options.get("dry-run", False)
            if dry_run:
                print(f"Dry run, not resubmitting {job}")
            else:
                if job in failedjobs:
                    os.system(f"rm {self.jobs_dir}/{job}.failed")
                elif job in runningjobs:
                    os.system(f"rm {self.jobs_dir}/{job}.running")
                os.system(f"touch {self.jobs_dir}/{job}.idle")
                # A finished kubernetes Job is immutable: delete any previous
                # instance before re-applying the per-job manifest.
                self._kubectl(f"delete job {self.k8s_job_name(job_index)} --ignore-not-found")
                self._kubectl(f"apply -f {os.path.abspath(self.jobs_dir)}/{job}.yaml", raise_on_error=True)
                print(f"Resubmitted {job}")


def get_executor_factory(executor_name, **kwargs):
    if executor_name == "iterative":
        return IterativeExecutorFactory(**kwargs)
    elif executor_name == "futures":
        return FuturesExecutorFactory(**kwargs)
    elif executor_name == "dask":
        return DaskExecutorFactory(**kwargs)
    elif executor_name == "k8sjobs":
        return ExecutorFactoryKubernetesJobs(**kwargs)
    else:
        raise Exception(
            f"Executor {executor_name}@kubernetes not supported! "
            "Use dask@kubernetes or k8sjobs@kubernetes."
        )
