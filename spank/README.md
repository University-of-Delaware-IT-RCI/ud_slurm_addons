# SLURM SPANK plugins
Some SLURM customizations will be implemented by means of a SPANK plugin.

## gridengine_compat
In order to ease the transition from Grid Engine to SLURM, the `gridengine_compat` plugin mimics some of Grid Engine's functioality:

* The `--add-sge-env` flag to `sbatch/salloc` requests that SGE equivalents to the SLURM job environment variables be set (if they exist):
    * `SGE_CLUSTER_NAME` is set to the value of `SLURM_CLUSTER_NAME`
    * `SGE_O_HOST` is set to the hostname on which the job was submitted (`SLURM_SUBMIT_HOST`)
    * `SGE_O_WORKDIR` is set to the working directory when the job was submitted (`SLURM_SUBMIT_DIR`)
    * `JOB_ID` is set to the value of `SLURM_ARRAY_JOB_ID` (or `SLURM_JOB_ID` if the former is not set)
    * `JOB_NAME` is set to the value of `SLURM_JOB_NAME` (or "STDIN" if `SLURM_JOB_NAME` is unset or empty)
    * `NHOSTS` is set to the value of `SLURM_JOB_NUM_NODES` (defaults to "1")
    * `NSLOTS` is set to the number of CPUs assigned to the job (defaults to "1")
    * `NQUEUES` is set to "1" and `QUEUE` is set to the value of `SLURM_JOB_PARTITION`
    * If the job is an array job, then the following are also set:
        * `SGE_TASK_ID` is set to the value of `SLURM_ARRAY_TASK_ID`
        * `SGE_TASK_FIRST` is set to the value of `SLURM_ARRAY_TASK_MIN`
        * `SGE_TASK_LAST` is set to the value of `SLURM_ARRAY_TASK_MAX`
        * `SGE_TASK_STEPSIZE` is set to the value of `SLURM_ARRAY_TASK_STEP`
* A per-job temporary directory will automatically be created by the plugin for batch jobs, with its path in the `TMPDIR` variable.  This directory will automatically be deleted when the job completes.

