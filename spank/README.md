# SLURM SPANK plugins
Some SLURM customizations will be implemented by means of a SPANK plugin.

## gridengine_compat
In order to ease the transition from Grid Engine to SLURM, the `gridengine_compat` plugin mimics some of Grid Engine's functioality:

* The `--add-sge-env` flag to `sbatch/salloc/srun` requests that SGE equivalents to the SLURM job environment be set.  For example, `NHOSTS` and `NSLOTS` will be set accordingly.
* The `--cwd` flag to `sbatch/salloc/srun` forces the job to begin execution in the working directory from which the job was submitted.  Equivalent to the `-cwd` option to `qsub`.
* A per-job temporary directory will automatically be created by the plugin, with its path in the `TMPDIR` variable.  This directory will automatically be deleted when the job completes.

