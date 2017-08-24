# SLURM job submission plugin

Our job submission (and alteration) verifier for SLURM.

## Job Account

If an explicit job account was not provided by the user, then the gid at time of submission is used to set the account for the job.  If the gid is not within the workgroup range, the submission fails and the error message:

    Please choose a workgroup before submitting a job

is returned.

For job modifications, the account quite simply cannot be modified.

