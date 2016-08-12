/*
 * gridengine_compat
 *
 * SLURM SPANK plugin that implements some GridEngine compatibility
 * behaviors:
 *
 *   - Sets a per-job TMPDIR in the job environment and handles removal of the directory
 *     once the job exits
 *   - Sets some GridEngine per-job environment variables using their SLURM equivalents
 *     if the user requested with a flag to sbatch/salloc/run:
 *
 *        SGE_O_WORKDIR           = SLURM_SUBMIT_DIR
 *        JOB_ID                  = SLURM_ARRAY_JOB_ID or SLURM_JOB_ID
 *        JOB_NAME                = SLURM_JOB_NAME
 *        NHOSTS                  = SLURM_JOB_NUM_NODES
 *        NSLOTS                  = SLURM_JOB_CPUS_PER_NODE (evaluated and summed)
 *        TASK_ID                 = SLURM_ARRAY_TASK_ID
 *        SGE_TASK_FIRST          = SLURM_ARRAY_TASK_MIN
 *        SGE_TASK_LAST           = SLURM_ARRAY_TASK_MAX
 *        SGE_TASK_STEPSIZE       = SLURM_ARRAY_TASK_STEP
 *
 *   - Changes the job's working directory to the directory at the time of job
 *     submission if the user requested with a flag to sbatch/salloc/srun
 *
 */

#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fts.h>
#include <limits.h>
#include <string.h>

#include <slurm/spank.h>
#include <slurm/slurm.h>

/* 
 * All spank plugins must define this macro for the SLURM plugin loader.
 */
SPANK_PLUGIN(gridengine_compat, 1)

/*
 * Should the job environment be augmented by SGE_* versions of the SLURM
 * environment variables?
 */
static int should_add_sge_env = 0;

/*
 * Should the job start in the working directory from which it was
 * submitted?
 */
static int should_start_in_submit_wd = 0;

/*
 * @function _opt_add_sge_env
 *
 * Parse the --add-sge-env option.
 *
 */
static int _opt_add_sge_env(
  int         val,
  const char  *optarg,
  int         remote
)
{
  if ( ! remote ) {
    should_add_sge_env = 1;
    slurm_verbose("gridengine_compat:  will add SGE-style environment variables to job");
  } else {
    slurm_error("gridengine_compat: cannot use --add-sge-env option in this context");
    return (-1);
  }
  return (0);
}

/*
 * @function _opt_cwd
 *
 * Parse the --cwd option.
 *
 */
static int _opt_cwd(
  int         val,
  const char  *optarg,
  int         remote
)
{
  if ( ! remote ) {
    should_start_in_submit_wd = 1;
    slurm_verbose("gridengine_compat:  will set job working directory to that from which job was submitted");
  } else {
    slurm_error("gridengine_compat: cannot use --cwd option in this context");
    return (-1);
  }
  return (0);
}

/*
 * Options available to this spank plugin:
 */
struct spank_option spank_options[] =
{
    { "add-sge-env", NULL,
      "Add GridEngine equivalents of SLURM job environment variables.",
      0, 0, (spank_opt_cb_f) _opt_add_sge_env },
    { "cwd", NULL,
      "Start the job in the same working directory from which it was submitted.",
      0, 0, (spank_opt_cb_f) _opt_cwd },
    SPANK_OPTIONS_TABLE_END
};


int
_rmdir_recurse(
  const char      *path,
  uid_t           match_uid
)
{
  int             rc = 0;
  char            *path_argv[2] = { (char*)path, NULL };
  
  // FTS_NOCHDIR  - Avoid changing cwd, which could cause unexpected behavior
  //                in multithreaded programs
  // FTS_PHYSICAL - Don't follow symlinks. Prevents deletion of files outside
  //                of the specified directory
  // FTS_XDEV     - Don't cross filesystem boundaries
  FTS             *ftsPtr = fts_open(path_argv, FTS_NOCHDIR | FTS_PHYSICAL | FTS_XDEV, NULL);
  FTSENT          *ftsItem;

  if ( ! ftsPtr ) {
    slurm_error("gridengine_compat: _rmdir_recurse(): Failed to open file traversal context on %s: %s", path, strerror(errno));
    return (-1);
  }
  
  //
  // Read the room item -- should be a directory owned by match_uid:
  //
  if ( (ftsItem = fts_read(ftsPtr)) ) {
    switch ( ftsItem->fts_info ) {
      case FTS_NS:
      case FTS_DNR:
      case FTS_ERR: {
        slurm_verbose("gridengine_compat: _rmdir_recurse(%s): directory does not exist", path);
        break;
      }
      case FTS_D: {
        //
        // We're entering a directory -- exactly what we want!
        //
        if ( ftsItem->fts_statp->st_uid == match_uid ) {
          while ( (ftsItem = fts_read(ftsPtr)) ) {
            switch ( ftsItem->fts_info ) {
              case FTS_NS:
              case FTS_DNR:
              case FTS_ERR:
                slurm_error("gridengine_compat: _rmdir_recurse(): Error in fts_read(%s): %s\n", ftsItem->fts_accpath, strerror(ftsItem->fts_errno));
                rc = -1;
                break;

              case FTS_DC:
              case FTS_DOT:
              case FTS_NSOK:
                // Not reached unless FTS_LOGICAL, FTS_SEEDOT, or FTS_NOSTAT were
                // passed to fts_open()
                break;

              case FTS_D:
                // Do nothing. Need depth-first search, so directories are deleted
                // in FTS_DP
                break;

              case FTS_DP:
              case FTS_F:
              case FTS_SL:
              case FTS_SLNONE:
              case FTS_DEFAULT:
                if ( remove(ftsItem->fts_accpath) < 0 ) {
                  slurm_error("gridengine_compat: _rmdir_recurse(): Failed to remove %s: %s\n", ftsItem->fts_path, strerror(errno));
                  rc = -1;
                }
                break;
            }
          }
        } else {
          slurm_error("gridengine_compat: _rmdir_recurse(): Failed to remove %s: not owned by job user (%d != %d)\n", path, ftsItem->fts_statp->st_uid, match_uid);
          rc = -1;
        }
        break;
      }
    }
  }
  fts_close(ftsPtr);
  return rc;
}


/*
 * @function slurm_spank_local_user_init
 *
 * Set job-specific TMPDIR in environment.  For batch scripts the path
 * uses just the job id; for all others, the path uses the job id, a dot,
 * and the job step id.  The value of TMPDIR handed to us by SLURM is
 * the base path for the new TMPDIR; if SLURM doesn't hand us a TMPDIR
 * then we default to using /tmp as our base directory.
 *
 * This function does not actually create the directory; 
 *
 * (Called from srun after allocation before launch.)
 */
int
slurm_spank_local_user_init(
  spank_t       spank,
  int           ac,
  char          *argv[]
)
{
  uint32_t      job_id = 0;
  uint32_t      job_step_id = 0;
  const char    *tmpdir = NULL;
  const char    *tmpdir_format = NULL;
  int           newtmpdir_len = 0;
  
  /* Get the job id and step id: */
  if ( spank_get_item(spank, S_JOB_ID, &job_id) != ESPANK_SUCCESS ) {
    slurm_error("gridengine_compat: no job id associated with job??");
    return (-1);
  }
  if ( spank_get_item(spank, S_JOB_STEPID, &job_step_id) != ESPANK_SUCCESS ) {
    slurm_error("gridengine_compat: no step id associated with job %u??", job_id);
    return (-1);
  }
  
  /* Default to /tmp if SLURM doesn't have TMPDIR set in the environment: */
  if ( (tmpdir = getenv ("TMPDIR")) == NULL ) tmpdir = "/tmp";
  
  /* Decide which format the directory should use and determine string length: */
  if ( job_step_id == SLURM_BATCH_SCRIPT ) {
    tmpdir_format = "%s/%u";
  } else {
    tmpdir_format = "%s/%u.%u";
  }
  newtmpdir_len = snprintf(NULL, 0, tmpdir_format, tmpdir, job_id, job_step_id);
  
  /* If the snprintf() failed then we've got big problems: */
  if ( newtmpdir_len < 0 ) {
    slurm_error("gridengine_compat: Failure while creating new tmpdir path: %d", newtmpdir_len);
    return (-1);
  } else {
    char        newtmpdir[newtmpdir_len + 1];
    
    /* Create the TMPDIR string: */
    newtmpdir_len = snprintf(newtmpdir, sizeof(newtmpdir), tmpdir_format, tmpdir, job_id, job_step_id);
    if ( newtmpdir_len < 0 ) {
      slurm_error("gridengine_compat: Failure while creating new tmpdir path: %d", newtmpdir_len);
      return (-1);
    }
    
    /* Set the TMPDIR variable in the current environment: */
    if ( setenv("TMPDIR", newtmpdir, 1) < 0 ) {
      slurm_error("setenv(TMPDIR, \"%s\"): %m", newtmpdir);
      return (-1);
    }
    
    slurm_verbose("gridengine_compat: TMPDIR = %s", newtmpdir);
  }
  return (0);
}


/*
 * @function slurm_spank_task_init
 *
 * Set GridEngine versions of environment variables.
 *
 * (Called as job user after fork() and before execve().)
 */
int
slurm_spank_task_init(
  spank_t       spank_ctxt,
  int           argc,
  char*         *argv
)
{
  char          value[8192];
  
  if ( spank_remote(spank_ctxt) && should_start_in_submit_wd ) {
    if ( spank_getenv(spank_ctxt, "SLURM_SUBMIT_DIR", value, sizeof(value)) == ESPANK_SUCCESS && *value ) {
      if ( ! chdir(value) ) {
        slurm_error("gridengine_compat: slurm_spank_task_init: Unable to change working directory (%d: %s)", errno, strerror(errno));
        return (-1);
      }
    } else {
      slurm_error("gridengine_compat: slurm_spank_task_init: Unable to change working directory (SLURM_SUBMIT_DIR not set)");
      return (-1);
    }
  }
  
  if ( spank_remote(spank_ctxt) && should_add_sge_env ) {
    
    if ( spank_getenv(spank_ctxt, "SLURM_SUBMIT_DIR", value, sizeof(value)) == ESPANK_SUCCESS && *value ) spank_setenv(spank_ctxt, "SGE_O_WORKDIR", value, 1);
    
    if ( spank_getenv(spank_ctxt, "SLURM_ARRAY_JOB_ID", value, sizeof(value)) == ESPANK_SUCCESS && *value ) spank_setenv(spank_ctxt, "JOB_ID", value, 1);
    else if ( spank_getenv(spank_ctxt, "SLURM_JOB_ID", value, sizeof(value)) == ESPANK_SUCCESS && *value ) spank_setenv(spank_ctxt, "JOB_ID", value, 1);
    
    if ( spank_getenv(spank_ctxt, "SLURM_JOB_NAME", value, sizeof(value)) == ESPANK_SUCCESS && *value ) spank_setenv(spank_ctxt, "JOB_NAME", value, 1);
    
    if ( spank_getenv(spank_ctxt, "SLURM_JOB_NUM_NODES", value, sizeof(value)) == ESPANK_SUCCESS && *value ) {
      spank_setenv(spank_ctxt, "NHOSTS", value, 1);
    } else {
      spank_setenv(spank_ctxt, "NHOSTS", "1", 1);
    }
    
    /*
     * Hmm...how do we calculate NSLOTS for a job step?  Looks like the
     * best bet is SLURM_JOB_CPUS_PER_NODE, which is a comma-delimited list
     * of integers with optional repeat counts:
     *
     *    1(x2),2(x3) == 1,1,2,2,2
     *
     * the sequence matches that of node names that are presented in the
     * SLURM_JOB_NODELIST variable (as a set of SLURM hostname expressions).
     *
     */
    if ( (spank_getenv(spank_ctxt, "SLURM_JOB_CPUS_PER_NODE", value, sizeof(value)) == ESPANK_SUCCESS) && *value ) {
      unsigned  nslots = 0;
      char      *p = value;
      
      while ( *p ) {
        char    *e;
        long    n;
        
        if ( ((n = strtol(p, &e, 10)) > 0) && (e > p) ) {
          if ( e[0] == '(' && e[1] == 'x' ) {
            long    r;
            
            p = e + 2;
            if ( ((r = strtol(p, &e, 10)) > 0) && (e > p) ) {
              n *= r;
              p = ( e[0] == ')' ) ? (e + 1) : e;
            } else {
              slurm_error("gridengine_compat: slurm_spank_task_init: Unable to parse SLURM_JOB_CPUS_PER_NODE (at index %ld): %s", (p - value), value);
              break;
            }
          } else {
            p = e;
          }
          nslots += n;
          
          if ( *p == ',' ) {
            p++;
          } else {
            slurm_error("gridengine_compat: slurm_spank_task_init: Unable to parse SLURM_JOB_CPUS_PER_NODE (at index %ld): %s", (p - value), value);
            break;
          }
        } else {
          slurm_error("gridengine_compat: slurm_spank_task_init: Unable to parse SLURM_JOB_CPUS_PER_NODE (at index %ld): %s", (p - value), value);
          break;
        }
      }
      if ( (nslots > 0) && (snprintf(value, sizeof(value), "%u", nslots) > 0) ) {
        spank_setenv(spank_ctxt, "NSLOTS", value, 1);
      }
    }
    
    if ( spank_getenv(spank_ctxt, "SLURM_ARRAY_TASK_ID", value, sizeof(value)) == ESPANK_SUCCESS && *value ) spank_setenv(spank_ctxt, "TASK_ID", value, 1);
    
    if ( spank_getenv(spank_ctxt, "SLURM_ARRAY_TASK_MIN", value, sizeof(value)) == ESPANK_SUCCESS && *value ) spank_setenv(spank_ctxt, "SGE_TASK_FIRST", value, 1);
    
    if ( spank_getenv(spank_ctxt, "SLURM_ARRAY_TASK_MAX", value, sizeof(value)) == ESPANK_SUCCESS && *value ) spank_setenv(spank_ctxt, "SGE_TASK_LAST", value, 1);
    
    if ( spank_getenv(spank_ctxt, "SLURM_ARRAY_TASK_STEP", value, sizeof(value)) == ESPANK_SUCCESS && *value ) spank_setenv(spank_ctxt, "SGE_TASK_STEPSIZE", value, 1);
  }
  return (0);
}


/*
 * @function slurm_spank_exit
 *
 * Remove the job's TMPDIR to keep temporary volumes neat and tidy.
 *
 * (Called as root user after tasks have exited.)
 */
int
slurm_spank_exit(
  spank_t       spank_ctxt,
  int           ac,
  char          **av
)
{
  if ( spank_remote(spank_ctxt) ) {
    char        tmpdir[PATH_MAX];
    uid_t       jobUid = -1;
    
    if ( spank_getenv(spank_ctxt, "TMPDIR", tmpdir, sizeof(tmpdir)) != ESPANK_SUCCESS ) {
      slurm_error("gridengine_compat: Unable to remove TMPDIR at exit (no TMPDIR in job environment)");
      return (-1);
    }
    
    if (spank_get_item (spank_ctxt, S_JOB_UID, &jobUid) != ESPANK_SUCCESS) {
      slurm_error("gridengine_compat: Unable to remove TMPDIR at exit (failed to get job's user id)");
      return (-1);
    }
    
    if ( ! _rmdir_recurse(tmpdir, jobUid) ) {
      slurm_error("gridengine_compat: Unable to remove TMPDIR at exit (failure in _rmdir_recurse())");
      return (-1);
    }
    
    slurm_verbose("gridengine_compat: rm -rf %s", tmpdir);
  }
  return (0);
}
