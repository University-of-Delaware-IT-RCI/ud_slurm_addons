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
#include <ctype.h>

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
 * What's the base directory to use for temp files?
 */
static const char *base_tmpdir = NULL;

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
  should_add_sge_env = 1;
  slurm_verbose("gridengine_compat:  will add SGE-style environment variables to job");
  return ESPANK_SUCCESS;
}

/*
 * @function _opt_tmpdir
 *
 * Parse the --tmpdir=<path> option.
 *
 */
static int _opt_tmpdir(
  int         val,
  const char  *optarg,
  int         remote
)
{
  if ( *optarg != '/' ) {
    slurm_error("gridengine_compat:  invalid path to --tmpdir: %s", optarg);
    return ESPANK_BAD_ARG;
  }
  
  base_tmpdir = strdup(optarg);
  slurm_verbose("gridengine_compat:  will add SGE-style environment variables to job");
  return ESPANK_SUCCESS;
}

/*
 * Options available to this spank plugin:
 */
struct spank_option spank_options[] =
{
    { "add-sge-env", NULL,
      "Add GridEngine equivalents of SLURM job environment variables.",
      0, 0, (spank_opt_cb_f) _opt_add_sge_env },
    { "tmpdir", "<path>",
      "Use the given path as the base directory for temporary files.",
      1, 0, (spank_opt_cb_f) _opt_tmpdir },
    SPANK_OPTIONS_TABLE_END
};


const char*
_get_base_tmpdir()
{
  const char      *tmpdir;
  
  if ( base_tmpdir ) return base_tmpdir;
  return "/tmp";
}



int
_get_tmpdir(
  spank_t         spank,
  char            *outTmpDir,
  size_t          outTmpDirLen
)
{
  uint32_t      job_id = 0;
  uint32_t      job_step_id = 0;
  const char    *tmpdir = NULL;
  int           actual_len = 0;
  
  /* Get the job id and step id: */
  if ( spank_get_item(spank, S_JOB_ID, &job_id) != ESPANK_SUCCESS ) {
    slurm_error("gridengine_compat: no job id associated with job??");
    return (-1);
  }
  if ( spank_get_item(spank, S_JOB_STEPID, &job_step_id) != ESPANK_SUCCESS ) {
    slurm_error("gridengine_compat: no step id associated with job %u??", job_id);
    return (-1);
  }
  
  /* Retrieve the base temp directory: */
  tmpdir = _get_base_tmpdir();
  
  /* Decide which format the directory should use and determine string length: */
  if ( job_step_id == SLURM_BATCH_SCRIPT ) {
    actual_len = snprintf(outTmpDir, outTmpDirLen, "%s/%u", tmpdir, job_id);
  } else {
    actual_len = snprintf(outTmpDir, outTmpDirLen, "%s/%u/%u", tmpdir, job_id, job_step_id);
  }
  
  /* If the snprintf() failed then we've got big problems: */
  if ( (actual_len < 0) || (actual_len >= outTmpDirLen) ) {
    slurm_error("gridengine_compat: Failure while creating new tmpdir path: %d", actual_len);
    return (-1);
  } else {
    struct stat   finfo;
    
    /* Build the path, making sure each component exists: */
    strncpy(outTmpDir, tmpdir, outTmpDirLen);
    if ( (stat(outTmpDir, &finfo) == 0) && S_ISDIR(finfo.st_mode) ) {
      /* At the least we'll need the job directory: */
      actual_len = snprintf(outTmpDir, outTmpDirLen, "%s/%u", tmpdir, job_id);
      if ( stat(outTmpDir, &finfo) != 0 ) {
        if ( mkdir(outTmpDir, 0700) != 0 ) {
          slurm_error("gridengine_compat: failed creating job tmpdir: %s", outTmpDir);
          return (-1);
        }
        stat(outTmpDir, &finfo);
      }
      if ( ! S_ISDIR(finfo.st_mode) ) {
        slurm_error("gridengine_compat: job tmpdir is not a directory: %s", outTmpDir);
        return (-1);
      }
      
      /* If this isn't the batch portion of a job, worry about the step subdir: */
      if ( job_step_id != SLURM_BATCH_SCRIPT ) {
        actual_len = snprintf(outTmpDir, outTmpDirLen, "%s/%u/%u", tmpdir, job_id, job_step_id);
        if ( stat(outTmpDir, &finfo) != 0 ) {
          if ( mkdir(outTmpDir, 0700) != 0 ) {
            slurm_error("gridengine_compat: failed creating step tmpdir: %s", outTmpDir);
            return (-1);
          }
          stat(outTmpDir, &finfo);
        }
        if ( ! S_ISDIR(finfo.st_mode) ) {
          slurm_error("gridengine_compat: step tmpdir is not a directory: %s", outTmpDir);
          return (-1);
        }
      }
    } else {
      slurm_error("gridengine_compat: base tmpdir is not a directory: %s", tmpdir);
      return (-1);
    }
  }
  
  return actual_len;
}


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
 * @function slurm_spank_init
 *
 * In the ALLOCATOR context, the 'spank_options' don't get automatically
 * registered as they do under LOCAL and REMOTE.  So under that context
 * we explicitly register our cli options.
 *
 */
int
slurm_spank_init(
  spank_t       spank_ctxt,
  int           argc,
  char          *argv[]
)
{
  int                     rc = ESPANK_SUCCESS;
  int                     i;
  
  if ( spank_context() == S_CTX_ALLOCATOR ) {
    struct spank_option   *o = spank_options;
    
    while ( o->name && (rc == ESPANK_SUCCESS) ) rc = spank_option_register(spank_ctxt, o++);
  }
  
  for ( i = 0; i < argc; i++ ) {
    if ( strncmp("enable=", argv[i], 7) == 0 ) {
      const char          *optarg = argv[i] + 7;
      
      if ( isdigit(*optarg) ) {
        char              *e;
        long              v = strtol(optarg, &e, 10);
        
        if ( e > optarg && ! *e ) {
          if ( v ) should_add_sge_env = 1;
        } else {
          slurm_error("gridengine_compat: Ignoring invalid enable option: %s", argv[i]);
        }
      } else if ( ! strcasecmp(optarg, "y") || ! strcasecmp(optarg, "yes") || ! strcasecmp(optarg, "t") || ! strcasecmp(optarg, "true") ) {
        should_add_sge_env = 1;
      } else if ( strcasecmp(optarg, "n") && strcasecmp(optarg, "no") && strcasecmp(optarg, "f") && strcasecmp(optarg, "false") ) {
        slurm_error("gridengine_compat: Ignoring invalid enable option: %s", argv[i]);
      }
    }
    else if ( strncmp("tmpdir=", argv[i], 7) == 0 ) {
      const char          *optarg = argv[i] + 7;
      
      if ( *optarg == '/' ) {
        base_tmpdir = strdup(optarg);
      } else {
        slurm_error("gridengine_compat: base tmpdir must be an absolute path: %s", argv[i]);
      }
    }
    else {
      slurm_error("gridengine_compat: Invalid option: %s", argv[i]);
    }
  }
  
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
 * This function does not actually create the directory.
 *
 * (Called from srun after allocation before launch.)
 */
int
slurm_spank_local_user_init(
  spank_t       spank_ctxt,
  int           argc,
  char          *argv[]
)
{
  char          tmpdir[PATH_MAX];
  int           tmpdirlen = _get_tmpdir(spank_ctxt, tmpdir, sizeof(tmpdir));
  
  if ( tmpdirlen > 0 ) {
    if ( setenv("TMPDIR", tmpdir, 1) < 0 ) {
      slurm_error("setenv(TMPDIR, \"%s\"): %m", tmpdir);
      return (-1);
    }
    slurm_verbose("gridengine_compat: TMPDIR = %s", tmpdir);
  }
  return (0);
}


/*
 * @function slurm_spank_user_init
 *
 * Set job-specific TMPDIR in environment.  For batch scripts the path
 * uses just the job id; for all others, the path uses the job id, a dot,
 * and the job step id.  The value of TMPDIR handed to us by SLURM is
 * the base path for the new TMPDIR; if SLURM doesn't hand us a TMPDIR
 * then we default to using /tmp as our base directory.
 *
 * This function does not actually create the directory.
 *
 * (Called from slurmstepd after it starts.)
 */
int
slurm_spank_user_init(
  spank_t       spank_ctxt,
  int           argc,
  char          *argv[]
)
{
  char          tmpdir[PATH_MAX];
  int           tmpdirlen = _get_tmpdir(spank_ctxt, tmpdir, sizeof(tmpdir));
  
  if ( tmpdirlen > 0 ) {
    if ( spank_setenv(spank_ctxt, "TMPDIR", tmpdir, tmpdirlen) != ESPANK_SUCCESS ) {
      slurm_error("setenv(TMPDIR, \"%s\"): %m", tmpdir);
      return (-1);
    }
    slurm_verbose("gridengine_compat: TMPDIR = %s", tmpdir);
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
  int           did_set_nslots = 0;
  
  if ( spank_remote(spank_ctxt) && should_add_sge_env ) {
    
    if ( spank_getenv(spank_ctxt, "SLURM_CLUSTER_NAME", value, sizeof(value)) == ESPANK_SUCCESS && *value ) spank_setenv(spank_ctxt, "SGE_CLUSTER_NAME", value, 1);
    
    if ( spank_getenv(spank_ctxt, "SLURM_SUBMIT_DIR", value, sizeof(value)) == ESPANK_SUCCESS && *value ) spank_setenv(spank_ctxt, "SGE_O_WORKDIR", value, 1);
    
    if ( spank_getenv(spank_ctxt, "SLURM_SUBMIT_HOST", value, sizeof(value)) == ESPANK_SUCCESS && *value ) spank_setenv(spank_ctxt, "SGE_O_HOST", value, 1);
    
    if ( spank_getenv(spank_ctxt, "SLURM_ARRAY_JOB_ID", value, sizeof(value)) == ESPANK_SUCCESS && *value ) {
      spank_setenv(spank_ctxt, "JOB_ID", value, 1);
    
      if ( spank_getenv(spank_ctxt, "SLURM_ARRAY_TASK_ID", value, sizeof(value)) == ESPANK_SUCCESS && *value ) spank_setenv(spank_ctxt, "SGE_TASK_ID", value, 1);
      
      if ( spank_getenv(spank_ctxt, "SLURM_ARRAY_TASK_MIN", value, sizeof(value)) == ESPANK_SUCCESS && *value ) spank_setenv(spank_ctxt, "SGE_TASK_FIRST", value, 1);
      
      if ( spank_getenv(spank_ctxt, "SLURM_ARRAY_TASK_MAX", value, sizeof(value)) == ESPANK_SUCCESS && *value ) spank_setenv(spank_ctxt, "SGE_TASK_LAST", value, 1);
      
      if ( spank_getenv(spank_ctxt, "SLURM_ARRAY_TASK_STEP", value, sizeof(value)) == ESPANK_SUCCESS && *value ) spank_setenv(spank_ctxt, "SGE_TASK_STEPSIZE", value, 1);
    }
    else if ( spank_getenv(spank_ctxt, "SLURM_JOB_ID", value, sizeof(value)) == ESPANK_SUCCESS && *value ) {
      spank_setenv(spank_ctxt, "JOB_ID", value, 1);
    }
    
    if ( spank_getenv(spank_ctxt, "SLURM_JOB_NAME", value, sizeof(value)) == ESPANK_SUCCESS && *value ) {
      spank_setenv(spank_ctxt, "JOB_NAME", value, 1);
    }
    
    if ( spank_getenv(spank_ctxt, "SLURM_JOB_PARTITION", value, sizeof(value)) == ESPANK_SUCCESS && *value ) spank_setenv(spank_ctxt, "QUEUE", value, 1);
    spank_setenv(spank_ctxt, "NQUEUES", "1", 1);
    
    if ( spank_getenv(spank_ctxt, "SLURM_JOB_NUM_NODES", value, sizeof(value)) == ESPANK_SUCCESS && *value ) {
      spank_setenv(spank_ctxt, "NHOSTS", value, 1);
    } else {
      spank_setenv(spank_ctxt, "NHOSTS", "1", 1);
    }
    
    /*
     * We will not setup a PE_HOSTFILE, since doing so could cause tightly-integrated
     * MPI implementations (like Open MPI) to mistakenly think that we're actually
     * using Grid Engine.
     */
    
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
          if ( *p ) {
            if ( *p == ',' ) {
              p++;
            } else {
              slurm_error("gridengine_compat: slurm_spank_task_init: Unable to parse SLURM_JOB_CPUS_PER_NODE (at index %ld): %s", (p - value), value);
              break;
            }
          }
        } else {
          slurm_error("gridengine_compat: slurm_spank_task_init: Unable to parse SLURM_JOB_CPUS_PER_NODE (at index %ld): %s", (p - value), value);
          break;
        }
      }
      if ( (nslots > 0) && (snprintf(value, sizeof(value), "%u", nslots) > 0) ) {
        spank_setenv(spank_ctxt, "NSLOTS", value, 1);
        did_set_nslots = 1;
      }
    }
    if ( ! did_set_nslots ) {
      spank_setenv(spank_ctxt, "NSLOTS", "1", 1);
    }
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
    char            tmpdir[PATH_MAX];
    
    if ( spank_getenv(spank_ctxt, "TMPDIR", tmpdir, sizeof(tmpdir)) == ESPANK_SUCCESS ) {
      uid_t         jobUid = -1;
      struct stat   finfo;

      if (spank_get_item (spank_ctxt, S_JOB_UID, &jobUid) != ESPANK_SUCCESS) {
        slurm_error ("gridengine_compat: remote: unable to get job's user id");
        return (-1);
      }
      
      if ( (stat(tmpdir, &finfo) == 0) && S_ISDIR(finfo.st_mode) ) {
        if ( _rmdir_recurse(tmpdir, jobUid) != 0 ) {
          slurm_error("gridengine_compat: remote: Unable to remove TMPDIR at exit (failure in _rmdir_recurse(%s,%d))", tmpdir, jobUid);
          return (-1);
        }
        slurm_verbose("gridengine_compat: remote: rm -rf %s", tmpdir);
      } else {
        slurm_error("gridengine_compat: remote: failed stat check of %s (uid = %d, st_mode = %x, errno = %d)", tmpdir, jobUid, finfo.st_mode, errno);
      }
    }
  } else {
    const char      *tmpdir = getenv("TMPDIR");
    
    if ( tmpdir ) {
      uid_t         jobUid = geteuid();
      struct stat   finfo;
      
      /* We don't care if the directory doesn't exist anymore... */
      if ( stat(tmpdir, &finfo) == 0 ) {
        /*  ...but if it does, let's get rid of it. */
        if ( S_ISDIR(finfo.st_mode) ) {
          if ( _rmdir_recurse(tmpdir, jobUid) != 0 ) {
            slurm_error("gridengine_compat: local: Unable to remove TMPDIR at exit (failure in _rmdir_recurse(%s,%d))", tmpdir, jobUid);
            return (-1);
          }
          slurm_verbose("gridengine_compat: local: rm -rf %s", tmpdir);
        }
      }
    }
  }
  return (0);
}
