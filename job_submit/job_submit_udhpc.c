#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <grp.h>

#include "slurm/slurm_errno.h"
#include "src/common/slurm_xlator.h"
#include "src/slurmctld/locks.h"
#include "src/slurmctld/slurmctld.h"

/*
 * Plugin identity and versioning info:
 */
const char plugin_name[]        = "Job submit UD HPC conventions plugin";
const char plugin_type[]        = "job_submit/udhpc";
const uint32_t plugin_version   = SLURM_VERSION_NUMBER;


#ifndef UDHPC_BASE_GID
# define UDHPC_BASE_GID         1001
#endif
const gid_t udhpc_base_gid = UDHPC_BASE_GID;


char*
job_submit_udhpc_getgrgid(
  gid_t           the_gid
)
{
  struct group    group_info, *group_info_ptr = NULL;
  char            *out_gname = NULL;
  
#ifdef _SC_GETGR_R_SIZE_MAX
  size_t          gid_str_buffer_len = sysconf(_SC_GETGR_R_SIZE_MAX);
#else
  size_t          gid_str_buffer_len = 1024;
#endif
  char            *gid_str_buffer = xmalloc_nz(gid_str_buffer_len);
  
  if ( gid_str_buffer ) {
    while ( 1 ) {
      if ( getgrgid_r(the_gid, &group_info, gid_str_buffer, gid_str_buffer_len, &group_info_ptr) != 0 ) {
        /* Failure -- errno will tell us what went wrong: */
        if ( (errno == ERANGE) && (gid_str_buffer_len < 64 * 1024) ) {
          /* Resize the buffer and try again: */
          char    *resized_buffer = xrealloc_nz(gid_str_buffer, gid_str_buffer_len + 1024);
          
          if ( resized_buffer ) {
            gid_str_buffer = resized_buffer;
            gid_str_buffer_len += 1024;
            continue;
          }
        }
      } else {
        /* Success! */
        out_gname = xstrdup(group_info.gr_name);
      }
      break;
    }
    xfree(gid_str_buffer);
  }
  return out_gname;
}


/*
 * @function job_submit
 *
 */
extern int
job_submit(
  struct job_descriptor   *job_desc,
  uint32_t                submit_uid,
  char                    **err_msg
)
{
  
  
  /* Set the job account to match the submission group: */
  if ( ! job_desc->account ) {
    gid_t                 submit_gid = job_desc->group_id;
    
    if ( submit_gid >= udhpc_base_gid ) {
      /* Resolve gid to name: */
      char                *submit_gname = job_submit_udhpc_getgrgid(submit_gid);
      
      if ( submit_gname ) {
        job_desc->account = submit_gname;
        info("Setting job account to %s (%u)", submit_gname, submit_gid);
      } else {
        info("Unable to resolve job submission gid %u; job account not set", submit_gid);
        if ( err_msg ) {
          *err_msg = xstrdup_printf("Unable to resolve job submission gid %u", submit_gid);
        }
        return SLURM_ERROR;
      }
    }
    else if ( submit_uid != 0 ) {
      /* User cannot submit a job unless they're in a valid workgroup: */
      if ( err_msg ) {
        *err_msg = xstrdup("Please choose a workgroup before submitting a job");
      }
      return SLURM_ERROR;
    }
  }
  
  return SLURM_SUCCESS;
}

extern int
job_modify(
  struct job_descriptor   *job_desc,
  struct job_record       *job_ptr,
  uint32_t                submit_uid
)
{
  /* We do not allow the job account to change: */
  if ( job_desc->account && (! job_ptr->account || xstrcasecmp(job_desc->account, job_ptr->account)) ) {
    info("Job account cannot be modified after submission");
    return SLURM_ERROR;
  }
  return SLURM_SUCCESS;
}
