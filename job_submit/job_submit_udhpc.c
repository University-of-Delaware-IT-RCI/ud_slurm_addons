#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <grp.h>
#include <ctype.h>

#include "slurm/slurm_errno.h"
#include "src/common/slurm_xlator.h"
#include "src/common/fd.h"
#include "src/slurmctld/locks.h"
#include "src/slurmctld/slurmctld.h"

/*
 * Plugin identity and versioning info:
 */
#ifndef PLUGIN_SUBTYPE
# define PLUGIN_SUBTYPE "udhpc"
#endif
const char plugin_name[]        = "Job submit UD HPC conventions plugin";
const char plugin_type[]        = "job_submit/" PLUGIN_SUBTYPE;
const uint32_t plugin_version   = SLURM_VERSION_NUMBER;


#ifndef UDHPC_BASE_GID
# define UDHPC_BASE_GID         500
#endif
const gid_t udhpc_base_gid = UDHPC_BASE_GID;

#ifndef UDHPC_MIN_MEM_MB
# define UDHPC_MIN_MEM_MB       1024
#endif
const uint64_t udhcp_min_mem_mb = UDHPC_MIN_MEM_MB;


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
 * The following arrays of strings provide a mapping between SGE and SLURM
 * tokens that are permissible in stdio paths:
 */
const char* job_submit_sge_path_pseudo_variables[] = {
                "$USER",
                "$JOB_ID",
                "$JOB_NAME",
                "$HOSTNAME",
                "$TASK_ID",
                NULL
              };
const char* job_submit_slurm_path_pseudo_variables[] = {
                "%u",
                "%A",
                "%x",
                "%N",
                "%a",
                NULL
              };

extern int
job_submit_sge_parse_file_path(
  const char              *s,
  char*                   *out_path,
  char                    **err_msg
)
{
  const char    *e = s, *p;
  bool          has_leading_colon;
  
retry:
  if ( *s == ':' ) {
    has_leading_colon = true;
    s++;
  } else {
    has_leading_colon = false;
  }
  p = s;
  while ( *e && (*e != '\n') && (*e != ',') ) {
    if ( ! has_leading_colon && (*e == ':') ) {
      /* SLURM doesn't have an analog to host-specific log file syntax. */
      e++;
      while ( *e && (*e != '\n') && (*e != ',') ) e++;
      p = NULL;
      break;
    }
    e++;
  }
  if ( p ) {
    /* Extract the path */
    char      *path = xstrndup(p, e - p);
    char      *path_end = path + strlen(path);
    char      *path_ptr;
    int       i = 0;
    
    debug3(PLUGIN_SUBTYPE ": found path \"%s\", performing token substitutions", path);
    
    /* Replace any pseudo environment variables that SGE allowed.  This is
     * relatively simple because all SLURM tokens are _shorter_ than their
     * SGE tokens, so no resizing of the string pointer is necessary.
     */
    while ( job_submit_sge_path_pseudo_variables[i] ) {
      size_t  orig_len = strlen(job_submit_sge_path_pseudo_variables[i]);
      size_t  rep_len = strlen(job_submit_slurm_path_pseudo_variables[i]);
      size_t  dlen = orig_len - rep_len;
      
      while ( (path_ptr = strstr(path, job_submit_sge_path_pseudo_variables[i])) ) {
        memcpy(path_ptr, job_submit_slurm_path_pseudo_variables[i], rep_len);
        if ( dlen > 0 ) {
          size_t     mv_len = 1 + (path_end - (path_ptr + orig_len));
          
          memcpy(path_ptr + rep_len, path_ptr + orig_len, mv_len);
          path_end -= dlen;
        }
      }
      debug3(PLUGIN_SUBTYPE ": token %d => \"%s\"", i, path);
      i++;
    }
    *out_path = path;
    return SLURM_SUCCESS;
  }
  if ( *e == ',' ) {
    e++;
    s = e;
    goto retry;
  }
  
  return SLURM_SUCCESS;
}


extern int
job_submit_sge_parser(
  struct job_descriptor   *job_desc,
  char                    **err_msg
)
{
  long                  line_no = 1;
  char                  *line = job_desc->script;
  size_t                line_len;
  bool                  should_join_stdout_stderr = true;
  bool                  is_set_stderr = ( job_desc->std_err ? true : false );
  
  while ( *line == '#' ) {
    line_len = 0;
    
    /* Find length of line: */
    while ( line[line_len] && line[line_len] != '\n' ) line_len++;
    
    if ( line_len > 0 ) {
      /* Check the line for a leading #$ sequence, indicating special SGE-aware
       * comment lines with CLI options on 'em:
       */
      if ( line[1] == '$' ) {
        char      *s = line + 2, *e;
        
        debug3(PLUGIN_SUBTYPE ": SGE option line found");
        
        /* Drop leading whitespace: */
        while ( (*s != '\n') && isspace(*s) ) s++;
        
        /* A flag? */
        if ( *s++ == '-' ) {
          /* Which flag? */
          
/*
 * -pe <name> #{-#}
 */
          if ( (strncmp(s, "pe", 2) == 0) && isspace(s[2]) ) {
            s += 2;
            
            /* We only apply these options if they haven't already
             * been chosen on the CLI.
             */
            if ( (job_desc->num_tasks == NO_VAL) ||
                 (job_desc->cpus_per_task == NO_VAL16) ||
                 (job_desc->min_cpus == 1) ||
                 (job_desc->max_cpus == NO_VAL) ||
                 (job_desc->min_nodes == NO_VAL) ||
                 (job_desc->max_nodes == NO_VAL) ||
                 (job_desc->ntasks_per_node == NO_VAL16) ||
                 (job_desc->ntasks_per_socket == NO_VAL16) ||
                 (job_desc->ntasks_per_core == NO_VAL16) ||
                 (job_desc->ntasks_per_board == 0) ||
                 (job_desc->pn_min_cpus == NO_VAL16)
              )
            {
              bool        is_checking_max = false;
              uint32_t    min_cpus = 1;
              uint32_t    max_cpus = NO_VAL;
              
              debug3(PLUGIN_SUBTYPE ": -pe option found");
              
              /* Skip whitespace: */
              while ( *s && (*s != '\n') && isspace(*s) ) s++;
              
              /* Skip the next word: */
              while ( *s && (*s != '\n') && ! isspace(*s) ) s++;
              
              /* Skip whitespace: */
              while ( *s && (*s != '\n') && isspace(*s) ) s++;
              
              /* If the next character is a hyphen, this is a range
               * with an implied starting value of 1:
               */
              if ( *s == '-' ) {
                s++;
                is_checking_max = true;
              }
              
              /* If the next word looks like a number, then proceed: */
              if ( *s && (*s != '\n') && isdigit(*s) ) {
                long        v;
                
next_number:
                v = strtol(s, &e, 10);
                if ( v && (e > s) ) {
                  /* Which value are we setting? */
                  if ( is_checking_max ) {
                    if ( v > 0 && v <= NO_VAL ) max_cpus = v;
                  } else {
                    if ( v > 0 && v <= NO_VAL ) min_cpus = v;
                    /* Now:  if the character we ended on is a dash, then this is probably a
                     * range and we should check the next word, too:
                     */
                    is_checking_max = true;
                    if ( *e++ == '-') {
                      if ( isdigit(*e) ) {
                        s = e;
                        goto next_number;
                      } else {
                        if ( err_msg ) {
                          *err_msg = xstrdup_printf("invalid slot count at line %ld of job script", line_no);
                        }
                        return SLURM_ERROR;
                      }
                    }
                  }
                } else {
                  if ( err_msg ) {
                    *err_msg = xstrdup_printf("invalid slot count at line %ld of job script", line_no);
                  }
                  return SLURM_ERROR;
                }
                if ( min_cpus <= max_cpus ) {
                  /* Set the total CPU count: */
                  job_desc->min_cpus = min_cpus;
                  job_desc->max_cpus = max_cpus;
                  info(PLUGIN_SUBTYPE ": cpu settings from -pe option => cpu range %u-%u", min_cpus, max_cpus);
                } else {
                  if ( err_msg ) {
                    *err_msg = xstrdup_printf("slot minimum (%u) > maximum (%u) at line %ld of job script", min_cpus, max_cpus, line_no);
                  }
                  return SLURM_ERROR;
                }
              }
            } else {
              info(PLUGIN_SUBTYPE ": ignoring -pe option, cpu settings specified elsewhere");
            }
          }
          
/*
 * -m b|e|a|s|n,...
 */
          else if ( (*s == 'm') && isspace(s[1]) ) {
            s += 1;
              
            debug3(PLUGIN_SUBTYPE ": -m option found");
            
            if ( job_desc->mail_type == 0 ) {
              uint16_t      modes = 0;
              
              /* Skip whitespace: */
              while ( *s && (*s != '\n') && isspace(*s) ) s++;
              
              /* Process the rest of the line: */
              while ( *s && (*s != '\n') && ! isspace(*s) ) {
                switch ( *s ) {
                  case 'b':
                    modes |= MAIL_JOB_BEGIN;
                    break;
                  case 'e':
                    modes |= MAIL_JOB_END;
                    break;
                  case 'a':
                    modes |= MAIL_JOB_FAIL;
                    break;
                  case 's':
                    modes |= MAIL_JOB_REQUEUE;
                    break;
                  case 'n':
                    modes = 0;
                    break;
                  case ',':
                    break;
                  default:
                    if ( err_msg ) {
                      *err_msg = xstrdup_printf("invalid mail option %c at line %ld of job script", *s, line_no);
                    }
                    return SLURM_ERROR;
                }
                s++;
              }
              if ( modes != job_desc->mail_type ) {
                job_desc->mail_type = modes;
                info(PLUGIN_SUBTYPE ": mail mode settings from -m option => 0x%04hx", modes);
              }
            } else {
              info(PLUGIN_SUBTYPE ": ignoring -m option, mail options specified elsewhere");
            }
          }
          
/*
 * -M <address>
 */
          else if ( (*s == 'M') && isspace(s[1]) ) {
            s += 1;
              
            debug3(PLUGIN_SUBTYPE ": -M option found");
            
            if ( ! job_desc->mail_user ) {
              size_t          addr_len;
              
              /* Skip whitespace: */
              while ( *s && (*s != '\n') && isspace(*s) ) s++;
            
              /* Isolate the valid characters: */
              addr_len = strcspn(s, "\n\r\t\v ");
              if ( addr_len > 0 ) {
                job_desc->mail_user = xstrndup(s, addr_len);
                info(PLUGIN_SUBTYPE ": email address \"%s\" from -M option", job_desc->mail_user);
              }
            } else {
              info(PLUGIN_SUBTYPE ": ignoring -M option, mail user specified elsewhere");
            }
          }
          
/*
 * -N <name>
 */
          else if ( (*s == 'N') && isspace(s[1]) ) {
            size_t      name_len;
            
            s += 1;
              
            debug3(PLUGIN_SUBTYPE ": -N option found");
            
            /* Skip whitespace: */
            while ( *s && (*s != '\n') && isspace(*s) ) s++;
            
            /* Isolate the valid characters: */
            name_len = strcspn(s, "\n\t\r/:@\\*?");
            if ( name_len > 0 ) {
              /* We'll drop the SGE name into the SLURM job name if none
               * is present.  Otherwise, use the comment field:
               */
              if ( (job_desc->name == NULL) || ! job_desc->name[0] ) {
                /* Drop what was there before: */
                if ( job_desc->name ) xfree(job_desc->name);
                
                job_desc->name = xstrndup(s, name_len);
                info(PLUGIN_SUBTYPE ": name \"%s\" from -N option", job_desc->name);
              }
              else if ( (job_desc->comment == NULL) || ! job_desc->comment[0] ) {
                /* Drop what was there before: */
                if ( job_desc->comment ) xfree(job_desc->comment);
                
                job_desc->comment = xstrndup(s, name_len);
                info(PLUGIN_SUBTYPE ": comment \"%s\" from -N option", job_desc->comment);
              }
              else {
                info(PLUGIN_SUBTYPE ": ignoring -N option, name and comment specified elsewhere");
              }
            }
          }
          
/*
 * -{o|e|i} <path>
 */
          else if ( (*s == 'o' || *s == 'e' || *s == 'i') && isspace(s[1]) ) {
            char        *path = NULL;
            char        variant[2] = { *s, '\0' };
            
            s += 1;
              
            debug3(PLUGIN_SUBTYPE ": -%s option found", variant);
            
            /* Skip whitespace: */
            while ( *s && (*s != '\n') && isspace(*s) ) s++;
            
            /* Try to get a path: */
            if ( job_submit_sge_parse_file_path(s, &path, err_msg) != SLURM_SUCCESS ) {
              return SLURM_ERROR;
            }
            if ( path ) {
              switch ( variant[0] ) {
              
                case 'o':
                  if ( job_desc->std_out ) xfree(job_desc->std_out);
                  job_desc->std_out = path;
                  break;
              
                case 'e':
                  if ( job_desc->std_err ) xfree(job_desc->std_err);
                  job_desc->std_err = path;
                  is_set_stderr = true;
                  break;
              
                case 'i':
                  if ( job_desc->std_in ) xfree(job_desc->std_in);
                  job_desc->std_in = path;
                  break;
                  
              }
              
              info(PLUGIN_SUBTYPE ": stdio path \"%s\" from -%s option", path, variant);
            }
          }
          
/*
 * -j y[es]|n[o]
 */
          else if ( (*s == 'j') && isspace(s[1]) ) {
            s += 1;
              
            debug3(PLUGIN_SUBTYPE ": -j option found");
            
            /* Skip whitespace: */
            while ( *s && (*s != '\n') && isspace(*s) ) s++;
            
            if ( *s == 'y') {
              if ( ! isspace(s[1]) && ((s[1] != 'e') || (s[2] != 's') || ! isspace(s[3])) ) {
                if ( err_msg ) {
                  *err_msg = xstrdup_printf("invalid -j argument at line %ld of job script", line_no);
                }
                return SLURM_ERROR;
              }
              should_join_stdout_stderr = true;
            }
            else if ( *s == 'n' ) {
              if ( ! isspace(s[1]) && ((s[1] != 'o') || ! isspace(s[2])) ) {
                if ( err_msg ) {
                  *err_msg = xstrdup_printf("invalid -j argument at line %ld of job script", line_no);
                }
                return SLURM_ERROR;
              }
              should_join_stdout_stderr = false;
            } else {
              if ( err_msg ) {
                *err_msg = xstrdup_printf("invalid -j argument at line %ld of job script", line_no);
              }
              return SLURM_ERROR;
            }
          }
          
/*
 * -q <wc_queue_list>
 */
          else if ( (*s == 'q') && isspace(s[1]) ) {
            s += 1;
              
            debug3(PLUGIN_SUBTYPE ": -q option found");
            
            if ( ! job_desc->partition || ! *(job_desc->partition) ) {
              char          *partition_list = NULL;
              
              /* Skip whitespace: */
              while ( *s && (*s != '\n') && isspace(*s) ) s++;
            
              /* Start grabbing queue names: */
              while ( *s && (*s != '\n') ) {
                size_t      qname_len = strcspn(s, "\n\t\r@, ");
                
                if ( qname_len > 0 ) {
                  if ( partition_list ) xstrcatchar(partition_list, ',');
                  xstrncat(partition_list, s, qname_len);
                }
                s += qname_len;
                if ( *s == '@' ) {
                  /* Skip over this part, we're not going to support host restrictions */
                  s++;
                  while ( *s && ! isspace(*s) && (*s != ',') ) s++;
                }
                qname_len = strspn(s, "\t\r, ");
                s += qname_len;
              }
              if ( partition_list ) {
                job_desc->partition = partition_list;
                info(PLUGIN_SUBTYPE ": partition list \"%s\" from -q option", partition_list);
              }
            } else {
              info(PLUGIN_SUBTYPE ": ignoring -q option, partition list specified elsewhere");
            }
          }
          
/*
 * -l <complex>{=<value>}{,<complex>{=<value>}{,..}}
 */
          else if ( (*s == 'l') && isspace(s[1]) ) {
            s += 1;
              
            debug3(PLUGIN_SUBTYPE ": -l option found");
            
            /* Skip whitespace: */
            while ( *s && (*s != '\n') && isspace(*s) ) s++;
            
          }
        }
      }
      
      line_no++;
    } else {
      break;
    }
    line = line + (line_len + ((line[line_len] == '\n') ? 1 : 0));
  }
  
  /* If requested, set stderr to match the path provided for stdout.  By default,
   * both of these paths will be NULL and the implied behavior in SLURM is that
   * they'll both go the the default file, e.g. slurm-<jobid>.out
   *
   * The SGE behavior is such that -j y only works when no explicit stderr path
   * has been provided, so we mimic that.    But that's just SLURM's default
   * behavior anyway.  The only case we actually have to handle is -j n and std_err
   * is NOT set:
   */
  if ( ! should_join_stdout_stderr && ! is_set_stderr ) {
    if ( job_desc->std_out ) {
      size_t      std_out_len = strlen(job_desc->std_out);
      
      if ( (std_out_len > 4) && (strcmp(job_desc->std_out + std_out_len - 4, ".out") == 0) ) {
        char      *base = xstrndup(job_desc->std_out, std_out_len - 4);
        job_desc->std_err = xstrdup_printf("%s.err", base);
        xfree(base);
      } else {
        job_desc->std_err = xstrdup_printf("%s.err", job_desc->std_out);
      }
    } else {
      job_desc->std_err = xstrdup("slurm-%j.err");
    }
    info(PLUGIN_SUBTYPE ": stderr set to path \"%s\"", job_desc->std_err);
  }
  
  return SLURM_SUCCESS;
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
  /* First and foremost, let's look through the job script
   * (if present) and for SGE compatibility check for
   * directives:
   */
  if ( job_desc->script && *(job_desc->script) ) {
    /* Must start with hash-bang: */
    if ( strncmp(job_desc->script, "#!", 2) == 0 ) {
      debug(PLUGIN_SUBTYPE ": checking for SGE flags in script");
      if ( job_submit_sge_parser(job_desc, err_msg) != SLURM_SUCCESS ) {
        return SLURM_ERROR;
      }
    }
  }


  /* Memory limit _must_ be set: */
  if ( (job_desc->pn_min_memory <= 0) || (job_desc->pn_min_memory == NO_VAL64) ) {
    job_desc->pn_min_memory = udhcp_min_mem_mb;
    info(PLUGIN_SUBTYPE ": setting default memory limit (%lu MiB)", udhcp_min_mem_mb);
  }
  
  /* Set the job account to match the submission group: */
  if ( ! job_desc->account ) {
    gid_t                 submit_gid = job_desc->group_id;
    
    if ( submit_gid >= udhpc_base_gid ) {
      /* Resolve gid to name: */
      char                *submit_gname = job_submit_udhpc_getgrgid(submit_gid);
      
      if ( submit_gname ) {
        job_desc->account = submit_gname;
        info(PLUGIN_SUBTYPE ": setting job account to %s (%u)", submit_gname, submit_gid);
      } else {
        info(PLUGIN_SUBTYPE ": unable to resolve job submission gid %u; job account not set", submit_gid);
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
    info(PLUGIN_SUBTYPE ": job account cannot be modified after submission");
    return SLURM_ERROR;
  }
  return SLURM_SUCCESS;
}
