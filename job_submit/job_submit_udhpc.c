#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <grp.h>
#include <ctype.h>
#include <math.h>

#include "slurm/slurm_errno.h"
#include "src/common/slurm_xlator.h"
#include "src/common/fd.h"
#include "src/common/env.h"
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
const uint64_t udhpc_min_mem_mb = UDHPC_MIN_MEM_MB;


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


extern int
job_submit_strncasecmp(
  const char    *s1,
  const char    *s2,
  size_t        l
)
{
  while ( l-- ) {
    if ( *s1 && *s2 ) {
      if (tolower(*s1) != tolower(*s2)) return (tolower(*s2) - tolower(*s1));
      s1++; s2++;
    } else {
      if ( *s1 != *s2 ) return (tolower(*s2) - tolower(*s1));
      break;
    }
  }
  return 0;
}


static inline int
job_submit_resource_name_equal(
  const char    *s,
  size_t        l,
  const char    *d
)
{
  return (job_submit_strncasecmp(s, d, l) == 0) ? 1 : 0;
}


static inline int
job_submit_resource_name_in_pair(
  const char    *s,
  size_t        l,
  const char    *d1,
  const char    *d2
)
{
  if ( job_submit_strncasecmp(s, d1, l) == 0 ) return 1;
  return (job_submit_strncasecmp(s, d2, l) == 0) ? 1 : 0;
}


static inline int
job_submit_resource_name_in_set(
  const char    *s,
  size_t        l,
  ...
)
{
  va_list       vargs;
  const char    *d;
  
  va_start(vargs, l);
  while ( (d = va_arg(vargs, const char*)) ) {
    if ( job_submit_strncasecmp(s, d, l) == 0 ) return 1;
  }
  va_end(vargs);
  return 0;
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
job_submit_sge_parse_memory(
  const char            *s,
  size_t                l,
  uint64_t              *megabytes
)
{
  if ( l > 0 ) {
    char                S[l + 1];
    char                *e;
    long long           v;
    
    memcpy(S, s, l);
    S[l] = '\0';
    s = &S[0];
    
    debug3(PLUGIN_SUBTYPE ": parse memory: [%s]", S);
    
    /* Attempt to scan an integer value: */
    v = strtoll(s, &e, 0);
    if ( (v >= 0) && (e > s) ) {
      switch ( *e ) {
        case 'G':
          v *= 1024;
        case 'M':
          v *= 1024;
        case 'K':
          v *= 1024;
          break;
          
        case 'g':
          v *= 1000;
        case 'm':
          v *= 1000;
        case 'k':
          v *= 1000;
          break;
      
      }
      /* Convert to MiB: */
      v = (v / 1048576) + ((v %1048576) ? 1 : 0);
      
      if ( (v > 0) && (v < udhpc_min_mem_mb) ) v = udhpc_min_mem_mb;
      *megabytes = v;
      return 1;
    }
  }
  return 0;
}


extern int
job_submit_sge_parse_time(
  const char            *s,
  size_t                l,
  uint32_t              *minutes
)
{
  if ( l > 0 ) {
    char                S[l + 1];
    double              seconds;
    char                *e;
    long                v, t[3] = { 0, 0, 0 };
    
    memcpy(S, s, l);
    S[l] = '\0';
    s = &S[0];
    
    debug3(PLUGIN_SUBTYPE ": parse time: [%s]", S);
    
    /* Attempt to scan an integer value: */
    v = strtol(s, &e, 0);
    
    /* If *e is a colon, this is a full time spec: */
    if ( *e == ':' ) {
      t[0] = (e > s) ? v : 0;
      v = strtol((s = e + 1), &e, 0);
      if ( *e != ':' ) return 0;
      t[1] = (e > s) ? v : 0;
      v = strtol((s = e + 1), &e, 0);
      t[2] = (e > s) ? v : 0;
    } else {
      /* Seconds only: */
      t[2] = (e > s) ? v : 0;
    }
    
    /* Total the seconds: */
    seconds = t[2] + 60 * (t[1] + 60 * t[0]);
    if ( seconds >= 0.0 ) {
      seconds = ceil(seconds / 60.0);
      if ( seconds < UINT32_MAX ) {
        debug3(PLUGIN_SUBTYPE ": => %f minutes", seconds);
        *minutes = (uint32_t)seconds;
        return 1;
      }
    }
  }
  return 0;
}


extern int
job_submit_sge_parse_int(
  const char            *s,
  size_t                l,
  long                  *value
)
{
  if ( l > 0 ) {
    char                S[l + 1];
    char                *e;
    long                v;
    
    memcpy(S, s, l);
    S[l] = '\0';
    s = &S[0];
    
    debug3(PLUGIN_SUBTYPE ": parse int: [%s]", S);
    
    /* Attempt to scan an integer value: */
    v = strtol(s, &e, 0);
    if ( e > s ) {
      *value = v;
      return 1;
    }
  }
  return 0;
}


typedef enum {
  kSGEBooleanNoValue = -1,
  kSGEBooleanFalse = 0,
  kSGEBooleanTrue = 1
} sge_bool_t;

extern int
job_submit_sge_parse_bool(
  const char            *s,
  size_t                l,
  sge_bool_t            *value
)
{
  if ( l > 0 ) {
    /* Allowed values are TRUE, FALSE, 1, 0: */
    switch ( *s ) {
    
      case 'T':
      case 't': {
        ++s, l--; if ( (l == 0) || ! *s || ((*s != 'r') && (*s != 'R')) ) return 0;
        ++s, l--; if ( (l == 0) || ! *s || ((*s != 'u') && (*s != 'U')) ) return 0;
        ++s, l--; if ( (l == 0) || ! *s || ((*s != 'e') && (*s != 'E')) ) return 0;
        if ( --l == 0 ) {
          *value = kSGEBooleanTrue;
          return 1;
        }
        break;
      }
    
      case 'F':
      case 'f': {
        ++s, l--; if ( (l == 0) || ! *s || ((*s != 'a') && (*s != 'A')) ) return 0;
        ++s, l--; if ( (l == 0) || ! *s || ((*s != 'l') && (*s != 'L')) ) return 0;
        ++s, l--; if ( (l == 0) || ! *s || ((*s != 's') && (*s != 'S')) ) return 0;
        ++s, l--; if ( (l == 0) || ! *s || ((*s != 'e') && (*s != 'E')) ) return 0;
        if ( --l == 0 ) {
          *value = kSGEBooleanFalse;
          return 1;
        }
        break;
      }
      
      case '1': {
        ++s; l--; if ( l > 0 ) return 0;
        *value = kSGEBooleanTrue;
        return 1;
      }
      
      case '0': {
        ++s; l--; if ( l > 0 ) return 0;
        *value = kSGEBooleanFalse;
        return 1;
      }
    }
  }
  return 0;
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
  bool                  has_cpu_counts = 0;
                  
  info(PLUGIN_SUBTYPE ": cpu constraints before => ntasks = %u, cpus_per_task = %hu, bitflags = %08x, cpus = %u-%u, nodes = %u-%u, %u, %u, %u, %u, %u, %u, %u, %u, %u, %u",
      job_desc->num_tasks,
      job_desc->cpus_per_task,
      job_desc->bitflags,
      job_desc->min_cpus,
      job_desc->max_cpus,
      job_desc->min_nodes,
      job_desc->max_nodes,
      job_desc->boards_per_node,
      job_desc->sockets_per_board,
      job_desc->sockets_per_node,
      job_desc->cores_per_socket,
      job_desc->threads_per_core,
      job_desc->ntasks_per_node,
      job_desc->ntasks_per_socket,
      job_desc->ntasks_per_core,
      job_desc->ntasks_per_board,
      job_desc->pn_min_cpus
    );
  int     i = 0;
  while ( i < job_desc->env_size ) {
    if ( (strncmp(job_desc->environment[i], "SLURM_NTASKS=", 13) == 0) ||
         (strncmp(job_desc->environment[i], "SLURM_CPUS_PER_TASK=", 20) == 0)
    ) {
      has_cpu_counts = 1;
    }
    i++;
  }
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
            if ( ! has_cpu_counts ) {
              int         index = 0;
              uint32_t    cpu_range[2] = { 1, NO_VAL };
              char        *pe_name = NULL;
              size_t      pe_name_len = 0;
              int         is_range = 0;
              
              debug3(PLUGIN_SUBTYPE ": -pe option found");
              
              /* Skip whitespace: */
              while ( *s && (*s != '\n') && isspace(*s) ) s++;
              
              /* Isolate the next word: */
              pe_name = s;
              while ( *s && (*s != '\n') && ! isspace(*s) ) s++;
              pe_name_len = s - pe_name;
              
              /* Skip whitespace: */
              while ( *s && (*s != '\n') && isspace(*s) ) s++;
              
              /* If the next character is a hyphen, this is a range
               * with an implied starting value of 1:
               */
              if ( *s == '-' ) {
                s++;
                index = 1;
                is_range = 1;
              }
              
              /* If the next word looks like a number, then proceed: */
              if ( *s && (*s != '\n') && isdigit(*s) ) {
                long        v;
                
next_number:    v = strtol(s, &e, 10);
                if ( v && (e > s) ) {
                  if ( v > 0 && v <= NO_VAL ) {
                    cpu_range[index] = v;
                    
                    /* Which value are we setting? */
                    if ( index++ == 0 ) {
                      /* Now:  if the character we ended on is a dash, then this is probably a
                       * range and we should check the next word, too:
                       */
                      if ( *e++ == '-') {
                        is_range = 1;
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
                      *err_msg = xstrdup_printf("invalid slot count (%ld) at line %ld of job script", v, line_no);
                    }
                    return SLURM_ERROR;
                  }
                } else {
                  if ( err_msg ) {
                    *err_msg = xstrdup_printf("invalid slot count at line %ld of job script", line_no);
                  }
                  return SLURM_ERROR;
                }
                
                if ( cpu_range[0] <= cpu_range[1] ) {
                    
                  /* Set the task count and cpus per task: */
                  if ( (pe_name_len == 7) && (strncmp(pe_name, "threads", 7) == 0) ) {
                    job_desc->num_tasks = 1;
                    job_desc->cpus_per_task = ( is_range ) ? cpu_range[1] : cpu_range[0];
                    job_desc->pn_min_cpus = job_desc->min_cpus = job_desc->cpus_per_task;
                    job_desc->max_cpus = NO_VAL;
                  }
                  else if (
                    ((pe_name_len == 3) && (strncmp(pe_name, "mpi", 3) == 0)) ||
                    ((pe_name_len == 11) && (strncmp(pe_name, "generic-mpi", 11) == 0))
                  ) {
                    job_desc->num_tasks = ( is_range ) ? cpu_range[1] : cpu_range[0];
                    job_desc->cpus_per_task = 1;
                    job_desc->pn_min_cpus = job_desc->min_cpus = job_desc->num_tasks;
                    job_desc->max_cpus = NO_VAL;
                  }
                  else {
                    if ( err_msg ) {
                      *err_msg = xstrdup_printf("invalid pe name at line %ld of job script", line_no);
                    }
                    return SLURM_ERROR;
                  }
                  
                  //
                  // We need to update the environment variables a'la sbatch's init_envs() function:
                  //
                  if ( ! env_array_append_fmt(&job_desc->environment, "SLURM_NTASKS", "%u", (unsigned int)job_desc->num_tasks) ) {
                    if ( err_msg ) {
                      *err_msg = xstrdup("unable to export SLURM_NTASKS to job environment");
                    }
                    return SLURM_ERROR;
                  }
                  job_desc->env_size++;
                  if ( ! env_array_append_fmt(&job_desc->environment, "SLURM_NPROCS", "%u", (unsigned int)job_desc->num_tasks) ) {
                    if ( err_msg ) {
                      *err_msg = xstrdup("unable to export SLURM_NPROCS to job environment");
                    }
                    return SLURM_ERROR;
                  }
                  job_desc->env_size++;
                  if ( ! env_array_append_fmt(&job_desc->environment, "SLURM_CPUS_PER_TASK", "%u", (unsigned int)job_desc->num_tasks) ) {
                    if ( err_msg ) {
                      *err_msg = xstrdup("unable to export SLURM_CPUS_PER_TASK to job environment");
                    }
                    return SLURM_ERROR;
                  }
                  job_desc->env_size++;
                  
                } else {
                  if ( err_msg ) {
                    *err_msg = xstrdup_printf("slot minimum (%u) > maximum (%u) at line %ld of job script", cpu_range[0], cpu_range[1], line_no);
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
 * -l <complex>=<value>{,<complex>=<value>{,..}}
 */
          else if ( (*s == 'l') && isspace(s[1]) ) {
            s += 1;
              
            debug3(PLUGIN_SUBTYPE ": -l option found");
            
            /* Skip whitespace: */
            while ( *s && (*s != '\n') && isspace(*s) ) s++;
            
            /* Begin processing comma-separated "resource=value" pairs: */
            while ( *s && (*s != '\n') ) {
              char          *rs = s, *re = s;
              char          *vs = NULL, *ve = NULL;
              
              /* Isolate the resource name: */
              while ( *re && (*re != '\n') && (*re != '=') ) re++;
              
              /* Is that the end of the line? */
              if ( *re == '=' ) {
                char        delim = ',';
                char        pc = 0;
                
                /* There's a value to grab, too: */
                vs = re + 1;
                
                /* Quoted strings need to be handled, too: */
                if ( (*vs == '"') || (*vs == '\'') ) {
                  delim = *vs;
                  vs++;
                }
                
                /* Locate the terminating delimiter: */
                ve = vs;
                while ( *ve && (*ve != '\n') ) {
                  if ( (*ve == delim) && (pc != '\\') ) break;
                  pc = *ve;
                  ve++;
                }
                
                /* Badly quoted string? */
                if ( (delim != ',') && (*ve != delim) ) {
                  if ( err_msg ) {
                    *err_msg = xstrdup_printf("unterminated quoted string at line %ld of job script %c", line_no, delim);
                  }
                  return SLURM_ERROR;
                }
                
                if ( *ve && (*ve != '\n') ) {
                  s = ve + 1;
                } else {
                  s = ve;
                }
              } else {
                if ( *re == ',' ) {
                  s = re + 1;
                } else {
                  s = re;
                }
              }
              
              /* React to the resource=value: */
              if ( (re - rs) > 1 ) {
                if ( job_submit_resource_name_in_set(rs, re - rs, "m_mem_free", "mfree", "mem_free", "mf", NULL) ) {
                  /* Per-slot memory request: */
                  uint64_t      mem_per_cpu;
                  
                  debug3(PLUGIN_SUBTYPE ": m_mem_free resource spec present");
                  if ( job_submit_sge_parse_memory(vs, ve - vs, &mem_per_cpu) ) {
                    if ( job_desc->pn_min_memory == NO_VAL64 ) {
                      job_desc->pn_min_memory = mem_per_cpu | MEM_PER_CPU;
                      info(PLUGIN_SUBTYPE ": memory request of %lu MiB per CPU from -l m_mem_free option", mem_per_cpu);
                    } else {
                      info(PLUGIN_SUBTYPE ": ignoring -l m_mem_free option, value specified elsewhere");
                    }
                  } else {
                    if ( err_msg ) {
                      *err_msg = xstrdup_printf("invalid memory specification for m_mem_free resource at line %ld of job script", line_no);
                    }
                    return SLURM_ERROR;
                  }
                }
                else if ( job_submit_resource_name_equal(rs, re - rs, "h_rt") ) {
                  /* Maximum walltime request: */
                  uint32_t      limit;
                  
                  debug3(PLUGIN_SUBTYPE ": h_rt resource spec present");
                  if ( job_submit_sge_parse_time(vs, ve - vs, &limit) ) {
                    if ( job_desc->time_limit == NO_VAL ) {
                      job_desc->time_limit = limit;
                      info(PLUGIN_SUBTYPE ": maximum walltime of %u minute%s from -l h_rt option", limit, (limit == 1) ? "" : "s");
                    } else {
                      info(PLUGIN_SUBTYPE ": ignoring -l h_rt option, value specified elsewhere");
                    }
                  } else {
                    if ( err_msg ) {
                      *err_msg = xstrdup_printf("invalid time specification for h_rt resource at line %ld of job script", line_no);
                    }
                    return SLURM_ERROR;
                  }
                }
                else if ( job_submit_resource_name_in_pair(rs, re - rs, "exclusive", "excl") ) {
                  /* Per-slot memory request: */
                  sge_bool_t      flag;
                  
                  debug3(PLUGIN_SUBTYPE ": exclusive resource spec present");
                  if ( job_submit_sge_parse_bool(vs, ve - vs, &flag) ) {
                    if ( flag != kSGEBooleanNoValue ) {
                      if ( job_desc->shared == NO_VAL16 ) {
                        job_desc->shared = (flag == kSGEBooleanTrue) ? 0 : 1;
                        info(PLUGIN_SUBTYPE ": node sharing option %d from -l exclusive option", (int)job_desc->shared);
                      } else {
                        info(PLUGIN_SUBTYPE ": ignoring -l exclusive option, value specified elsewhere");
                      }
                    }
                  } else {
                    if ( err_msg ) {
                      *err_msg = xstrdup_printf("invalid specification for exclusive resource at line %ld of job script", line_no);
                    }
                    return SLURM_ERROR;
                  }
                }
              }
            }
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
  
                  
  info(PLUGIN_SUBTYPE ": cpu constraints after => ntasks = %u, cpus_per_task = %hu, bitflags = %08x, cpus = %u-%u, nodes = %u-%u, %u, %u, %u, %u, %u, %u, %u, %u, %u, %u",
      job_desc->num_tasks,
      job_desc->cpus_per_task,
      job_desc->bitflags,
      job_desc->min_cpus,
      job_desc->max_cpus,
      job_desc->min_nodes,
      job_desc->max_nodes,
      job_desc->boards_per_node,
      job_desc->sockets_per_board,
      job_desc->sockets_per_node,
      job_desc->cores_per_socket,
      job_desc->threads_per_core,
      job_desc->ntasks_per_node,
      job_desc->ntasks_per_socket,
      job_desc->ntasks_per_core,
      job_desc->ntasks_per_board,
      job_desc->pn_min_cpus
    );
  
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
    job_desc->pn_min_memory = udhpc_min_mem_mb | MEM_PER_CPU;
    info(PLUGIN_SUBTYPE ": setting default memory limit (%lu MiB per CPU)", udhpc_min_mem_mb);
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
