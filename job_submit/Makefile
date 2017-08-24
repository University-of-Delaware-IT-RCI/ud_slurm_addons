#
# Makefile for UD SLURM SPANK plugins
#

SLURM_PREFIX		= /opt/shared/slurm/current
SLURM_INCLUDE		= $(SLURM_PREFIX)/include
SLURM_LIB		= $(SLURM_PREFIX)/lib
SLURM_SRC		= $(SLURM_PREFIX)/src

PLUGIN			= job_submit_udhpc.so

#

CPPFLAGS		+= -I$(SLURM_INCLUDE) -I$(SLURM_SRC)
CFLAGS			+= -Wall -fPIC -DPIC
LDFLAGS			+= -L$(SLURM_LIB) -Wl,-soname,$(PLUGIN)
LIBS			+=

#

.SUFFIXES: .c .o .so

all: $(PLUGIN)

clean::
	$(RM) $(PLUGIN) *.o

install: $(PLUGIN)
	cp $(PLUGIN) $(SLURM_LIB)/slurm

#

%.o: %.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ -fPIC -c $<

%.so: %.o
	$(CC) -shared -o $@ $< $(LDFLAGS) $(LIBS)

