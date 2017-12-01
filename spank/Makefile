#
## Makefile for UD SLURM SPANK plugins
#
#
#SLURM_PREFIX		= /opt/shared/slurm/17.02.7
SLURM_INCLUDE		= $(SLURM_PREFIX)/include
SLURM_LIB		= $(SLURM_PREFIX)/lib

PLUGINS			= gridengine_compat.so

#
#
#CPPFLAGS		+= -I$(SLURM_INCLUDE)
CFLAGS			+= -Wall
LDFLAGS			+= -L$(SLURM_LIB)
LIBS			+=

#
#
#.SUFFIXES: .c .o .so

all: $(PLUGINS)

clean::
	$(RM) $(PLUGINS) *.o

install: $(PLUGINS)
	cp $(PLUGINS) $(SLURM_LIB)/slurm

#
#
#%.o: %.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ -fPIC -c $<

%.so: %.o
	$(CC) -shared -o $@ $< $(LDFLAGS) $(LIBS)

