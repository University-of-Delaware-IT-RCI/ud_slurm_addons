#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# As time goes by and jobs are killed or cancelled, files get orphaned in
# /dev/shm and tie-up valuable memory.  This script builds a list of first-level
# entities under /dev/shm, filtering by various criteria.  Next, lsof is used
# to generate a list of all in-use entities under /dev/shm (retaining just the
# first-level-deep path).  Anything in the first set that's not present in the
# second set is removed.
#
# The filtering is defined in the devShmPathShouldInclude() function.  Right
# now the criteria are:
#
#     - one of st_{mtime,ctime,atime} are newer than the cutoff timestamp
#
# If the criteria are met, False is returned and the first-level path in
# question is added to an exclusion set.  Otherwise, it is added to an
# inclusion set.
#
# Some files should be treated such that they MUST be open to be retained, or
# at least be opened and have a very limited window for atime/mtime to affect
# their exclusion.  We call these "special treatment" cases, and their
# cutoff timestamp is always 1 hour ago.  A CLI flag allows special treatment
# to be disabled.
#
# Once the two sets are generated, the set difference produces the first-level
# paths that are okay for removal according to our criteria.
#
# Copyright © 2018
# Dr. Jeffrey Frey
# Network & Systems Services, University of Delaware
#

import sys
import os
import argparse
import logging
import re
import time
import sets
import subprocess

try:
	test_set = set()
	def newSet():
		return set()
except:
	try:
		import sets
		def newSet():
			return sets.Set()
	except:
		sys.stdout.write('FATAL ERROR:  no set class available in this Python interpreter\n')
		sys.exit(1)

#
##
#

# Will get set after CLI args are processed:
firstLevelDevShmRegex = None

def firstLevelDevShmPath(path):
	m = firstLevelDevShmRegex.match(path)
	if m:
		return m.group(1)
	raise ValueError('invalid path to firstLevelDevShmPath: ' + path)

#
##
#

# Default to Unix epoch for cutoff -- which means everything would be
# excluded.  Later in this script this will get changed to a more
# useful value.
cutoff_timestamp = 0
special_cutoff_timestamp = 0

# Number of seconds that special-treatments files must be newer than:
special_cutoff_threshold = 3600

def devShmPathShouldInclude_Strict(path):
	s = os.stat(path)
	if s and s.st_mtime > cutoff_timestamp or s.st_ctime > cutoff_timestamp or s.st_atime > cutoff_timestamp:
		return False
	return True

def devShmPathShouldInclude_SpecialTreatment(path):
	if 'psm2_shm' in path or 'vader_segment' in path:
		s = os.stat(path)
		if s and s.st_mtime > special_cutoff_timestamp or s.st_ctime > special_cutoff_timestamp or s.st_atime > special_cutoff_timestamp:
			return False
	else:
		return devShmPathShouldInclude_Strict(path)
	return True

devShmPathShouldInclude = devShmPathShouldInclude_SpecialTreatment

#
##
#

def recursiveRm(path):
	try:
		rm_process = subprocess.Popen(['rm', '-rf', path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		(rm_stdout, rm_stderr) = rm_process.communicate()
		if rm_process.returncode != 0:
			raise RuntimeError('failed to remove ' + path + ': ' + rm_stderr.replace('\n', '; '))
	except Exception as E:
		raise RuntimeError('failed to remove ' + path + ': ' + str(E).replace('\n', '; '))

#
##
#

def timeStringToSeconds(time_str, implied_unit = 's'):
	unit_multipliers = { 's':1, 'S':1, 'm':60, 'M':60, 'h':3600, 'H':3600, 'd':86400, 'D':86400 }
	try:
		time_bits = re.match(r'^([+-]?(([0-9]*(\.[0-9]+))|([0-9]+(\.[0-9]*)?)))([smhdSMHD])?$', time_str)
		if not time_bits:
			return None
		seconds = float(time_bits.group(1))
		if time_bits.group(7):
			implied_unit = time_bits.group(7)
		if implied_unit in unit_multipliers:
			seconds *= unit_multipliers[implied_unit]
		else:
			return None
		return seconds
	except:
		return None

#
# Setup CLI arguments:
#
cli_parser = argparse.ArgumentParser(description='Cleanup /dev/shm')
cli_parser.add_argument('-v', '--verbose',
		default=0, action='count', dest='verbose_level',
		help='increase level of verbosity'
	)
cli_parser.add_argument('-q', '--quiet',
		default=0, action='count', dest='quiet_level',
		help='decrease level of verbosity'
	)
cli_parser.add_argument('-n', '--dry-run',
		default=False, action='store_true', dest='is_dry_run',
		help='do not remove any files, just display what would be done; this option sets the base verbosity level to INFO (as in -vv)'
	)
cli_parser.add_argument('--dev-shm-prefix', '-P', metavar='<path>',
		default='/dev/shm', dest='dev_shm_prefix',
		help='mountpoint of the shm file system (default: /dev/shm)'
	)
cli_parser.add_argument('--lsof', '-L', metavar='<cmd>',
		default='lsof', dest='lsof',
		help='path to the lsof command; if the value is a bare command (not a path) then lsof commands are executed in a shell (default: lsof)'
	)
cli_parser.add_argument('--show-log-timestamps', '-t',
		default=False, action='store_true', dest='show_log_timestamps',
		help='display timestamps on all messages logged by this program'
	)
cli_parser.add_argument('--age', '-a',
		metavar='<age-threshold>', default='1', dest='age_threshold',
		help='only items older than this will be removed; integer or floating-point values are acceptable with optional unit of s/m/h/d (default: d)'
	)
cli_parser.add_argument('--no-special-treatment',
		default=False, action='store_true', dest='is_special_treatment_disabled',
		help='do not treat PSM2 and vader segment files any differently than other files'
	)
cli_parser.add_argument('--log-file', '-l',
		metavar='<filename>', dest='log_file',
		help='send all logging to this file instead of to stderr; timestamps are always enabled when logging to a file'
	)
cli_parser.add_argument('--daemon',
		default=False, action='store_true', dest='is_daemon',
		help='run as a daemon, periodically waking to re-check'
	)
cli_parser.add_argument('--daemon-period',
		metavar='<period>', default='86400', dest='daemon_period',
		help='wake to re-check on the given period; integer or floating-point values are acceptable with optional unit of s/m/h/d (default: s)'
	)
cli_parser.add_argument('--pid-file',
		metavar='<filename>', default='/var/run/shm-cleanup.pid', dest='pid_file',
		help='in daemon mode, write our pid to this file (default: /var/run/shm-cleanup.pid)'
	)


cli_args = cli_parser.parse_args()

#
# for dry-run, the default logging level is INFO (20)
# and otherwise it's ERROR (40):
#
base_logging_level = 2 if cli_args.is_dry_run else 4

#
# logging verbosity = 10 * (base - verbose_level + quiet_level)
#
# clamped to [0,50]
#
verbosity = 10 * (base_logging_level - cli_args.verbose_level + cli_args.quiet_level)
verbosity = (50 if verbosity >= 50 else (10 if verbosity <= 10 else verbosity))
if cli_args.log_file and cli_args.log_file != '-':
	logging.basicConfig(
			filename=cli_args.log_file,
			format='%(asctime)s [%(levelname)-8s] %(message)s',
			datefmt='%Y-%m-%d %H:%M:%S',
			level=verbosity
		)
else:
	logging.basicConfig(
			format='%(asctime)s [%(levelname)-8s] %(message)s' if cli_args.show_log_timestamps else '[%(levelname)-8s] %(message)s',
			datefmt='%Y-%m-%d %H:%M:%S',
			level=verbosity
		)

#
# Validate the shm directory prefix:
#
dev_shm_prefix = os.path.realpath(cli_args.dev_shm_prefix)
if not os.path.isdir(dev_shm_prefix):
	logging.critical('shm prefix path "{:s}" does not exist'.format(cli_args.dev_shm_prefix))
	sys.exit(1)
# Generate the regex:
firstLevelDevShmRegex = re.compile('({:s}/[^/]+)'.format(dev_shm_prefix))

#
# No special treatment?
#
if cli_args.is_special_treatment_disabled:
	devShmPathShouldInclude = devShmPathShouldInclude_Strict
	logging.info('no special treatment of PSM2 and vader segment files')

#
# Calculate age threshold:
#
age_threshold = timeStringToSeconds(cli_args.age_threshold, implied_unit = 'd')
if age_threshold is None:
	logging.error('invalid age threshold specified: %s', cli_args.age_threshold)
	sys.exit(2)
logging.info('age threshold of %d second(s)', age_threshold)

#
# This function does the actual scan-and-cleanup work:
#
def do_scan(dev_shm_prefix, lsof):
	global cutoff_timestamp, special_cutoff_timestamp
	#
	# Scan /dev/shm for all entities with modification timestamps greater than age_threshold
	# seconds ago:
	#
	cutoff_timestamp = time.time() - age_threshold
	special_cutoff_timestamp = time.time() - special_cutoff_threshold
	logging.info('cutoff timestamp for modification timestamps, standard: %d', cutoff_timestamp)
	logging.info('cutoff timestamp for modification timestamps, specials: %d', special_cutoff_timestamp)
	include_shm_entities = newSet()
	exclude_shm_entities = newSet()
	for root_dir, dirs, files in os.walk(dev_shm_prefix, topdown=False):
		# Check files:
		for file in files:
			p = os.path.join(root_dir, file)
			if devShmPathShouldInclude(p):
				include_shm_entities.add(firstLevelDevShmPath(p))
			else:
				exclude_shm_entities.add(firstLevelDevShmPath(p))
		# Once we get to /dev/shm, scan the directories, too; but we
		# don't let a directory's timestamp being newer exclude it from
		# being removed -- only child files can do that:
		if root_dir == dev_shm_prefix:
			for dir in dirs:
				p = os.path.join(root_dir, dir)
				if devShmPathShouldInclude(p):
					include_shm_entities.add(p)

	#
	# Get the set difference, include_shm_entities / exclude_shm_entities:
	#
	include_shm_entities -= exclude_shm_entities

	#
	# Count and summarize how many items we see:
	#
	def summarizeDevShmEntitySet(theSet, whatAreThey):
		vader_count = 0
		psm_count = 0
		unknown_count = 0
		unknown_paths = []
		for p in theSet:
			if 'psm2_shm' in p:
				psm_count += 1
			elif 'vader_segment' in p:
				vader_count += 1
			else:
				unknown_count += 1
				unknown_paths.append(p)
		if unknown_count > 0:
			logging.warning('found %d %s', len(theSet), whatAreThey)
			logging.warning('  PSM2 segments:           %8d', psm_count)
			logging.warning('  Open MPI vader segments: %8d', vader_count)
			logging.warning('  Unidentified items:      %8d', len(theSet) - (psm_count + vader_count))
			for p in unknown_paths:
				logging.warning('      %s', p)
		else:
			logging.info('found %d %s', len(theSet), whatAreThey)
			if psm_count + vader_count > 0:
				logging.info('  PSM2 segments:           %8d', psm_count)
				logging.info('  Open MPI vader segments: %8d', vader_count)

	summarizeDevShmEntitySet(include_shm_entities, 'removable first-level entities under ' + dev_shm_prefix)

	#
	# Make sure we're running as root:
	#
	if os.getuid() != 0:
		logging.critical('scanning for active {:s} files requires root privileges'.format(dev_shm_prefix))
		sys.exit(1)

	#
	# Ask lsof to show us all open files under /dev/shm.  We can't use
	# check_output() because lsof +D will return non-zero due to files
	# present under /dev/shm that aren't in use :-\
	#
	try:
		cmd_seq = [lsof, '-lnP', '+D', dev_shm_prefix]
		if os.sep in lsof:
			logging.debug('performing lsof command "{:s}"'.format(' '.join(cmd_seq)))
			lsof_process = subprocess.Popen(cmd_seq, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		else:
			logging.debug('performing lsof command "{:s}" in a shell'.format(' '.join(cmd_seq)))
			lsof_process = subprocess.Popen(' '.join(cmd_seq), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
	except Exception as E:
		logging.error(str(E))
		sys.exit(1)

	#
	# We only want to check the stdout from lsof.  Scan first-level
	# paths into a set.
	#
	inuse_shm_entities = newSet()
	while True:
		line = lsof_process.stdout.readline()
		if line != b'':
			m = firstLevelDevShmRegex.search(str(line).strip())
			if m:
				inuse_shm_entities.add(m.group(1))
		else:
			break
	lsof_process.wait()

	#
	# Count and summarize how many items we see in-use:
	#
	summarizeDevShmEntitySet(inuse_shm_entities, 'in-use first-level entities under ' + dev_shm_prefix)

	#
	# The set difference gives us what we need to remove:
	#
	remove_shm_entities = include_shm_entities - inuse_shm_entities
	if len(remove_shm_entities) < len(include_shm_entities):
		summarizeDevShmEntitySet(remove_shm_entities, 'first-level entities under {:s} to be removed'.format(dev_shm_prefix))

	if len(remove_shm_entities) > 0:
		#
		# Actually remove stuff -- or show that we would if we're running
		# in dry-run mode:
		#
		if cli_args.is_dry_run:
			logging.info('dry-run summary of actions that would be performed')
			for p in remove_shm_entities:
				logging.info('  rm -rf %s', p)
		else:
			logging.info('processing removal list')
			for p in remove_shm_entities:
				try:
					recursiveRm(p)
					logging.info('  OK   rm -rf %s', p)
				except Exception as E:
					logging.info('  FAIL rm -rf %s : %s', p, str(E))
	else:
		logging.warning('nothing to be removed from ' + dev_shm_prefix)


#
# Running as a daemon?
#
if cli_args.is_daemon:
	#
	# Determine how long to wait between checks:
	#
	daemon_period = timeStringToSeconds(cli_args.daemon_period, implied_unit = 's')
	if daemon_period < 30:
		logging.warning('daemon wake period limited to 30s (instead of %ds)', daemon_period)
		daemon_period = 30
	logging.info('daemonizing on a period of %d second(s)', daemon_period)

	#
	# Get pid file setup:
	#
	pid_file = None
	if cli_args.pid_file:
		if not cli_args.pid_file.startswith('/'):
			logging.critical('pid file not an absolute path: %s', cli_args.pid_file)
			sys.exit(2)
		# File exists?
		if os.path.exists(cli_args.pid_file):
			logging.critical('pid file already exists: %s', cli_args.pid_file)
			sys.exit(2)
		# Open file for write:
		try:
			pid_fptr = open(cli_args.pid_file, 'w')
			pid_fptr.write(str(os.getpid()))
			pid_fptr.close()
		except Exception as E:
			logging.critical('could not write to pid file %s: %s', cli_args.pidfile, str(E))
		pid_file = cli_args.pid_file
		logging.info('pid written to %s', pid_file)

	#
	# We want to ignore SIGHUP instead of being killed and remove
	# our pid file on termination:
	#
	import signal
	signal.signal(signal.SIGHUP, signal.SIG_IGN)

	def termination_handler(signum, frame):
		if pid_file is not None and os.path.exists(pid_file):
			try:
				os.remove(pid_file)
				logging.info('removed pid file %s', pid_file)
			except Exception as E:
				logging.critical('could not remove pid file %s: %s', pid_file, str(E))
		logging.info('exiting on signal %d', signum)
		sys.exit(0)
	signal.signal(signal.SIGTERM, termination_handler)
	signal.signal(signal.SIGINT, termination_handler)

	#
	# Enter our runloop; only being killed will break us out:
	#
	try:
		while True:
			do_scan(dev_shm_prefix, cli_args.lsof)
			time.sleep(daemon_period)
	except:
		pass
	if pid_file is not None and os.path.exists(pid_file):
		try:
			os.remove(pid_file)
			logging.info('removed pid file %s', pid_file)
		except Exception as E:
			logging.critical('could not remove pid file %s: %s', pid_file, str(E))
else:
	#
	# Single run only:
	#
	do_scan(dev_shm_prefix, cli_args.lsof)
