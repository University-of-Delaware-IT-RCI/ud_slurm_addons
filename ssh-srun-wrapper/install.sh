#!/bin/bash
#
# Install this "ssh" over top of the existing "ssh" on the current path.
#

ORIG_SSH="$(which ssh)"
if [ $? -ne 0 -o -z "$ORIG_SSH" ]; then
    echo "ERROR:  unable to determine path to existing 'ssh' command"
    exit 1
fi

SRC_DIR="$(dirname "$(realpath "${BASH_SOURCE[0]}")")"
if [ $? -ne 0 -o -z "$SRC_DIR" ]; then
    echo "ERROR:  unable to determine installation source directory"
    exit 1
fi

if [ ! -f "${SRC_DIR}/ssh" ]; then
    echo "ERROR:  'ssh' script not found in $SRC_DIR"
    exit 1
fi

install --version >/dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "ERROR:  'install' command not available"
    exit 1
fi

install --backup --suffix=.orig "${SRC_DIR}/ssh" "$ORIG_SSH"

