# rocksdb - real persistent engine (self-concurrent).
#
# Defaults target the system v11 install: headers under /usr/include/rocksdb and
# librocksdb.so.11 on the default library search path.
#
# The header pin needs care when a second rocksdb sits under /usr/local, since
# /usr/local/include is searched ahead of /usr/include and its headers shadow the
# v11 ones. A plain -I/usr/include does not fix that. The compiler drops any -I
# naming a directory already on its standard search path, keeping the standard
# order, so the flag is silently ignored, the shadow still wins, and the build
# compiles the older headers against the v11 library while the report prints the
# v11 version read from ROCKSDB_VERSION_H. Stage a private include root holding a
# single rocksdb symlink instead and point -I at that. It is not a standard
# directory, so it is honoured and lands ahead of /usr/local/include.
#
# Override for a custom install, e.g.:
#   make ROCKSDB=1 ROCKSDB_CFLAGS=-I/opt/rocksdb/include \
#                  ROCKSDB_LIBS="-L/opt/rocksdb/lib -lrocksdb" \
#                  ROCKSDB_VERSION_H=/opt/rocksdb/include/rocksdb/version.h
# Overriding ROCKSDB_CFLAGS leaves the staged root unused, so a custom prefix
# needs no knowledge of it.
# ROCKSDB_PREFIX points the whole build at one self-contained install, which is
# what install_rocksdb.sh --prefix produces. It derives the three variables below,
# and the rpath matters as much as the -L: -L only steers the link, so without it
# the loader can still hand the process a different build of the same soname and
# the report would name a version that never ran.
#   make ROCKSDB=1 ROCKSDB_PREFIX=/opt/engines/rocksdb-11.8.1
ROCKSDB_PREFIX ?=
ifneq ($(ROCKSDB_PREFIX),)
  ROCKSDB_CFLAGS    ?= -I$(ROCKSDB_PREFIX)/include
  ROCKSDB_LIBS      ?= -L$(ROCKSDB_PREFIX)/lib -Wl,-rpath,$(ROCKSDB_PREFIX)/lib -lrocksdb
  ROCKSDB_VERSION_H ?= $(ROCKSDB_PREFIX)/include/rocksdb/version.h
  ifeq ($(wildcard $(ROCKSDB_PREFIX)/include/rocksdb/c.h),)
    $(error no rocksdb/c.h under $(ROCKSDB_PREFIX)/include -- not a rocksdb prefix)
  endif
  ifeq ($(wildcard $(ROCKSDB_PREFIX)/lib/librocksdb.*),)
    $(error no librocksdb under $(ROCKSDB_PREFIX)/lib -- a distro layout like /usr keeps libraries in lib/<triplet> and would link a different build than $(ROCKSDB_PREFIX)/include describes)
  endif
endif

ROCKSDB_INC_DIR   ?= /usr/include
ROCKSDB_STAGE_INC := .build/rocksdb-include
$(shell mkdir -p $(ROCKSDB_STAGE_INC) && ln -sfn $(ROCKSDB_INC_DIR)/rocksdb $(ROCKSDB_STAGE_INC)/rocksdb)

ROCKSDB_CFLAGS    ?= -I$(ROCKSDB_STAGE_INC)
ROCKSDB_LIBS      ?= -l:librocksdb.so.11
ROCKSDB_VERSION_H ?= $(ROCKSDB_INC_DIR)/rocksdb/version.h

# <rocksdb/version.h> is a C++ header, so extract the version macros at build
# time and pass them as a string define (used by rdb_version()).
ROCKSDB_VER := $(shell awk '/define ROCKSDB_MAJOR/{a=$$3}/define ROCKSDB_MINOR/{b=$$3}/define ROCKSDB_PATCH/{c=$$3}END{if(a!="")print a"."b"."c}' $(ROCKSDB_VERSION_H) 2>/dev/null)

# ROCKSDB_FLAVOR labels a fork linked through this backend, e.g. a prefix holding
# Flink's bundled FRocksDB or ForSt library:
#   make ROCKSDB=1 ROCKSDB_PREFIX=/opt/engines/forst-0.1.8 ROCKSDB_FLAVOR=forst-0.1.8
# The report then names "forst-0.1.8 (rocksdb 8.10.0)" instead of plain rocksdb.
ROCKSDB_FLAVOR ?=
ifneq ($(ROCKSDB_FLAVOR),)
  CFLAGS += -DKB_ROCKSDB_FLAVOR='"$(ROCKSDB_FLAVOR)"'
endif

SRC    += backends/rocksdb/rocksdb.c
CFLAGS += $(ROCKSDB_CFLAGS)
ifneq ($(ROCKSDB_VER),)
  CFLAGS += -DKB_ROCKSDB_VERSION='"$(ROCKSDB_VER)"'
endif
LDLIBS += $(ROCKSDB_LIBS)
