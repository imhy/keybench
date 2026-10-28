# This Makefile first compiles the vendored Lua into a static library and then
# links the benchmark against it. A plain make builds ./keybench with only the
# in memory skiplist backend. The run target builds and then runs the cart
# workload, the all-wl target builds and runs every workload under workloads,
# and clean removes the binary and the built Lua objects.
#
# Backends are modular. Each one lives under backends with its own backend.mk
# and registers itself at load time, as src/backend_registry.h describes. Pick
# which to compile in by listing them, for example make BACKENDS="skiplist
# rocksdb". The shorthands make ROCKSDB=1 and make TIDESDB=1 add those two
# backends to the default list.
#
# To build against a system Lua rather than the vendored copy, run make
# LUA_SYS=1, which locates the library through pkg-config.

CC      ?= cc
CFLAGS  ?= -O2 -std=c11 -Wall -Wextra
LDLIBS  := -lpthread -lm           # threads for concurrent workers, libm for the histogram
BIN     := keybench

# keybench version, read from the VERSION file and surfaced in reports.
KB_VERSION := $(shell cat VERSION 2>/dev/null || echo dev)
CFLAGS += -DKEYBENCH_VERSION='"$(KB_VERSION)"'

# Core sources, the ones that do not depend on any particular engine.
SRC     := src/main.c src/hist.c src/kvlua.c src/store.c src/reporter.c src/registry.c src/config.c \
           src/probe.c src/probe_system.c src/probe_build.c src/affinity.c

# Which backends to compile in. Each backend has a backend.mk that appends its
# own sources, compiler flags, and libraries. The ROCKSDB=1 and TIDESDB=1
# shorthands each add a backend to this list.
BACKENDS ?= skiplist
ifeq ($(ROCKSDB),1)
  BACKENDS += rocksdb
endif
ifeq ($(TIDESDB),1)
  BACKENDS += tidesdb
endif
ifeq ($(STATEENGINE),1)
  BACKENDS += stateengine
endif
include $(foreach b,$(BACKENDS),backends/$(b)/backend.mk)

# Memory allocator (optional, off by default, uses the system allocator).
# Linking an allocator into the executable as the earliest NEEDED library makes
# the whole malloc family, malloc_usable_size included, resolve to it. This
# matters when a distro rocksdb or tidesdb was itself built against jemalloc,
# since glibc malloc_usable_size called on jemalloc owned memory segfaults when a
# database is reopened across sweep points. The --no-as-needed flag keeps the
# dependency even though keybench's own malloc calls are also satisfiable by libc.
#
# Set ALLOC to jemalloc or tcmalloc to link that allocator first. For any other
# allocator set ALLOC_LIBS directly, for example ALLOC_LIBS="-L/opt/lib
# -l:libmimalloc.so". Override ALLOC_LIBS to point at a custom install path too.
ALLOC ?=
ifeq ($(ALLOC),jemalloc)
  ALLOC_LIBS ?= -Wl,--no-as-needed,-l:libjemalloc.so.2,--as-needed
endif
ifeq ($(ALLOC),tcmalloc)
  ALLOC_LIBS ?= -Wl,--no-as-needed,-l:libtcmalloc.so.4,--as-needed
endif
ifneq ($(strip $(ALLOC_LIBS)),)
  LDLIBS := $(ALLOC_LIBS) $(LDLIBS)
endif

.PHONY: all
all: $(BIN)

ifeq ($(LUA_SYS),1)
  LUA_PKG := $(shell pkg-config --exists lua5.4 && echo lua5.4 || echo lua)
  LUA_CFLAGS := $(shell pkg-config --cflags $(LUA_PKG))
  LUA_LIBS   := $(shell pkg-config --libs $(LUA_PKG))
  $(BIN): $(SRC)
	$(CC) $(CFLAGS) $(LUA_CFLAGS) -Isrc $(SRC) -o $@ $(LUA_LIBS) $(LDLIBS)
else
  LUA_DIR := vendor
  LUA_LIB := $(LUA_DIR)/liblua.a
  # Everything except lua.c and luac.c goes into the library. Those two each
  # define their own main(), the interpreter and the bytecode compiler, so they
  # must stay out of the archive.
  LUA_SRC := $(filter-out $(LUA_DIR)/lua.c $(LUA_DIR)/luac.c,$(wildcard $(LUA_DIR)/*.c))
  LUA_OBJ := $(LUA_SRC:.c=.o)
  # LUA_USE_POSIX routes os.tmpname through mkstemp instead of tmpnam, which
  # also silences the linker's tmpnam warning, and is the intended POSIX build.
  $(LUA_DIR)/%.o: $(LUA_DIR)/%.c
	$(CC) -O2 -w -DLUA_USE_POSIX -c $< -o $@
  $(LUA_LIB): $(LUA_OBJ)
	ar rcs $@ $(LUA_OBJ)
  $(BIN): $(SRC) $(LUA_LIB)
	$(CC) $(CFLAGS) -I$(LUA_DIR) -Isrc $(SRC) $(LUA_LIB) -o $@ $(LDLIBS)
endif

# What the binary will actually load, which is the thing worth checking: a build
# can link against one prefix and still resolve to another, and then the report
# names a version that never ran.
.PHONY: verify
verify: $(BIN)
	@echo "=== resolved at runtime ==="
	@ldd ./$(BIN) | grep -E "rocksdb|tidesdb|jemalloc|tcmalloc" || echo "  (no engine libs linked)"
	@echo "=== versions the binary reports ==="
	@for e in rocksdb tidesdb; do \
	  ./$(BIN) --backend $$e --ops 1 --threads 1 --probe none --data-dir $${TMPDIR:-/tmp} \
	    workloads/mixed.lua 2>/dev/null | grep -oE "engine=$$e [0-9.]+" | head -1; \
	done

.PHONY: run all-wl clean
run: $(BIN)
	./$(BIN) workloads/cart.lua

all-wl: $(BIN)
	./$(BIN) workloads/cart.lua workloads/mixed.lua workloads/scan.lua workloads/batch.lua

clean:
	rm -f $(BIN) vendor/*.o vendor/liblua.a
	rm -rf .build
