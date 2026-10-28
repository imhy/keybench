# stateengine - the streaming state engine through libstateengine.so's C ABI
# (stateengine-flink src/ffi.rs), the same handle layer the Flink bridge drives.
#
# STATEENGINE_PREFIX names a self-contained prefix holding include/stateengine.h
# and lib/libstateengine.so, for example one staged from a release build:
#   cargo build -p stateengine-flink --release
#   make STATEENGINE=1 STATEENGINE_PREFIX=/opt/engines/stateengine-<sha>
# A SOURCE file in the prefix, if present, supplies the version the report prints
# (its first line's second field, a git commit).
STATEENGINE_PREFIX ?=
ifeq ($(STATEENGINE_PREFIX),)
  $(error STATEENGINE=1 needs STATEENGINE_PREFIX pointing at a prefix with include/stateengine.h and lib/libstateengine.so)
endif
ifeq ($(wildcard $(STATEENGINE_PREFIX)/include/stateengine.h),)
  $(error no stateengine.h under $(STATEENGINE_PREFIX)/include -- not a stateengine prefix)
endif
ifeq ($(wildcard $(STATEENGINE_PREFIX)/lib/libstateengine.so),)
  $(error no libstateengine.so under $(STATEENGINE_PREFIX)/lib)
endif

STATEENGINE_CFLAGS ?= -I$(STATEENGINE_PREFIX)/include
STATEENGINE_LIBS   ?= -L$(STATEENGINE_PREFIX)/lib -Wl,-rpath,$(STATEENGINE_PREFIX)/lib -lstateengine
STATEENGINE_VER    := $(shell awk 'NR==1{print substr($$2,1,8)}' $(STATEENGINE_PREFIX)/SOURCE 2>/dev/null)

SRC    += backends/stateengine/stateengine.c
CFLAGS += $(STATEENGINE_CFLAGS)
ifneq ($(STATEENGINE_VER),)
  CFLAGS += -DKB_STATEENGINE_VERSION='"$(STATEENGINE_VER)"'
endif
LDLIBS += $(STATEENGINE_LIBS)
