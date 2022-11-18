ASAN ?= 0
TSAN ?= 0
UBSAN ?= 0
DEBUG ?= 1
DIRS = $(sort $(dir $(wildcard ./src/*/), ./src/))
SRCS = $(foreach dir,$(DIRS),$(wildcard $(dir)*.cpp))
OBJS = $(SRCS:./src/%.cpp=./obj/%.o)
DEPS = $(OBJS:%.o=%.d)
CC = clang++-16
AR = ar
BIN = pe

SDL2_SRC = ./deps/SDL2
SDL2_LIB = libSDL2.a

INCLUDES = \
	-Isrc \
	-I$(SDL2_SRC)/include

LIBS = \
	./lib/$(SDL2_LIB)

DEFS = \
	$(if $(filter $(DEBUG),0),-DNDEBUG)

ifneq ($(ASAN),0)
ASAN_CFLAGS = -fsanitize=address -static-libsan
ASAN_LDFLAGS = -fsanitize=address -static-libsan
endif

ifneq ($(TSAN),0)
TSAN_CFLAGS = -fsanitize=thread -static-libsan
TSAN_LDFLAGS = -fsanitize=thread -static-libsan
endif

ifneq ($(UBSAN),0)
TSAN_CFLAGS = -fsanitize=undefined -static-libsan
TSAN_LDFLAGS = -fsanitize=undefined -static-libsan
endif

CFLAGS = \
	-std=c++20 \
	-stdlib=libc++ \
	-fmodules \
	-fmodule-map-file=module.modulemap \
	-fprebuilt-module-path=modules \
	-Wall \
	-Werror \
	-pedantic \
	$(if $(filter-out $(DEBUG),0),-O0,-O3) \
	$(if $(filter-out $(DEBUG),0),-g3) \
	$(ASAN_CFLAGS) \
	$(TSAN_CFLAGS) \
	$(INCLUDES)



LDFLAGS = \
	-L./lib \
	$(LIBS:./lib/%=-l:%) \
	-lstdc++ \
	-ldl \
	-lpthread \
	-lm \
	-lvulkan \
	-latomic \
	-no-pie \
	-flto \
	$(ASAN_LDFLAGS) \
	$(TSAN_LDFLAGS)

MODNAMES = \
	sync \
	sync-scheduler \
	logger \
	platform \
	concurrency \
	lockfree_queue \
	event \
	shared_ptr-base \
	shared_ptr \
	meta \
	assert \
	lockfree_list \
	iterable_lockfree_list \
	snap_collector \
	tls \
	hazard_ptr
	wait_free_serial_work

TEST_DIR = ./test
TEST_SRCS = $(wildcard $(TEST_DIR)/*.cpp)
TEST_BINS = $(TEST_SRCS:./test/%.cpp=./test/bin/%)

MODULES = $(MODNAMES:%=modules/%.pcm)

.PHONY: all tests libs mods clean distclean
all: $(BIN)
tests: $(TEST_BINS)

test/bin/%: test/%.cpp $(MODULES) $(LIBS)
	@mkdir -p $(dir $@)
	@printf "%-8s %s\n" "[CC]" $@
	@$(CC) $(CFLAGS) $(DEFS) $^ -o $@ $(LDFLAGS)

lib/$(SDL2_LIB):
	@mkdir -p $(dir $@)
	@mkdir -p $(SDL2_SRC)/build
	@cd $(SDL2_SRC)/build \
		&& ../configure \
		&& make
	@cp $(SDL2_SRC)/build/build/.libs/$(SDL2_LIB) $@

modules/wait_free_serial_work.pcm: \
	src/wait_free_serial_work.cpp \
	modules/concurrency.pcm \
	modules/shared_ptr.pcm \
	modules/assert.pcm

modules/hazard_ptr.pcm: \
	src/hazard_ptr.cpp \
	modules/platform.pcm \
	modules/logger.pcm \
	modules/tls.pcm

modules/iterable_lockfree_list.pcm: \
	src/iterable_lockfree_list.cpp \
	modules/platform.pcm \
	modules/concurrency.pcm \
	modules/snap_collector.pcm \
	modules/hazard_ptr.pcm

modules/tls.pcm: \
	src/tls.cpp \
	modules/platform.pcm \
	modules/assert.pcm \
	modules/shared_ptr.pcm

modules/snap_collector.pcm: \
	src/snap_collector.cpp \
	modules/tls.pcm \
	modules/lockfree_list.pcm

modules/lockfree_list.pcm: \
	src/lockfree_list.cpp \
	modules/platform.pcm \
	modules/concurrency.pcm \
	modules/hazard_ptr.pcm

modules/assert.pcm: \
	src/assert.cpp \
	modules/platform.pcm \
	modules/logger.pcm

modules/meta.pcm: \
	src/meta.cpp

modules/shared_ptr-base.pcm: \
	src/shared_ptr.cpp \
	modules/platform.pcm \
	modules/logger.pcm \
	modules/meta.pcm \
	modules/concurrency.pcm

modules/shared_ptr.pcm: \
	src/atomic_shared_ptr.cpp \
	modules/shared_ptr-base.pcm \
	modules/platform.pcm \
	modules/logger.pcm \
	modules/concurrency.pcm

modules/event.pcm: \
	src/event.cpp

modules/lockfree_queue.pcm: \
	src/lockfree_queue.cpp \
	modules/concurrency.pcm \
	modules/platform.pcm \
	modules/hazard_ptr.pcm

modules/platform.pcm: \
	src/platform.cpp

modules/concurrency.pcm: \
	src/concurrency.cpp \
	modules/platform.pcm \
	modules/logger.pcm

modules/sync.pcm: \
	src/sync.cpp \
	modules/sync-scheduler.pcm \
	modules/concurrency.pcm \
	modules/logger.pcm \
	modules/meta.pcm \
	modules/platform.pcm

modules/sync-scheduler.pcm: \
	src/scheduler.cpp \
	modules/logger.pcm \
	modules/platform.pcm \
	modules/concurrency.pcm \
	modules/lockfree_queue.pcm \
	modules/lockfree_list.pcm \
	modules/event.pcm \
	modules/shared_ptr.pcm \
	modules/meta.pcm \
	modules/assert.pcm \
	modules/wait_free_serial_work.pcm

modules/logger.pcm: \
	src/logger.cpp \
	modules/platform.pcm

obj/main.o: \
	modules/sync.pcm \
	modules/logger.pcm

$(MODULES): module.modulemap

%.pcm:
	@mkdir -p $(dir $@)
	@rm -f ./deps/range-v3/include/module.modulemap
	@printf "%-8s %s\n" "[CM]" $(notdir $@)
	@$(CC) --precompile $(CFLAGS) $(DEFS) -x c++-module $< -o $@

$(OBJS): ./obj/%.o: ./src/%.cpp | $(MODULES)
	@mkdir -p $(dir $@)
	@printf "%-8s %s\n" "[CC]" $(notdir $@)
	@$(CC) -MT $@ -MMD -MP -MF $(dir $@)$(notdir $*.d) $(CFLAGS) $(DEFS) -c $< -o $@

$(BIN): $(LIBS) $(OBJS)
	@mkdir -p $(dir $@)
	@printf "%-8s %s\n" "[LD]" $(notdir $@)
	@$(CC) $(CFLAGS) $(OBJS) -o $(BIN) $(LDFLAGS)

-include $(DEPS)

mods: $(MODULES)
libs: $(LIBS)

clean:
	@rm -rf $(BIN) $(OBJS) $(DEPS) $(MODULES)

distclean:
	@rm -rf obj lib modules test/bin

