ASAN ?= 0
TSAN ?= 0
UBSAN ?= 0
DEBUG ?= 1
DIRS = $(sort $(dir $(wildcard ./src/*/), ./src/))
SRCS = $(foreach dir,$(DIRS),$(wildcard $(dir)*.cpp))
OBJS = $(SRCS:./src/%.cpp=./obj/%.o)
ASM = $(SRCS:./src/%.cpp=./asm/%.S)
DEPS = $(OBJS:%.o=%.d)
CC = clang++-16
OBJDUMP = llvm-objdump-16
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
	-fverbose-asm \
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
	-save-temps \
	$(ASAN_LDFLAGS) \
	$(TSAN_LDFLAGS)

MODNAMES = \
	sync \
	sync-scheduler \
	sync-worker_pool \
	sync-system_tasks \
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
	lockfree_iterable_list \
	snap_collector \
	tls \
	hazard_ptr \
	atomic_work \
	engine \
	event_pumper \
	lockfree_deque \
	atomic_trace \
	atomic_bitset \
	lockfree_sequenced_queue \
	alloc \
	lockfree_stack \
	static_stack \
	atomic_struct

TEST_DIR = ./test
TEST_SRCS = $(wildcard $(TEST_DIR)/*.cpp)
TEST_BINS = $(TEST_SRCS:./test/%.cpp=./test/bin/%)

MODULES = $(MODNAMES:%=modules/%.pcm)

.PHONY: all tests libs mods clean distclean
all: $(BIN)
tests: $(TEST_BINS)

lib/$(SDL2_LIB):
	@mkdir -p $(dir $@)
	@mkdir -p $(SDL2_SRC)/build
	@cd $(SDL2_SRC)/build \
		&& ../configure \
		&& make
	@cp $(SDL2_SRC)/build/build/.libs/$(SDL2_LIB) $@

modules/static_stack.pcm: \
	src/static_stack.cpp \
	modules/assert.pcm

modules/alloc.pcm: \
	src/alloc.cpp \
	modules/platform.pcm \
	modules/concurrency.pcm \
	modules/logger.pcm \
	modules/meta.pcm \
	modules/static_stack.pcm

modules/lockfree_stack.pcm: \
	src/lockfree_stack.cpp \
	modules/platform.pcm \
	modules/concurrency.pcm \
	modules/meta.pcm

modules/lockfree_sequenced_queue.pcm: \
	src/lockfree_sequenced_queue.cpp \
	modules/platform.pcm \
	modules/concurrency.pcm \
	modules/lockfree_list.pcm \
	modules/atomic_work.pcm \
	modules/assert.pcm \
	modules/shared_ptr.pcm \
	modules/meta.pcm

modules/atomic_bitset.pcm: \
	src/atomic_bitset.cpp \
	modules/shared_ptr.pcm \
	modules/snap_collector.pcm \
	modules/assert.pcm

modules/atomic_trace.pcm: \
	src/atomic_trace.cpp \
	modules/tls.pcm \
	modules/logger.pcm \
	modules/platform.pcm \
	modules/shared_ptr.pcm

modules/lockfree_deque.pcm: \
	src/lockfree_deque.cpp \
	modules/platform.pcm \
	modules/concurrency.pcm \
	modules/logger.pcm \
	modules/hazard_ptr.pcm \
	modules/atomic_trace.pcm

modules/atomic_work.pcm: \
	src/atomic_work.cpp \
	modules/concurrency.pcm \
	modules/shared_ptr.pcm \
	modules/assert.pcm \
	modules/hazard_ptr.pcm \
	modules/logger.pcm \
	modules/platform.pcm \
	modules/lockfree_iterable_list.pcm

modules/hazard_ptr.pcm: \
	src/hazard_ptr.cpp \
	modules/platform.pcm \
	modules/logger.pcm \
	modules/tls.pcm \
	modules/assert.pcm

modules/lockfree_iterable_list.pcm: \
	src/lockfree_iterable_list.cpp \
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

modules/event_pumper.pcm: \
	src/event_pumper.cpp \
	modules/sync.pcm \
	modules/logger.pcm \
	modules/event.pcm

modules/engine.pcm: \
	src/engine.cpp \
	modules/sync.pcm \
	modules/event_pumper.pcm \
	modules/logger.pcm \
	modules/event.pcm

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
	src/event.cpp \
	modules/shared_ptr.pcm

modules/lockfree_queue.pcm: \
	src/lockfree_queue.cpp \
	modules/concurrency.pcm \
	modules/platform.pcm \
	modules/hazard_ptr.pcm \
	modules/assert.pcm

modules/platform.pcm: \
	src/platform.cpp

modules/concurrency.pcm: \
	src/concurrency.cpp \
	modules/platform.pcm \
	modules/logger.pcm \
	modules/assert.pcm

modules/sync.pcm: \
	src/sync.cpp \
	modules/sync-scheduler.pcm \
	modules/sync-system_tasks.pcm \
	modules/concurrency.pcm \
	modules/logger.pcm \
	modules/meta.pcm \
	modules/platform.pcm

modules/sync-scheduler.pcm: \
	src/scheduler.cpp \
	modules/sync-worker_pool.pcm \
	modules/logger.pcm \
	modules/platform.pcm \
	modules/concurrency.pcm \
	modules/lockfree_queue.pcm \
	modules/lockfree_iterable_list.pcm \
	modules/event.pcm \
	modules/shared_ptr.pcm \
	modules/meta.pcm \
	modules/assert.pcm \
	modules/atomic_work.pcm \
	modules/lockfree_sequenced_queue.pcm

modules/sync-worker_pool.pcm: \
	src/worker_pool.cpp \
	modules/lockfree_deque.pcm \
	modules/lockfree_queue.pcm \
	modules/shared_ptr.pcm \
	modules/assert.pcm \
	modules/atomic_bitset.pcm \
	modules/platform.pcm

modules/sync-system_tasks.pcm: \
	src/system_tasks.cpp \
	modules/sync-scheduler.pcm \
	modules/logger.pcm \
	modules/event.pcm

modules/logger.pcm: \
	src/logger.cpp \
	modules/platform.pcm

modules/atomic_struct.pcm: \
	src/atomic_struct.cpp \
	modules/shared_ptr.pcm \
	modules/assert.pcm \
	modules/logger.pcm

obj/main.o: $(MODULES)

$(MODULES): module.modulemap

%.pcm:
	@mkdir -p $(dir $@)
	@printf "%-8s %s\n" "[CM]" $(notdir $@)
	@$(CC) --precompile $(CFLAGS) $(DEFS) -x c++-module $< -o $@

.PRECIOUS: ./obj/test/%.o
./obj/test/%.o: ./test/%.cpp $(MODULES)
	@mkdir -p $(dir $@)
	@printf "%-8s %s\n" "[CC]" $(notdir $@)
	@$(CC) -MT $@ -MMD -MP -MF $(dir $@)$(notdir $*.d) $(CFLAGS) $(DEFS) -c $< -o $@

test/bin/%: ./obj/test/%.o $(LIBS) $(filter-out ./obj/main.o, $(OBJS))
	@mkdir -p $(dir $@)
	@printf "%-8s %s\n" "[LD]" $@
	@$(CC) $(CFLAGS) $< $(filter-out ./obj/main.o, $(OBJS)) -o $@ $(LDFLAGS)

$(ASM): ./asm/%.S: ./obj/%.o
	@mkdir -p $(dir $@)
	@printf "%-8s %s\n" "[DS]" $@
	@$(OBJDUMP) -S -C $< > $@

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
asm: $(ASM)

clean:
	@rm -rf $(BIN) $(OBJS) $(DEPS) $(MODULES)

distclean:
	@rm -rf obj lib modules test/bin

