ASAN ?= 0
TSAN ?= 0
DIRS = $(sort $(dir $(wildcard ./src/*/), ./src/))
SRCS = $(foreach dir,$(DIRS),$(wildcard $(dir)*.cpp))
OBJS = $(SRCS:./src/%.cpp=./obj/%.o)
DEPS = $(OBJS:%.o=%.d)
CC = clang++-16
AR = ar
BIN = pe

INCLUDES = \
	-Isrc

LIBS =

DEFS =

ifneq ($(ASAN),0)
ASAN_CFLAGS = -fsanitize=address -static-libsan
ASAN_LDFLAGS = -fsanitize=address -static-libsan
endif

ifneq ($(TSAN),0)
TSAN_CFLAGS = -fsanitize=thread -static-libsan
TSAN_LDFLAGS = -fsanitize=thread -static-libsan
endif

CFLAGS = \
	-std=c++20 \
	-stdlib=libc++ \
	-fmodules \
	-fmodule-map-file=module.modulemap \
	-Wall \
	-Werror \
	-pedantic \
	-O3 \
	-g \
	$(ASAN_CFLAGS) \
	$(TSAN_CFLAGS) \
	$(INCLUDES)

LDFLAGS = -L./lib \
	$(LIBS:./lib/%=-l:%) \
	-lstdc++ \
	-ldl \
	-lpthread \
	-lm \
	-lvulkan \
	-no-pie \
	-flto \
	$(ASAN_LDFLAGS) \
	$(TSAN_LDFLAGS)

MODNAMES = \
	scheduler \
	task

MODULES = $(MODNAMES:%=modules/%.pcm)

.PHONY: all
all: $(BIN)

modules/scheduler.pcm: \
	src/scheduler.cpp

modules/task.pcm: \
	src/task.cpp

obj/main.o: \
	modules/scheduler.pcm

$(MODULES): module.modulemap

%.pcm:
	@mkdir -p $(dir $@)
	@rm -f ./deps/range-v3/include/module.modulemap
	@printf "%-8s %s\n" "[CM]" $(notdir $@)
	@$(CC) $(CFLAGS) $(DEFS) -fprebuilt-module-path=modules -Xclang -emit-module-interface -c $< -o $@

$(OBJS): ./obj/%.o: ./src/%.cpp | $(MODULES)
	@mkdir -p $(dir $@)
	@printf "%-8s %s\n" "[CC]" $@
	@$(CC) -MT $@ -MMD -MP -MF $(dir $@)$(notdir $*.d) $(CFLAGS) -fprebuilt-module-path=modules $(DEFS) -c $< -o $@

$(BIN): $(LIBS) $(OBJS)
	@mkdir -p $(dir $@)
	@printf "%-8s %s\n" "[LD]" $@
	@$(CC) $(CFLAGS) -fprebuilt-module-path=modules $(OBJS) -o $(BIN) $(LDFLAGS)

-include $(DEPS)

.PHONY: clean distclean libs mods

mods: $(MODULES)
libs: $(LIBS)

clean:
	rm -rf $(BIN) $(OBJS) $(DEPS) $(MODULES)

distclean:
	rm -rf obj lib modules

