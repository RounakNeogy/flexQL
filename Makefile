CXX ?= g++
CXXFLAGS ?= -std=c++17 -O2 -Iinclude
LDFLAGS ?=
LDLIBS ?= -lpthread

BUILD_DIR := .build

CORE_SRCS := \
	src/core_types.cpp \
	src/storage_engine.cpp \
	src/table_storage.cpp \
	src/buffer_pool.cpp \
	src/row_codec.cpp \
	src/bplus_tree.cpp \
	src/bloom_filter.cpp \
	src/thread_pool.cpp \
	src/database.cpp \
	src/insert_buffer.cpp \
	src/execution_engine.cpp \
	src/query_parser.cpp \
	src/query_planner.cpp \
	src/wal_manager.cpp

SERVER_SRCS := $(CORE_SRCS) src/tcp_server.cpp src/flexql_server_main.cpp
REPL_SRCS := src/flexql_c_api.cpp src/flexql_repl_main.cpp
BENCH_SRCS := src/flexql_c_api.cpp benchmark_suite/benchmark_flexql.cpp
BENCH_AFTER_SRCS := src/flexql_c_api.cpp benchmark_suite/benchmark_after_insert.cpp
E2E_SRCS := $(CORE_SRCS) src/tcp_server.cpp src/flexql_c_api.cpp tests/c_api_e2e_demo.cpp
LAT_SRCS := $(CORE_SRCS) src/tcp_server.cpp src/flexql_c_api.cpp tests/network_latency_1m_demo.cpp

SERVER_BIN := $(BUILD_DIR)/flexql_server
REPL_BIN := $(BUILD_DIR)/flexql_repl
BENCH_BIN := $(BUILD_DIR)/benchmark_flexql
BENCH_AFTER_BIN := $(BUILD_DIR)/benchmark_after_insert
E2E_BIN := $(BUILD_DIR)/c_api_e2e_demo
LAT_BIN := $(BUILD_DIR)/network_latency_1m_demo

.PHONY: all server repl benchmark benchmark-after e2e latency unit run-server run-repl run-benchmark \
	run-benchmark-after run-benchmark-unit reset-data help clear clean

all: server repl benchmark

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

server: $(SERVER_BIN)

repl: $(REPL_BIN)

benchmark: $(BENCH_BIN)

benchmark-after: $(BENCH_AFTER_BIN)

e2e: $(E2E_BIN)

latency: $(LAT_BIN)

unit: $(BENCH_BIN)
	$(BENCH_BIN) --unit-test

run-server: $(SERVER_BIN)
	$(SERVER_BIN) $(PORT)

HOST ?= 127.0.0.1
PORT ?= 9000
ROWS ?= 200000
CLIENTS ?= 4

run-repl: $(REPL_BIN)
	$(REPL_BIN) $(HOST) $(PORT)

run-benchmark: $(BENCH_BIN)
	$(BENCH_BIN) $(ROWS) $(CLIENTS)

run-benchmark-after: $(BENCH_AFTER_BIN)
	$(BENCH_AFTER_BIN) $(ROWS) $(CLIENTS)

run-benchmark-unit: $(BENCH_BIN)
	$(BENCH_BIN) --unit-test

reset-data:
	rm -rf data/*
	mkdir -p data

help:
	@echo "Quick commands:"
	@echo "  make run-server                    # start server on PORT=9000"
	@echo "  make run-repl                      # open REPL to HOST=127.0.0.1 PORT=9000"
	@echo "  make run-benchmark                 # run benchmark with ROWS=200000"
	@echo "  make run-benchmark ROWS=10000000   # run 10M benchmark"
	@echo "  make run-benchmark-unit            # run unit tests from benchmark client"
	@echo "  make reset-data                    # wipe persisted data/"
	@echo "  make clear                         # wipe build outputs and data/"
	@echo ""
	@echo "Overrides:"
	@echo "  make run-server PORT=9010"
	@echo "  make run-repl HOST=127.0.0.1 PORT=9010"

clear: clean reset-data

$(SERVER_BIN): $(SERVER_SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS) $(LDLIBS)

READLINE_FLAGS := $(shell \
	if [ -f /opt/homebrew/opt/readline/lib/libreadline.dylib ]; then echo "-L/opt/homebrew/opt/readline/lib -lreadline"; \
	elif [ -f /opt/homebrew/lib/libreadline.dylib ]; then echo "-L/opt/homebrew/lib -lreadline"; \
	elif [ -f /usr/local/opt/readline/lib/libreadline.dylib ]; then echo "-L/usr/local/opt/readline/lib -lreadline"; \
	elif [ -f /usr/local/lib/libreadline.dylib ]; then echo "-L/usr/local/lib -lreadline"; \
	elif [ -f /usr/lib/libedit.dylib ]; then echo "-ledit"; \
	else echo "-lreadline"; \
	fi)

$(REPL_BIN): $(REPL_SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS) $(LDLIBS) $(READLINE_FLAGS)

$(BENCH_BIN): $(BENCH_SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS) $(LDLIBS)

$(BENCH_AFTER_BIN): $(BENCH_AFTER_SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS) $(LDLIBS)

$(E2E_BIN): $(E2E_SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS) $(LDLIBS)

$(LAT_BIN): $(LAT_SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS) $(LDLIBS)

clean:
	rm -rf $(BUILD_DIR)
