CXX      := g++
CXXFLAGS := -std=c++17 -O3 -march=native -funroll-loops -ffast-math \
            -Wall -Wextra -Wno-unused-parameter \
            -Iinclude
LDFLAGS  := -lpthread

# ─── Directories ─────────────────────────────────────────────────────────────
BUILDDIR := build
BINDIR   := bin

# ─── Sources ─────────────────────────────────────────────────────────────────
STORAGE_SRCS   := src/storage/pager.cpp \
                  src/storage/row.cpp   \
                  src/storage/table.cpp

INDEX_SRCS     := src/index/bptree.cpp

CACHE_SRCS     :=   # LRU is inside Pager

PARSER_SRCS    := src/parser/parser.cpp

QUERY_SRCS     := src/query/executor.cpp

CONCURR_SRCS   := src/concurrency/thread_pool.cpp

NETWORK_SRCS   := src/network/protocol.cpp

COMMON_SRCS    := $(STORAGE_SRCS) $(INDEX_SRCS) $(PARSER_SRCS) \
                  $(QUERY_SRCS) $(CONCURR_SRCS) $(NETWORK_SRCS)

SERVER_SRCS    := src/server/main.cpp
CLIENT_SRCS    := src/client/main.cpp        src/network/client.cpp
BENCH_SRCS     := benchmarks/benchmark_flexql.cpp src/network/client.cpp

# ─── Object files ────────────────────────────────────────────────────────────
COMMON_OBJS := $(COMMON_SRCS:%.cpp=$(BUILDDIR)/%.o)
SERVER_OBJS := $(SERVER_SRCS:%.cpp=$(BUILDDIR)/%.o)
CLIENT_OBJS := $(CLIENT_SRCS:%.cpp=$(BUILDDIR)/%.o)
BENCH_OBJS  := $(BENCH_SRCS:%.cpp=$(BUILDDIR)/%.o)

ALL_OBJS := $(COMMON_OBJS) $(SERVER_OBJS) $(CLIENT_OBJS) $(BENCH_OBJS)

# ─── Targets ─────────────────────────────────────────────────────────────────
.PHONY: all server client benchmark clean dirs

all: dirs server client benchmark

server: dirs $(BINDIR)/flexql_server

client: dirs $(BINDIR)/flexql_client

benchmark: dirs $(BINDIR)/flexql_master_benchmark

$(BINDIR)/flexql_server: $(COMMON_OBJS) $(SERVER_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "  ✓ Built $@"

$(BINDIR)/flexql_client: $(COMMON_OBJS) $(CLIENT_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "  ✓ Built $@"

$(BINDIR)/flexql_master_benchmark: $(COMMON_OBJS) $(BENCH_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "  ✓ Built $@"

run-server: server
	./$(BINDIR)/flexql_server

run-client: client
	./$(BINDIR)/flexql_client

# ─── Compilation rule ────────────────────────────────────────────────────────
$(BUILDDIR)/%.o: %.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c -o $@ $<

# ─── Helpers ─────────────────────────────────────────────────────────────────
dirs:
	@mkdir -p $(BINDIR) $(BUILDDIR)
	@mkdir -p data/tables data/indexes data/wal

clean:
	rm -rf $(BUILDDIR) $(BINDIR)
	@echo "  ✓ Cleaned"

purge: clean
	rm -rf data/tables/* data/indexes/* data/wal/*
	@echo "  ✓ Data purged"
