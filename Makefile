CXX = g++
CXXFLAGS = -std=c++17 -Iinclude -Wall -Wextra -O3 -pthread

# Directories
SRC_DIR = src
INC_DIR = include
BUILD_DIR = build
BIN_DIR = bin

# Source files
CLIENT_LIB_SRCS = $(SRC_DIR)/client/libflexql.cpp
SERVER_SRCS = $(SRC_DIR)/server/server.cpp $(SRC_DIR)/parser/parser.cpp
REPL_SRCS = $(SRC_DIR)/client/main.cpp
BENCHMARK_SRCS = benchmarks/benchmark.cpp
QUERY_BENCHMARK_SRCS = benchmarks/query_benchmark.cpp
JOIN_BENCHMARK_SRCS = benchmarks/join_benchmark.cpp

# Object files
CLIENT_LIB_OBJS = $(patsubst $(SRC_DIR)/%.cpp, $(BUILD_DIR)/%.o, $(CLIENT_LIB_SRCS))
SERVER_OBJS = $(patsubst $(SRC_DIR)/%.cpp, $(BUILD_DIR)/%.o, $(SERVER_SRCS))
REPL_OBJS = $(patsubst $(SRC_DIR)/%.cpp, $(BUILD_DIR)/%.o, $(REPL_SRCS))
BENCHMARK_OBJS = $(patsubst benchmarks/%.cpp, $(BUILD_DIR)/benchmarks/%.o, $(BENCHMARK_SRCS))
QUERY_BENCHMARK_OBJS = $(patsubst benchmarks/%.cpp, $(BUILD_DIR)/benchmarks/%.o, $(QUERY_BENCHMARK_SRCS))
JOIN_BENCHMARK_OBJS = $(patsubst benchmarks/%.cpp, $(BUILD_DIR)/benchmarks/%.o, $(JOIN_BENCHMARK_SRCS))

# Targets
LIBRARY = $(BIN_DIR)/libflexql.a
SERVER = $(BIN_DIR)/flexql_server
REPL = $(BIN_DIR)/flexql_repl
BENCHMARK = $(BIN_DIR)/flexql_benchmark
QUERY_BENCHMARK = $(BIN_DIR)/flexql_query_benchmark
JOIN_BENCHMARK = $(BIN_DIR)/flexql_join_benchmark

all: dirs $(LIBRARY) $(SERVER) $(REPL) $(BENCHMARK) $(QUERY_BENCHMARK) $(JOIN_BENCHMARK)

dirs:
	@mkdir -p $(BUILD_DIR)/client $(BUILD_DIR)/server $(BUILD_DIR)/benchmarks $(BIN_DIR)

$(LIBRARY): $(CLIENT_LIB_OBJS)
	ar rcs $@ $^

$(SERVER): $(SERVER_OBJS)
	$(CXX) $(CXXFLAGS) $^ -o $@

$(REPL): $(REPL_OBJS) $(LIBRARY)
	$(CXX) $(CXXFLAGS) $(REPL_OBJS) -L$(BIN_DIR) -lflexql -o $@

$(BENCHMARK): $(BENCHMARK_OBJS) $(LIBRARY)
	$(CXX) $(CXXFLAGS) $(BENCHMARK_OBJS) -L$(BIN_DIR) -lflexql -o $@

$(QUERY_BENCHMARK): $(QUERY_BENCHMARK_OBJS) $(LIBRARY)
	$(CXX) $(CXXFLAGS) $(QUERY_BENCHMARK_OBJS) -L$(BIN_DIR) -lflexql -o $@

$(JOIN_BENCHMARK): $(JOIN_BENCHMARK_OBJS) $(LIBRARY)
	$(CXX) $(CXXFLAGS) $(JOIN_BENCHMARK_OBJS) -L$(BIN_DIR) -lflexql -o $@

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD_DIR)/benchmarks/%.o: benchmarks/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -rf $(BUILD_DIR) $(BIN_DIR)

run-server: $(SERVER)
	@pkill flexql_server || true
	@./$(SERVER)

run-repl: $(REPL)
	@./$(REPL)

.PHONY: all dirs clean run-server run-repl

bin/flexql_bulk_ingest_benchmark: benchmarks/bulk_ingest_benchmark.cpp bin/libflexql.a
	$(CXX) $(CXXFLAGS) $< -Lbin -lflexql -o $@
