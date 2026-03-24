CXX = g++
CXXFLAGS = -std=c++17 -Iinclude -Wall -Wextra -O3 -march=native -pthread -DNDEBUG -flto -funroll-loops

# Directories
SRC_DIR = src
INC_DIR = include
BUILD_DIR = build
BIN_DIR = bin

# Source files
CLIENT_LIB_SRCS = $(SRC_DIR)/client/libflexql.cpp
SERVER_SRCS = $(SRC_DIR)/server/server.cpp $(SRC_DIR)/parser/parser.cpp $(SRC_DIR)/storage/checkpoint_manager.cpp $(SRC_DIR)/query/optimized_database.cpp
REPL_SRCS = $(SRC_DIR)/client/main.cpp
MASTER_BENCHMARK_SRCS = benchmarks/master_benchmark.cpp

# Object files
CLIENT_LIB_OBJS = $(patsubst $(SRC_DIR)/%.cpp, $(BUILD_DIR)/%.o, $(CLIENT_LIB_SRCS))
SERVER_OBJS = $(patsubst $(SRC_DIR)/%.cpp, $(BUILD_DIR)/%.o, $(SERVER_SRCS))
REPL_OBJS = $(patsubst $(SRC_DIR)/%.cpp, $(BUILD_DIR)/%.o, $(REPL_SRCS))
MASTER_BENCHMARK_OBJS = $(patsubst benchmarks/%.cpp, $(BUILD_DIR)/benchmarks/%.o, $(MASTER_BENCHMARK_SRCS))

# Targets
LIBRARY = $(BIN_DIR)/libflexql.a
SERVER = $(BIN_DIR)/flexql_server
REPL = $(BIN_DIR)/flexql_repl
MASTER_BENCHMARK = $(BIN_DIR)/flexql_master_benchmark

all: dirs $(LIBRARY) $(SERVER) $(REPL) $(MASTER_BENCHMARK)

dirs:
	@mkdir -p $(BUILD_DIR)/client $(BUILD_DIR)/server $(BUILD_DIR)/parser $(BUILD_DIR)/storage $(BUILD_DIR)/query $(BUILD_DIR)/benchmarks $(BIN_DIR)

$(LIBRARY): $(CLIENT_LIB_OBJS)
	ar rcs $@ $^

$(SERVER): $(SERVER_OBJS)
	$(CXX) $(CXXFLAGS) $^ -o $@

$(REPL): $(REPL_OBJS) $(LIBRARY)
	$(CXX) $(CXXFLAGS) $(REPL_OBJS) -L$(BIN_DIR) -lflexql -o $@

$(MASTER_BENCHMARK): $(MASTER_BENCHMARK_OBJS) $(LIBRARY)
	$(CXX) $(CXXFLAGS) $(MASTER_BENCHMARK_OBJS) -L$(BIN_DIR) -lflexql -o $@

run-benchmark: $(MASTER_BENCHMARK) $(SERVER)
	@pkill flexql_server || true
	@./$(SERVER) &
	@sleep 0.5
	@./$(MASTER_BENCHMARK) $(ARGS)

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

.PHONY: all dirs clean run-server run-repl run-benchmark

bin/flexql_bulk_ingest_benchmark: benchmarks/bulk_ingest_benchmark.cpp bin/libflexql.a
	$(CXX) $(CXXFLAGS) $< -Lbin -lflexql -o $@
