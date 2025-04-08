CC := gcc
CFLAGS := -Wall -Werror -O2 -I/opt/homebrew/Cellar/duckdb/1.2.1/include
LDFLAGS := -L/opt/homebrew/Cellar/duckdb/1.2.1/lib -lduckdb

SRC_DIR := src
BUILD_DIR := build
INCLUDE_DIR := include

TARGET := partition
SOURCES := $(wildcard $(SRC_DIR)/*.c)
OBJECTS := $(SOURCES:$(SRC_DIR)/%.c=$(BUILD_DIR)/%.o)

ifdef AFFINITY
CFLAGS += -DAFFINITY
endif

.DEFAULT_GOAL := all

# Targets
all: $(BUILD_DIR)/$(TARGET)

$(BUILD_DIR)/$(TARGET): $(OBJECTS)
	@mkdir -p $(dir $@)
	$(CC) $(OBJECTS) $(LDFLAGS) -o $@

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

run: $(BUILD_DIR)/$(TARGET)
	./$(BUILD_DIR)/$(TARGET) $(ARGS)

clean:
	rm -rf $(BUILD_DIR)

.PHONY: all run clean