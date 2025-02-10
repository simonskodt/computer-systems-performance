CC = gcc
CFLAGS = -Wall -Werror -I/usr/include/openssl
LDFLAGS = -lssl -lcrypto

TARGET = partition
SOURCES = main.c partitioning.c concurrent.c independent.c
OBJECTS = $(SOURCES:.c=.o)

all: $(TARGET)

$(TARGET): $(OBJECTS)
    $(CC) $(OBJECTS) -o $(TARGET)

%.o: %.c
    $(CC) $(CFLAGS) -c $< -o $@

clean:
    rm -f $(OBJECTS) $(TARGET)

.PHONY: all clean