#!/bin/bash

# Avoids same output when files have not changed
make 2>&1 | grep -v "make: Nothing to be done for [\`']all[\`']."

case "$1" in
    "ind"|"independent")
        if [ -n "$4" ]; then
            ./build/partition independent "$2" "$3" "$4"
        else
            ./build/partition independent "$2" "$3"
        fi
        ;;
    "con"|"concurrent")
        if [ -n "$4" ]; then
            ./build/partition concurrent "$2" "$3" "$4"
        else
            ./build/partition concurrent "$2" "$3"
        fi
        ;;
    *)
        ./build/partition
        ;;
esac