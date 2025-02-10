#!/bin/bash

# Avoids same output when files have not changed
make 2>&1 | grep -v "make: Nothing to be done for \`all'."

case "$1" in
    "ind")
        ./build/partition independent "$2" "$3" "$4"
        ;;
    "con")
        ./build/partition concurrent "$2" "$3" "$4"
        ;;
    *)
        ./build/partition
        ;;
esac