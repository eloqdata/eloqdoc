# Print the full stack trace on python exceptions to aid debugging
set python print-stack full

# Print all elements of arrays and strings without truncating
set print elements 0
set print length 0

# Load the mongodb utilities
source buildscripts/gdb/mongo.py

# Load the mongodb pretty printers
source buildscripts/gdb/mongo_printers.py

# Load the mongodb lock analysis
source buildscripts/gdb/mongo_lock.py
