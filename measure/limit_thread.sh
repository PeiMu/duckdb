#!/bin/bash

# Create a new cgroup named "mygroup"
sudo mkdir -p /sys/fs/cgroup/mygroup

# Set the cgroup to use only CPU 0 (the first CPU core)
echo 0 | sudo tee /sys/fs/cgroup/mygroup/cpuset.cpus

# Set the memory nodes to use (typically 0)
echo 0 | sudo tee /sys/fs/cgroup/mygroup/cpuset.mems

# Add the current shell process to the cgroup (or replace $$ with the desired PID)
echo $$ | sudo tee /sys/fs/cgroup/mygroup/cgroup.procs

echo "CPU limit applied. Processes in 'mygroup' cgroup are restricted to CPU 0."

