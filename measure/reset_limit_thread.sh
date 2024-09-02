#!/bin/bash

# Reset the cpuset.cpus to allow all available CPUs (adjust based on your system)
echo 0-11 | sudo tee /sys/fs/cgroup/mygroup/cpuset.cpus

# Reset cpuset.mems to the default value (typically 0)
echo 0 | sudo tee /sys/fs/cgroup/mygroup/cpuset.mems

# Move the current shell process out of the cgroup (or replace $$ with the desired PID)
echo $$ | sudo tee /sys/fs/cgroup/cgroup.procs

# Remove the cgroup directory
sudo rmdir /sys/fs/cgroup/mygroup

echo "CPU limit reset and 'mygroup' cgroup removed."

