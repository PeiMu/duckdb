#!/bin/bash
set -e

# from https://llvm.org/docs/Benchmarking.html
echo 0 > /proc/sys/kernel/randomize_va_space

#cset shield --cpu=1 -k on

for i in /sys/devices/system/cpu/cpu*/online
do
  echo 0 > $i
done

echo 1 > /sys/devices/system/cpu/intel_pstate/no_turbo

for i in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
do
  echo performance > $i
done

