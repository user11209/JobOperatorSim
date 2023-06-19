#!/bin/bash

#err_rate_list=(0 0.001 0.002 0.003 0.004 0.005 0.006 0.007 0.008)
err_rate_list=(0.004)

for err_rate in ${err_rate_list[*]}
do
	for i in $(seq 1 1)
	do
		#python3 timing_workload_generator.py
		for j in $(seq 1 1)
		do
			echo -e " $err_rate\c"
			python3 change_env.py $err_rate
			python3 Simulator.py 0
		done
	done
done
