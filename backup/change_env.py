import sys

err_rate = sys.argv[1]
with open("Env.json","w") as env_file:
	passage = '{\n  "Env_para": {\n    "GPU_num_per_host": 10,\n    "Host_num": 1,\n    "CPU": "No_limited",\n    "High_error_card": 10,\n    "High_error_rate": ' + str(err_rate) + ',\n    "Low_error_card": 0,\n    "Low_error_rate": 0.001,\n    "Max_err_num": 0,\n    "Preempt": "False",\n    "Scale": 60,\n    "Recover_time": 10\n  }\n}'
	env_file.write(passage)
