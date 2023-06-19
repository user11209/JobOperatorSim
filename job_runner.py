from queue import PriorityQueue
from copy import deepcopy
import random
import time

class Time_Axis:
	#yet to be implemented
	def __init__(self):
		self.PQueue = PriorityQueue()
		self.event_dict = {}
		self.top = 1
		self.PQueue.put((float("inf"),0))
		self.event_dict[0] = {"time":float("inf"),"event_type":"all_done","info":None}
	def add_to_time_axis(self,time,event_type,info):
		temp_dict = {"time":time,"event_type":event_type,"info":info}
		self.PQueue.put((time,self.top))
		self.event_dict[self.top] = temp_dict
		self.top += 1
	def pop_earliest_event(self):
		event_item = self.PQueue.get()
		event_id = event_item[1]
		event = self.event_dict[event_id]
		del self.event_dict[event_id]
		return event

class Job_Runner:
	def __init__(self,Workloads,Err,scheduler,gpu_num,env_para):
		self.time_axis = Time_Axis()
		self.time_axis.add_to_time_axis(0,"start",None)
		for err_gpu in Err:
			for time_id,err_time in enumerate(Err[err_gpu][0]):
				recover_time = Err[err_gpu][1][time_id]
				self.time_axis.add_to_time_axis(err_time,"error",err_gpu)
				self.time_axis.add_to_time_axis(err_time + recover_time + 1,"recover",err_gpu)
		self.time = 0
		self.workload = deepcopy(Workloads)
		#keep-in-memory storage of workload, all jobs can always be found here
		self.workload_keep = self.workload.copy()
		self.wait_q = {}
		self.wait_q_buffer = {}
		self.running = {}
		self.env_para = env_para
		self.free_gpu = set(range(gpu_num))
		#gpu_state: job_id->occupied by job_id; None->free; -1->error
		self.gpu_state = []
		for i in range(gpu_num):
			self.gpu_state.append(None)
		self.scheduler = scheduler
		self.finish_dict = {}
		self.left_job_num = len(Workloads)
	def get_highest_score_id(self):	
		maximum = float("-inf")
		arg_max = []
		for job_id in self.wait_q:
			if "temp_priority" in self.wait_q[job_id].keys() and self.wait_q[job_id]["temp_priority"] == 1:
				self.wait_q[job_id]["temp_priority"] = 0
				return job_id, True
			if self.wait_q[job_id]["score"] > maximum:
				arg_max = [job_id]
				maximum = self.wait_q[job_id]["score"]
			elif self.wait_q[job_id]["score"] == maximum:
				arg_max.append(job_id)
		#wait_q empty
		if len(arg_max) == 0:
			return -1, False
		#randomly select one from ones with the same score, that is, do not distinguish ones with the same score
		vote = random.randint(0,len(arg_max)-1)
		return arg_max[vote], False
	def submit_from_workload(self):
		'''
			submit from workload to wait_q_buffer
		'''
		minimum = float("inf")
		arg_min = []
		#can be faster by ordering aprior
		for job_id in self.workload:
			if self.workload[job_id]["submit_time"] < minimum:
				arg_min = [job_id]
				minimum = self.workload[job_id]["submit_time"]
			elif self.workload[job_id]["submit_time"] == minimum:
				arg_min.append(job_id)
		for job_id in arg_min:
			self.wait_q_buffer[job_id] = self.workload[job_id]
			del self.workload[job_id]
			self.workload_keep[job_id]["finish_count"] = 0
			self.workload_keep[job_id]["valid"] = 1
		return minimum,arg_min
	def terminate_job(self,job_id,reason):
		if job_id not in self.running.keys():
			self.workload_keep[job_id]["finish_count"] -= 1
			return
		job_item = self.running[job_id]
		job = job_item[0]
		occupy_list = job_item[1]
		finish_item = [job["sched_time"],self.time,occupy_list,reason]
		if reason =="None":
			if job["finish_count"] != 0:
				job["finish_count"] -= 1
				return
			self.left_job_num -= 1
			finish_item.append(job["total_time"])
		elif reason == "error" or reason == "preempted":
			job["running_time"] -= self.time - job["sched_time"]
			if job["running_time"] < 0:
				print("time: ", self.time, "; job ",job_id," scheduled at ",job["sched_time"])
				time.sleep(10)
			self.wait_q[job_id] = job
			#recover option switch
			if self.scheduler.restart(job_id,self.wait_q) == True:
				#urgently reschdule the job, use a temporary high priority
				job["temp_priority"] = 1
			#all jobs will restart at some time, so they all need to pay restart cost
			self.scheduler.restart_cost(job_id,self.wait_q)
			job["submit_time"] = self.time
			job["finish_count"] += 1
		del self.running[job_id]
		for gpu_id in occupy_list:
			self.free_gpu.add(gpu_id)
			self.gpu_state[gpu_id] = None
		if job_id in self.finish_dict:
			self.finish_dict[job_id].append(finish_item)
		else:
			self.finish_dict[job_id] = [finish_item]
	def deal_with_event(self,event):
		print("time: ", self.time, "; debug: ",event)
		event_type = event["event_type"]
		if event_type == "start":
			pass
		elif event_type == "submit":
			self.wait_q.update(self.wait_q_buffer)
			self.wait_q_buffer = {}
			submit_time,change_list = self.submit_from_workload()
			if change_list != []:
				self.time_axis.add_to_time_axis(submit_time,"submit",None)
		elif event_type == "finish":
			#MARKING: info is [job_id,occupy_list]
			info = event["info"]
			job_id = info[0]
			occupy_list = info[1]
			self.terminate_job(job_id,"None")
		elif event_type == "error":
			gpu_id = event["info"]
			job_id = self.gpu_state[gpu_id]
			if job_id != None:
				self.terminate_job(job_id,"error")
				print("time: ", self.time, "; job_id ",job_id," broken because of error on gpu ",gpu_id)
			self.free_gpu.remove(gpu_id)
			self.gpu_state[gpu_id] = -1
		elif event_type == "recover":
			gpu_id = event["info"]
			self.gpu_state[gpu_id] = None
			self.free_gpu.add(gpu_id)
		else:
			print("time: ", self.time, "; Is there another event_type? ",event_type)
		return None
	def occupy_gpu(self,occupy_list,job_id):
		'''
			start running wait_q[job_id] on gpus in occupy_list
		'''
		print("time: ", self.time, "; debug: scheduled job ",job_id,"at time ",self.time)
		print("time: ", self.time, "; and note: ' running ",self.running.keys())
		job = self.wait_q[job_id]
		job["sched_time"] = self.time
		self.running[job_id] = [job,occupy_list]
		del self.wait_q[job_id]
		for gpu_id in occupy_list:
			self.free_gpu.remove(gpu_id)
			self.gpu_state[gpu_id] = job_id
		self.time_axis.add_to_time_axis(self.time + job["running_time"],"finish",[job_id,occupy_list])
	def run_jobs(self):
		if self.wait_q_buffer == {}:
			submit_time,change_list = self.submit_from_workload()
			if change_list != []:
				self.time_axis.add_to_time_axis(submit_time,"submit",None)
		while True:
			event = self.time_axis.pop_earliest_event()
			self.time = event["time"]
			if self.time == float("inf"):
				break
			event_result = self.deal_with_event(event)
			if event_result != None:
				print("time: ", self.time, "; the result of this event is not None, how can that be?\n")
				break
			if self.left_job_num == 0:
				break
			self.scheduler.order(self.wait_q, self.running,self.env_para,8640000,self.time)
			while True:
				job_id, is_temp_priority = self.get_highest_score_id()
				if job_id == -1:
					break
				if self.wait_q[job_id]["GPU_num"] > len(self.free_gpu):
					#preemption switch
					preempted_job_id, preempt_time = self.scheduler.preempt(self.wait_q[job_id],self.running)
					if preempted_job_id != None:
						#terminate preempted jobs here
						self.terminate_job(preempted_job_id,"preempted")
						if self.wait_q[job_id]["GPU_num"] > len(self.free_gpu):
							if is_temp_priority:
								continue
							break
					else:
						if is_temp_priority:
							continue
						break
				occupy_list = self.scheduler.place(list(self.free_gpu),self.wait_q[job_id])
				self.occupy_gpu(occupy_list,job_id)
			#backfilling switch
			if (self.scheduler.backfill() == True):
				temp_wait_q_keys = self.wait_q.copy()
				for job_id in temp_wait_q_keys.keys():
					#check all jobs in wait_q, submit all that is able to be submitted
					if self.wait_q[job_id]["GPU_num"] <= len(self.free_gpu):
						occupy_list = self.scheduler.place(list(self.free_gpu),self.wait_q[job_id])
						self.occupy_gpu(occupy_list,job_id)
		return self.finish_dict	


