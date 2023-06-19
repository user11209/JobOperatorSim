import random
import numpy as np
import copy
import math

#my algorithm
class demo_scheduler:
    def __init__(self):
        pass
    def init_workflow(self, workloads):
        return workloads.keys()

    def order(self, wait_q, running,env_para,priority_weight,time):
        for job_id in wait_q.keys():
            wait_q[job_id]["score"] = int(job_id)
        for job_id in running.keys():
            running[job_id][0]["score"] = int(running[job_id][0]["job_ID"])

    def place(self, free_gpu_list, job_info):
        # 放置策略
        # e.g. 随机放置策略：
        num = job_info["GPU_num"]
        gpu_ids = random.sample(free_gpu_list, num)
        return gpu_ids

    def restart(self,job_id,wait_q):
        # 出错重启策略
        # e.g. 不重启:
        return False
        if "restart" not in wait_q[job_id].keys():
            wait_q[job_id]["restart"] = 0
        wait_q[job_id]["restart"] += 1
        if wait_q[job_id]["restart"] % 3 == 0:
            return False
        print("restarting now\n")
        return True

    def restart_cost(self,job_id,wait_q):
        print("resetting job_id %s, running_time %d" % (job_id, wait_q[job_id]["running_time"]))#debug zhang
        rest_time = math.ceil(wait_q[job_id]["running_time"] / wait_q[job_id]["epoch_time"]) * wait_q[job_id]["epoch_time"]
        wait_q[job_id]["total_time"] += rest_time - wait_q[job_id]["running_time"]
        wait_q[job_id]["running_time"] = rest_time
        return True

    def preempt(self,job_info, running):
        # 是否抢占（动态调度）, 抢占返回True，不抢占返回False
        preempt_flag = True
        if preempt_flag == False:
            return None,None
        else:
            for keys in running:
                if running[keys][0]["score"] < job_info["score"] and running[keys][0]["GPU_num"] > job_info["GPU_num"]:
                    return keys, job_info["submit_time"]
            return None, None

    def backfill(self):
        return False

#smallest area first
class baseline1_scheduler:
    def __init__(self):
        pass
    def init_workflow(self, workloads):
        return workloads.keys()
    '''
    def GPU_time_predict(self,GPU_num,running_time,epoch_time,env_para,err_rate):
        #err_rate = (env_para["High_error_rate"]*env_para["High_error_card"] + env_para["Low_error_rate"]*env_para["Low_error_card"]) / env_para["GPU_num_per_host"] / env_para["Host_num"]
        if err_rate == 0:
            return GPU_num*running_time
        lamda = -math.log(1-err_rate) / env_para["Scale"]
        return running_time / epoch_time / lamda * (math.pow(1-err_rate, -GPU_num*epoch_time/env_para["Scale"]) - 1)
    def order(self, wait_q, workload,env_para,priority_weight,time):
        # 作业得分策略，即排队策略，高分代表高优先级，得分必须是正整数。
        # 可更新所有作业的score，包括wait_q & workloads。也可以只更新wait_q中作业得分。
        err_rate = (env_para["High_error_rate"]*env_para["High_error_card"] + env_para["Low_error_rate"]*env_para["Low_error_card"]) / env_para["GPU_num_per_host"] / env_para["Host_num"]
        lamda = -math.log(1-err_rate) / env_para["Scale"] #此处用到scale = 60
        for job_id in workload.keys():
            if "priority" not in workload[job_id]:
                workload[job_id]["priority"] = workload[job_id]["score"]
                workload[job_id]["init_submit_time"] = workload[job_id]["submit_time"]
            temp_dict = workload[job_id]
            time_pre = self.GPU_time_predict(temp_dict["GPU_num"],temp_dict["running_time"],temp_dict["epoch_time"],env_para,err_rate)
            #The threshold needs verification
            #if time_pre < 1000000:
            #    temp_dict["score"] = temp_dict["priority"]*priority_weight - time_pre
            #else:
                #TO DO: priority = B(t) - A(t)*T, currently set B(t) = t, A(t) - 1/(t+1)
            temp_dict["score"] = temp_dict["priority"]*priority_weight - time_pre
        for job_id in wait_q.keys():
            if "priority" not in wait_q[job_id]:
                wait_q[job_id]["priority"] = wait_q[job_id]["score"]
                wait_q[job_id]["init_submit_time"] = wait_q[job_id]["submit_time"]
            temp_dict = wait_q[job_id]
            time_pre = self.GPU_time_predict(temp_dict["GPU_num"],temp_dict["running_time"],temp_dict["epoch_time"],env_para,err_rate)
            #The threshold needs verification
            #if time_pre < 1000000:
            #    temp_dict["score"] = temp_dict["priority"]*priority_weight - time_pre
            #else:
                #TO DO: priority = B(t) - A(t)*T, currently set B(t) = t, A(t) - 1/(t+1)
            temp_dict["score"] = temp_dict["priority"]*priority_weight - time_pre
    '''
    def order(self, wait_q, workload,env_para,priority_weight,time):
        # 作业得分策略，即排队策略，高分代表高优先级，得分必须是正整数。
        # 可更新所有作业的score，包括wait_q & workloads。也可以只更新wait_q中作业得分。
        # e.g. 每次都给wait_q中的所有作业得分*1.5倍：
        if len(workload) != 0:
            for job_id in workload.keys():
                if "priority" not in workload[job_id]:
                    workload[job_id]["priority"] = workload[job_id]["score"]
                workload[job_id]["score"] = workload[job_id]["priority"]*priority_weight - workload[job_id]["running_time"]*workload[job_id]["GPU_num"]
        if len(wait_q) != 0:
            for job_id in wait_q.keys():
                if "priority" not in wait_q[job_id]:
                    wait_q[job_id]["priority"] = wait_q[job_id]["score"]
                wait_q[job_id]["score"] = wait_q[job_id]["priority"]*priority_weight - wait_q[job_id]["running_time"]*wait_q[job_id]["GPU_num"]


    def place(sef, free_gpu_list, job_info):
        # 放置策略
        # e.g. 随机放置策略：
        num = job_info["GPU_num"]
        gpu_ids = random.sample(free_gpu_list, num)

        return gpu_ids

    def restart(self,job_id,wait_q):
        # 出错重启策略
        # e.g. 不重启:
        # print("Debug from zhang--job id "+job_id+" is broken")
    #         # if wait_q[job_id]["restart"] % 3 == 0 and wait_q[job_id]["restart"] != 0:
    #         #     return False
    #         # print("restarting now")
    #         # return True
        return False

    def restart_cost(self,job_id,wait_q):
        print("resetting job_id %s, running_time %d" % (job_id, wait_q[job_id]["running_time"]))#debug zhang
        rest_time = math.ceil(wait_q[job_id]["running_time"] / wait_q[job_id]["epoch_time"]) * wait_q[job_id]["epoch_time"]
        wait_q[job_id]["total_time"] += rest_time - wait_q[job_id]["running_time"]
        wait_q[job_id]["running_time"] = rest_time
        return True

    def preempt(self,job_info, running):
        # 是否抢占（动态调度）, 抢占返回True，不抢占返回False
        preempt_flag = False
        if preempt_flag == False:
            return None,None,None
        else:
            for keys in running:
                if running[keys][0]["score"] > job_info["score"] and running[keys][0]["GPU_num"] > job_info["GPU_num"]:
                    return keys, job_info["submit_time"]
            return None, None, None

    def backfill(self):
        return False

#shortest job first
class baseline2_scheduler:
    def __init__(self):
        pass
    def init_workflow(self, workloads):
        return workloads.keys()

    def order(self, wait_q, workload,env_para,priority_weight,time):
        # 作业得分策略，即排队策略，高分代表高优先级，得分必须是正整数。
        # 可更新所有作业的score，包括wait_q & workloads。也可以只更新wait_q中作业得分。
        # e.g. 每次都给wait_q中的所有作业得分*1.5倍：
        if len(workload) != 0:
            for job_id in workload.keys():
                if "priority" not in workload[job_id]:
                    workload[job_id]["priority"] = workload[job_id]["score"]
                workload[job_id]["score"] = workload[job_id]["priority"]*priority_weight - workload[job_id]["running_time"]
        if len(wait_q) != 0:
            for job_id in wait_q.keys():
                if "priority" not in wait_q[job_id]:

                    wait_q[job_id]["priority"] = wait_q[job_id]["score"]
                wait_q[job_id]["score"] = wait_q[job_id]["priority"]*priority_weight - wait_q[job_id]["running_time"]


    def place(sef, free_gpu_list, job_info):
        # 放置策略
        # e.g. 随机放置策略：
        num = job_info["GPU_num"]
        gpu_ids = random.sample(free_gpu_list, num)

        return gpu_ids

    def restart(self,job_id,wait_q):
        # 出错重启策略
        # e.g. 不重启:
        # print("Debug from zhang--job id "+job_id+" is broken")
    #         # if wait_q[job_id]["restart"] % 3 == 0 and wait_q[job_id]["restart"] != 0:
    #         #     return False
    #         # print("restarting now")
    #         # return True
        return False

    def restart_cost(self,job_id,wait_q):
        print("resetting job_id %s, running_time %d" % (job_id, wait_q[job_id]["running_time"]))#debug zhang
        rest_time = math.ceil(wait_q[job_id]["running_time"] / wait_q[job_id]["epoch_time"]) * wait_q[job_id]["epoch_time"]
        wait_q[job_id]["total_time"] += rest_time - wait_q[job_id]["running_time"]
        wait_q[job_id]["running_time"] = rest_time
        return True

    def preempt(self,job_info, running):
        # 是否抢占（动态调度）, 抢占返回True，不抢占返回False
        preempt_flag = False
        if preempt_flag == False:
            return None,None,None
        else:
            for keys in running:
                if running[keys][0]["score"] > job_info["score"] and running[keys][0]["GPU_num"] > job_info["GPU_num"]:
                    return keys, job_info["submit_time"]
            return None, None, None

    def backfill(self):
        return False

#lest resource
class baseline3_scheduler:
    def __init__(self):
        pass
    def init_workflow(self, workloads):
        return workloads.keys()

    def order(self, wait_q, workload,env_para,priority_weight,time):
        # 作业得分策略，即排队策略，高分代表高优先级，得分必须是正整数。
        # 可更新所有作业的score，包括wait_q & workloads。也可以只更新wait_q中作业得分。
        # e.g. 每次都给wait_q中的所有作业得分*1.5倍：
        if len(workload) != 0:
            for job_id in workload.keys():
                if "priority" not in workload[job_id]:
                    workload[job_id]["priority"] = workload[job_id]["score"]
                workload[job_id]["score"] = workload[job_id]["priority"]*priority_weight - workload[job_id]["GPU_num"]
        if len(wait_q) != 0:
            for job_id in wait_q.keys():
                if "priority" not in wait_q[job_id]:

                    wait_q[job_id]["priority"] = wait_q[job_id]["score"]
                wait_q[job_id]["score"] = wait_q[job_id]["priority"]*priority_weight - wait_q[job_id]["GPU_num"]


    def place(sef, free_gpu_list, job_info):
        # 放置策略
        # e.g. 随机放置策略：
        num = job_info["GPU_num"]
        gpu_ids = random.sample(free_gpu_list, num)

        return gpu_ids

    def restart(self,job_id,wait_q):
        # 出错重启策略
        # e.g. 不重启:
        # print("Debug from zhang--job id "+job_id+" is broken")
    #         # if wait_q[job_id]["restart"] % 3 == 0 and wait_q[job_id]["restart"] != 0:
    #         #     return False
    #         # print("restarting now")
    #         # return True
        return False

    def restart_cost(self,job_id,wait_q):
        print("resetting job_id %s, running_time %d" % (job_id, wait_q[job_id]["running_time"]))#debug zhang
        rest_time = math.ceil(wait_q[job_id]["running_time"] / wait_q[job_id]["epoch_time"]) * wait_q[job_id]["epoch_time"]
        wait_q[job_id]["total_time"] += rest_time - wait_q[job_id]["running_time"]
        wait_q[job_id]["running_time"] = rest_time
        return True

    def preempt(self,job_info, running):
        # 是否抢占（动态调度）, 抢占返回True，不抢占返回False
        preempt_flag = False
        if preempt_flag == False:
            return None,None,None
        else:
            for keys in running:
                if running[keys][0]["score"] > job_info["score"] and running[keys][0]["GPU_num"] > job_info["GPU_num"]:
                    return keys, job_info["submit_time"]
            return None, None, None

    def backfill(self):
        return False

#random
class baseline4_scheduler:
    def __init__(self):
        pass
    def init_workflow(self, workloads):
        return workloads.keys()

    def order(self, wait_q, workload,env_para,priority_weight,time):
        # 作业得分策略，即排队策略，高分代表高优先级，得分必须是正整数。
        # 可更新所有作业的score，包括wait_q & workloads。也可以只更新wait_q中作业得分。
        # e.g. 每次都给wait_q中的所有作业得分*1.5倍：
        if len(workload) != 0:
            for job_id in workload.keys():
                if "priority" not in workload[job_id]:
                    workload[job_id]["priority"] = workload[job_id]["score"]
                    workload[job_id]["score"] = workload[job_id]["priority"]*priority_weight - random.random()

        if len(wait_q) != 0:
            for job_id in wait_q.keys():
                if "priority" not in wait_q[job_id]:
                    wait_q[job_id]["priority"] = wait_q[job_id]["score"]
                    wait_q[job_id]["score"] = wait_q[job_id]["priority"]*priority_weight - random.random()


    def place(sef, free_gpu_list, job_info):
        # 放置策略
        # e.g. 随机放置策略：
        num = job_info["GPU_num"]
        gpu_ids = random.sample(free_gpu_list, num)

        return gpu_ids

    def restart(self,job_id,wait_q):
        # 出错重启策略
        # e.g. 不重启:
        # print("Debug from zhang--job id "+job_id+" is broken")
    #         # if wait_q[job_id]["restart"] % 3 == 0 and wait_q[job_id]["restart"] != 0:
    #         #     return False
    #         # print("restarting now")
    #         # return True
        return False

    def restart_cost(self,job_id,wait_q):
        print("resetting job_id %s, running_time %d" % (job_id, wait_q[job_id]["running_time"]))#debug zhang
        rest_time = math.ceil(wait_q[job_id]["running_time"] / wait_q[job_id]["epoch_time"]) * wait_q[job_id]["epoch_time"]
        wait_q[job_id]["total_time"] += rest_time - wait_q[job_id]["running_time"]
        wait_q[job_id]["running_time"] = rest_time
        return True

    def preempt(self,job_info, running):
        # 是否抢占（动态调度）, 抢占返回True，不抢占返回False
        preempt_flag = False
        if preempt_flag == False:
            return None,None,None
        else:
            for keys in running:
                if running[keys][0]["score"] > job_info["score"] and running[keys][0]["GPU_num"] > job_info["GPU_num"]:
                    return keys, job_info["submit_time"]
            return None, None, None

    def backfill(self):
        return False

#fcfs
class baseline5_scheduler:
    def __init__(self):
        pass
    def init_workflow(self, workloads):
        return workloads.keys()

    def order(self, wait_q, workload,env_para,priority_weight,time):
        # 作业得分策略，即排队策略，高分代表高优先级，得分必须是正整数。
        # 可更新所有作业的score，包括wait_q & workloads。也可以只更新wait_q中作业得分。
        # e.g. 每次都给wait_q中的所有作业得分*1.5倍：
        if len(workload) != 0:
            for job_id in workload.keys():
                if "priority" not in workload[job_id]:
                    workload[job_id]["priority"] = workload[job_id]["score"]
                    workload[job_id]["score"] = workload[job_id]["priority"]*priority_weight - workload[job_id]["submit_time"]

        if len(wait_q) != 0:
            for job_id in wait_q.keys():
                if "priority" not in wait_q[job_id]:
                    wait_q[job_id]["priority"] = wait_q[job_id]["score"]
                    wait_q[job_id]["score"] = wait_q[job_id]["priority"]*priority_weight - wait_q[job_id]["submit_time"]


    def place(sef, free_gpu_list, job_info):
        # 放置策略
        # e.g. 随机放置策略：
        num = job_info["GPU_num"]
        gpu_ids = random.sample(free_gpu_list, num)

        return gpu_ids

    def restart(self,job_id,wait_q):
        # 出错重启策略
        # e.g. 不重启:
        # print("Debug from zhang--job id "+job_id+" is broken")
    #         # if wait_q[job_id]["restart"] % 3 == 0 and wait_q[job_id]["restart"] != 0:
    #         #     return False
    #         # print("restarting now")
    #         # return True
        return False

    def restart_cost(self,job_id,wait_q):
        print("resetting job_id %s, running_time %d" % (job_id, wait_q[job_id]["running_time"]))#debug zhang
        rest_time = math.ceil(wait_q[job_id]["running_time"] / wait_q[job_id]["epoch_time"]) * wait_q[job_id]["epoch_time"]
        wait_q[job_id]["total_time"] += rest_time - wait_q[job_id]["running_time"]
        wait_q[job_id]["running_time"] = rest_time
        return True

    def preempt(self,job_info, running):
        # 是否抢占（动态调度）, 抢占返回True，不抢占返回False
        preempt_flag = False
        if preempt_flag == False:
            return None,None,None
        else:
            for keys in running:
                if running[keys][0]["score"] > job_info["score"] and running[keys][0]["GPU_num"] > job_info["GPU_num"]:
                    return keys, job_info["submit_time"]
            return None, None, None

    def backfill(self):
        return False

#fault-aware shortest job first
class baseline6_scheduler:
    def __init__(self):
        pass
    def init_workflow(self, workloads):
        return workloads.keys()
    def time_predict(self,GPU_num,running_time,epoch_time,env_para,err_rate):
        if err_rate == 0:
            return running_time
        lamda = -math.log(1-err_rate) / env_para["Scale"]
        return running_time / GPU_num / epoch_time / lamda * (math.pow(1-err_rate, -GPU_num*epoch_time/env_para["Scale"]) - 1)
    def order(self, wait_q, workload,env_para,priority_weight,time):
        # 作业得分策略，即排队策略，高分代表高优先级，得分必须是正整数。
        # 可更新所有作业的score，包括wait_q & workloads。也可以只更新wait_q中作业得分。
        err_rate = (env_para["High_error_rate"]*env_para["High_error_card"] + env_para["Low_error_rate"]*env_para["Low_error_card"]) / env_para["GPU_num_per_host"] / env_para["Host_num"]
        lamda = -math.log(1-err_rate) / env_para["Scale"] #此处用到scale = 60
        for job_id in workload.keys():
            if "priority" not in workload[job_id]:
                workload[job_id]["priority"] = workload[job_id]["score"]
                workload[job_id]["init_submit_time"] = workload[job_id]["submit_time"]
            temp_dict = workload[job_id]
            time_pre = self.GPU_time_predict(temp_dict["GPU_num"],temp_dict["running_time"],temp_dict["epoch_time"],env_para,err_rate)
            #The threshold needs verification
            #if time_pre < 1000000:
            #    temp_dict["score"] = temp_dict["priority"]*priority_weight - time_pre
            #else:
                #TO DO: priority = B(t) - A(t)*T, currently set B(t) = t, A(t) - 1/(t+1)
            temp_dict["score"] = temp_dict["priority"]*priority_weight - time_pre
        for job_id in wait_q.keys():
            if "priority" not in wait_q[job_id]:
                wait_q[job_id]["priority"] = wait_q[job_id]["score"]
                wait_q[job_id]["init_submit_time"] = wait_q[job_id]["submit_time"]
            temp_dict = wait_q[job_id]
            time_pre = self.time_predict(temp_dict["GPU_num"],temp_dict["running_time"],temp_dict["epoch_time"],env_para,err_rate)
            #The threshold needs verification
            #if time_pre < 1000000:
            #    temp_dict["score"] = temp_dict["priority"]*priority_weight - time_pre
            #else:
                #TO DO: priority = B(t) - A(t)*T, currently set B(t) = t, A(t) - 1/(t+1)
            temp_dict["score"] = temp_dict["priority"]*priority_weight - time_pre


    def place(self, free_gpu_list, job_info):
        # 放置策略
        # e.g. 随机放置策略：
        num = job_info["GPU_num"]
        gpu_ids = random.sample(free_gpu_list, num)
        return gpu_ids

    def restart(self,job_id,wait_q):
        # 出错重启策略
        # e.g. 不重启:
        # print("Debug from zhang--job id "+job_id+" is broken")
    #         # if wait_q[job_id]["restart"] % 3 == 0 and wait_q[job_id]["restart"] != 0:
    #         #     return False
    #         # print("restarting now")
    #         # return True
        return False

    def restart_cost(self,job_id,wait_q):
        print("resetting job_id %s, running_time %d" % (job_id, wait_q[job_id]["running_time"]))#debug zhang
        rest_time = math.ceil(wait_q[job_id]["running_time"] / wait_q[job_id]["epoch_time"]) * wait_q[job_id]["epoch_time"]
        wait_q[job_id]["total_time"] += rest_time - wait_q[job_id]["running_time"]
        wait_q[job_id]["running_time"] = rest_time
        return True

    def preempt(self,job_info, running):
        # 是否抢占（动态调度）, 抢占返回True，不抢占返回False
        preempt_flag = False
        if preempt_flag == False:
            return None,None,None
        else:
            for keys in running:
                if running[keys][0]["score"] > job_info["score"] and running[keys][0]["GPU_num"] > job_info["GPU_num"]:
                    return keys, job_info["submit_time"]
            return None, None, None

    def backfill(self):
        return False
