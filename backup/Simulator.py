from utilis import init, output_finish, gen_metric
from scheduler import demo_scheduler,baseline1_scheduler,baseline2_scheduler,baseline3_scheduler,baseline4_scheduler,baseline5_scheduler,baseline6_scheduler
from backup.job_runner import Job_Runner
import sys

if __name__ == '__main__':
    filecount = '0'
    if len(sys.argv) <= 1:
        arg_list = [0,1,2,3,4,5,6]
    else:
        arg_list = sys.argv[1:]
    print(arg_list)
    for count,i in enumerate(arg_list):
        i = int(i)
        if i == 0:
            #my scheduler
            Workloads, Err1, scheduler, gpu_num, env_para = init("Env.json", "workload_0.csv", demo_scheduler)
            tag = " my  "
        elif i == 1:
            #smallest area first
            Workloads, Err1, scheduler, gpu_num, env_para = init("Env.json", "workload_0.csv", baseline1_scheduler)
            tag = " saf "
        elif i == 2:
            #shortest job first
            Workloads, Err1, scheduler, gpu_num, env_para = init("Env.json", "workload_0.csv", baseline2_scheduler)
            tag = " sjf "
        elif i == 3:
            #smallest resource first
            Workloads, Err1, scheduler, gpu_num, env_para = init("Env.json", "workload_0.csv", baseline3_scheduler)
            tag = " srf "
        elif i == 4:
            #random
            Workloads, Err1, scheduler, gpu_num, env_para = init("Env.json", "workload_0.csv", baseline4_scheduler)
            tag = " rand"
        elif i == 5:
            #fcfs
            Workloads, Err1, scheduler, gpu_num, env_para = init("Env.json", "workload_0.csv", baseline5_scheduler)
            tag = " fcfs"
        elif i == 6:
            #fault-aware shortest job first
            Workloads, Err1, scheduler, gpu_num, env_para = init("Env.json", "workload_0.csv", baseline6_scheduler)
            tag = " fsjf"
        else:
            print("bullshit, there ain't no such scheduler called ",i)
            tag = None
        if count == 0:
            Err = Err1
            #print("Err:", Err)
        Err1 = None
        my_job_runner = Job_Runner(Workloads,Err,scheduler,gpu_num,env_para)
        finish_dict = my_job_runner.run_jobs()
        print("finish_dict: ")
        output_finish(finish_dict,'fin_'+filecount+'.out')
        gen_metric(finish_dict,Workloads,'opt.out',tag)
        my_job_runner = None

    '''
    #smallest area first
    Workloads, Err1, scheduler, gpu_num, env_para = init("Env.json", "workload_0.csv", baseline1_scheduler)
    my_job_runner = Job_Runner(Workloads,Err,scheduler,gpu_num,env_para)
    finish_dict = my_job_runner.run_jobs()
    print("finish_dict: ")
    output_finish(finish_dict,'fin_'+filecount+'.out')
    gen_metric(finish_dict,Workloads,'opt_base1.out')
    my_job_runner = None
    Err1 = None

    #shortest job first
    Workloads, Err1, scheduler, gpu_num, env_para = init("Env.json", "workload_0.csv", baseline2_scheduler)
    my_job_runner = Job_Runner(Workloads,Err,scheduler,gpu_num,env_para)
    finish_dict = my_job_runner.run_jobs()
    print("finish_dict: ")
    output_finish(finish_dict,'fin_'+filecount+'.out')
    gen_metric(finish_dict,Workloads,'opt_base2.out')
    my_job_runner = None
    Err1 = None

    #smallest resource first
    Workloads, Err1, scheduler, gpu_num, env_para = init("Env.json", "workload_0.csv", baseline3_scheduler)
    my_job_runner = Job_Runner(Workloads,Err,scheduler,gpu_num,env_para)
    finish_dict = my_job_runner.run_jobs()
    print("finish_dict: ")
    output_finish(finish_dict,'fin_'+filecount+'.out')
    gen_metric(finish_dict,Workloads,'opt_base3.out')
    my_job_runner = None
    Err1 = None

    #random
    Workloads, Err1, scheduler, gpu_num, env_para = init("Env.json", "workload_0.csv", baseline4_scheduler)
    my_job_runner = Job_Runner(Workloads,Err,scheduler,gpu_num,env_para)
    finish_dict = my_job_runner.run_jobs()
    print("finish_dict: ")
    output_finish(finish_dict,'fin_'+filecount+'.out')
    gen_metric(finish_dict,Workloads,'opt_base4.out')
    my_job_runner = None
    Err1 = None

    #fcfs
    Workloads, Err1, scheduler, gpu_num, env_para = init("Env.json", "workload_0.csv", baseline5_scheduler)
    my_job_runner = Job_Runner(Workloads,Err,scheduler,gpu_num,env_para)
    finish_dict = my_job_runner.run_jobs()
    print("finish_dict: ")
    output_finish(finish_dict,'fin_'+filecount+'.out')
    gen_metric(finish_dict,Workloads,'opt_base5.out')
    '''
