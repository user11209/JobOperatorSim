import csv
import random
import sys
import numpy as np
import database_generator

#generate workloads/workflow randomly according to density
def choose_a_job_with_model(job_id,density):
    rand_bound_density = list(map(sum,density))#verify
    total_prob = sum(rand_bound_density)
    rand_bound_distribution = []
    rand_type_distribution = []
    #normalization
    for i,l in enumerate(density):
        rand_bound_distribution.append(sum(rand_bound_density[:(i+1)])/total_prob)
        rand_type_distribution.append([])
        for j,e in enumerate(l):
            if rand_bound_density[i] != 0:
                l[j] = e/rand_bound_density[i]
            else:
                l[j] = 0
            rand_type_distribution[i].append(sum(l[:(j+1)]))

    rand_bound = random.random()
    if rand_bound < rand_bound_distribution[0]:
        model_type = "ResNet50"
        epoch_num = 120
        rand_type = random.random()
        if rand_type < rand_type_distribution[0][0]:
            GPU_num = 1
            epoch_time = 17252
        elif rand_type < rand_type_distribution[0][1]:
            GPU_num = 2
            epoch_time = 8863
        elif rand_type < rand_type_distribution[0][2]:
            GPU_num = 4
            epoch_time = 6861

        else:
            GPU_num = 8
            epoch_time = 4401
    elif rand_bound < rand_bound_distribution[1]:
        model_type = "ResNet101"
        epoch_num = 120
        rand_type = random.random()
        if rand_type < rand_type_distribution[1][0]:
            GPU_num = 1
            epoch_time = 15019
        elif rand_type < rand_type_distribution[1][1]:
            GPU_num = 2
            epoch_time = 9238
        elif rand_type < rand_type_distribution[1][2]:
            GPU_num = 4
            epoch_time = 5698
        else:
            GPU_num = 8
            epoch_time = 4289
    elif rand_bound < rand_bound_distribution[2]:
        model_type = "ResNet152"
        epoch_num = 120
        rand_type = random.random()
        if rand_type < rand_type_distribution[2][0]:
            GPU_num = 1
            epoch_time = 17551
        elif rand_type < rand_type_distribution[2][1]:
            GPU_num = 2

            epoch_time = 13959
        elif rand_type < rand_type_distribution[2][2]:
            GPU_num = 4
            epoch_time = 9430
        else:
            GPU_num = 8
            epoch_time = 5367
    elif rand_bound < rand_bound_distribution[3]:
        model_type = "VGG-16"
        epoch_num = 120
        rand_type = random.random()
        if rand_type < rand_type_distribution[3][0]:
            GPU_num = 1
            epoch_time = 17679
        elif rand_type < rand_type_distribution[3][1]:
            GPU_num = 2
            epoch_time = 14980
        elif rand_type < rand_type_distribution[3][2]:
            GPU_num = 4
            epoch_time = 8862
        else:
            GPU_num = 8
            epoch_time = 5270
    else:
        model_type = "AlexNet"
        epoch_num = 120
        rand_type = random.random()
        if rand_type < rand_type_distribution[4][0]:
            GPU_num = 1
            epoch_time = 17710
        elif rand_type < rand_type_distribution[4][1]:
            GPU_num = 2
            epoch_time = 8207
        elif rand_type < rand_type_distribution[4][2]:
            GPU_num = 4
            epoch_time = 6870
        else:
            GPU_num = 8
            epoch_time = 5145
    epoch_num_list = [4,8,12,16,20]
    epoch_num = epoch_num_list[random.randint(0,4)]
    #epoch_num = 20
    running_time = epoch_time * epoch_num
    row = [job_id,0,int(running_time/100),GPU_num,0,0,0,1,model_type,int(epoch_time/100),int(running_time/100)]
    return row

def choose_a_job(job_id,job_database="job_database.txt",density_database="density_database.txt"):
    '''
    #mad again
    vote = random.randint(0,1)
    if vote == 0:
        return [job_id,0,200,4,0,0,0,1,'hello_fat',50,200]
    else:
        return [job_id,0,400,1,0,0,0,1,'hello_tall',50,400]
    '''

    #`read_job_database` return format [['model_type',['GPU_num','epoch_time','epoch_num'],~],~]
    job_type = database_generator.read_job_database(job_database)
    density = database_generator.read_density_database(density_database)

    rand_bound_density = list(map(sum,density))#verify
    total_prob = sum(rand_bound_density)
    rand_bound_distribution = []
    rand_type_distribution = []
    #normalization
    for i,l in enumerate(density):
        rand_bound_distribution.append(sum(rand_bound_density[:(i+1)])/total_prob)
        rand_type_distribution.append([])
        for j,e in enumerate(l):
            l[j] = e/rand_bound_density[i]
            rand_type_distribution[i].append(sum(l[:(j+1)]))

    rand_bound = random.random()
    for model_type_id in range(len(job_type)):
        if rand_bound > rand_bound_distribution[model_type_id]:
            continue
        model = job_type[model_type_id]
        model_type = model[0]
        rand_type = random.random()
        for GPU_num_id in range(len(model)-1):
            if rand_type > rand_type_distribution[model_type_id][GPU_num_id]:
                continue
            para = model[GPU_num_id+1]
            GPU_num = para[0]
            epoch_time = para[1]
            #epoch_num = para[2]
            break
        break
    epoch_num = 10*random.randint(1,6)
    running_time = epoch_time * epoch_num
    row = [job_id,0,running_time,GPU_num,0,0,0,1,model_type,epoch_time,running_time]
    return row

def choose_a_time(timer,lambda_arrival):
    if lambda_arrival == 0:
        return timer
    timer = timer + int(np.random.exponential(lambda_arrival,1))
    return timer

def choose_lambda_arrival(timer,count):
    if count < 5:
        return 0
    return 300

def workflow_generator(workload_num,job_num_per_workload,workload_file,with_model):
    headers = ['job_ID','submit_time','running_time','GPU_num','restart','preempt_times','err_times','score','type','epoch_time','total_time']
    density = [[0,0,0,0],[0,0,0,0],[0,0,0,0],[0,0,0,0],[1,1,1,1]]
    lambda_arrival = 10000
    timer = 0
    for j in range(workload_num): #workload数量
        rows = []
        with open(workload_file+"workload_"+str(j)+".csv",'w',newline='')as fd:
            for i in range(job_num_per_workload):
                if with_model == True:
                    row = choose_a_job_with_model(i,density)
                else:
                    row = choose_a_job(i)
                row[1] = timer
                timer = choose_a_time(timer,choose_lambda_arrival(timer,i))
                rows.append(row)
            f_csv = csv.writer(fd)
            f_csv.writerow(headers)
            f_csv.writerows(rows)

if __name__ == '__main__':
    if len(sys.argv) == 1:
        job_num = 20
    else:
        job_num = int(sys.argv[1])
    database_generator.gen_database(10,0)
    workflow_generator(1,job_num,"",True)
