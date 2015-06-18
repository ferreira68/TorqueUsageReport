#!/usr/bin/env python
import string
import time
import os
import numpy as np
import sys

secs_to_hours = np.float(1.0e+0)/np.float(3600e+0)
zero = np.longdouble(0.0e+0)

data_dir = "./Data"

file_list = os.listdir(data_dir)
num_files = len(file_list)
print ("Found %d files in the following directory: %s") % (num_files,data_dir)

# Set up some important dates
# 2013
start_2013_str = '2013.01.01 00:00:00'
end_2013_str   = '2013.12.31 23:59:59'
start_2013 = int(time.mktime(time.strptime(start_2013_str,'%Y.%m.%d %H:%M:%S')))
end_2013   = int(time.mktime(time.strptime(end_2013_str,'%Y.%m.%d %H:%M:%S')))

# 2014
start_2014_str = '2014.01.01 00:00:00'
end_2014_str   = '2014.12.31 23:59:59'
start_2014 = int(time.mktime(time.strptime(start_2014_str,'%Y.%m.%d %H:%M:%S')))
end_2014   = int(time.mktime(time.strptime(end_2014_str,'%Y.%m.%d %H:%M:%S')))

# 2015
start_2015_str = '2015.01.01 00:00:00'
end_2015_str   = '2015.12.31 23:59:59'
start_2015 = int(time.mktime(time.strptime(start_2015_str,'%Y.%m.%d %H:%M:%S')))
end_2015   = int(time.mktime(time.strptime(end_2015_str,'%Y.%m.%d %H:%M:%S')))

start_time = int(time.mktime(time.localtime()))

class Job:
    def __init__(self,data):
        self.logtime      = data[0]
        self.jobstate     = data[1]
        self.full_ID      = data[2]
        self.ID           = self.full_ID.split('.')[0]
        for item in data[3]:
            if item.count('=') > 0:
                key = item.split('=',1)[0]
                value = item.split('=',1)[1]
                if key == "user":
                    self.user = value
                elif key == "group":
                    self.group = value
                elif key == "jobname":
                    self.jobname = value
                elif key == "queue":
                    self.queue = value
                elif key == "ctime":
                    self.ctime = int(value)
                elif key == "qtime":
                    self.qtime = int(value)
                elif key == "etime":
                    self.etime = int(value)
                elif key == "start":
                    self.start = int(value)
                elif key == "owner":
                    self.email = value
                elif key == "exec_host":
                    self.hosts = value
                    fields = value.split('+')
                    if len(fields) > 1:
                        self.multinode = True
                    else:
                        self.multinode = False
                    total_cores = 0
                    for f in fields:
                        core_spec = f.split('/')[1]
                        if ("-" in core_spec) or ("," in core_spec):
                            specs = core_spec.split(",")
                            for s in specs:
                                if ("-" in s):
                                    first,last = s.split("-")
                                    num_proc = int(last) - int(first) + 1
                                else:
                                    num_proc = 1
                                total_cores += num_proc
                        else:
                            total_cores = 1
                    self.num_cores = total_cores
                elif key == "Resource_List.ddisk":
                    self.req_disk = value
                elif key == "Resource_List.neednodes":
                    self.req_numnodes = value
                elif key == "Resource_List.nodes":
                    if "gpus" in value:
                        fields = value.split(':')
                        for f in fields:
                            if "gpus" in f:
                                self.gpu = int(f.split("=")[1])
                elif key == "Resource_List.pmem":
                    self.req_memory = value
                elif key == "Resource_List.walltime":
                    self.req_walltime = value
                elif key == "session":
                    self.session_ID = value
                elif key == "total_execution_slots":
                    self.slots = int(value)
                elif key == "unique_node_count":
                    self.unique_nodes = int(value)
                elif key == "end":
                    self.end = int(value)
                elif key == "Exit_status":
                    self.exit_status = int(value)
                elif key == "resources_used.walltime":
                    self.walltime = value
                    hours, mins, secs = self.walltime.split(':')
                    self.walltime_secs = 3600 * int(hours) + 60 * int(mins) + int(secs)
                elif key == "resources_used.cput":
                    self.cpu = value
                    hours, mins, secs = self.cpu.split(':')
                    self.cpu_secs = 3600 * int(hours) + 60 * int(mins) + int(secs)
                elif key == "resources_used.mem":
                    self.pmem = value
                elif key == "resources_used.vmem":
                    self.vmem = value

        # Catch some bad data where start time = 0
        self.start = max(self.start,self.etime)

        self.runtime  = self.end - self.start
        if self.runtime < 0: self.runtime = 0
        self.waittime = self.start - self.etime
        if self.waittime < 0: self.waittime = 0
        self.holdtime = self.etime - self.qtime
        if self.holdtime < 0: self.holdtime = 0

        if self.exit_status == 0:
            if not hasattr(self,'exec_host'):
                self.exec_host = "unknown"
            if not hasattr(self,'unique_nodes'):
                self.unique_nodes = self.exec_host.count('+') + 1
            if not hasattr(self,'hosts'):
                self.hosts = "unknown"
        else:
            if not hasattr(self,'exec_hosts'):
                self.exec_host = "unknown"
            if not hasattr(self,'hosts'):
                self.hosts = "failed"
            if not hasattr(self,'cpu'):
                self.cpu = "failed"
                self.cpu_secs = 0

        if not hasattr(self,'req_numnodes'):
            self.req_numnodes = -999

    def print_info(self):
        print ("Job ID = %s") % self.ID
        print ("======================================")
        print ("Status        = %s") % self.jobstate
        print ("Session ID    = %s") % self.session_ID
        print ("Exit status   = %s") % self.exit_status
        print ("Last log time = %s") % self.logtime
        print ("User          = %s") % self.user
        print ("  email       = %s") % self.email
        print ("Group         = %s") % self.group
        print ("Name          = %s") % self.jobname
        print ("Queue         = %s") % self.queue
        print ("Submit time   = %s (%d)") % (time.ctime(self.ctime),self.ctime)
        print ("Time Queued   = %s (%d)") % (time.ctime(self.qtime),self.qtime)
        print ("Time Eligible = %s (%d)") % (time.ctime(self.etime),self.etime)
        print ("Start Time    = %s (%d)") % (time.ctime(self.start),self.start)
        print ("End Time      = %s (%d)") % (time.ctime(self.end),self.end)
        if hasattr(self,'unique_nodes'):
            print ("Hosts used    = %s unique: %s (requested %s)") % \
                  (self.unique_nodes,self.hosts,self.req_numnodes)
        else:
            print ("Hosts used    = Unknown")
        print ("Cores         = %d") % self.num_cores
        if hasattr(self,'gpu'):
            print ("GPUs          = %d") % self.gpu
        print ("Memory        = %s physcial, %s virtual") % \
                                      (self.pmem,self.vmem)
        if hasattr(self,'req_memory'):
            print ("                (%s requested)") % self.req_memory
        print ("Walltime      = %s ( %d s)") % (self.walltime,self.walltime_secs)
        print ("CPU time      = %s ( %d s)") % (self.cpu,self.cpu_secs)
        print ("Wait time     = %d") % self.waittime
        print ("")
        

class SummaryStats:
    def __init__(self,owner,label,start_time,end_time):
        self.owner    = owner
        self.label    = label
        self.start_t  = start_time
        self.end_t    = end_time
        self.num_jobs = 0
        self.num_days = (self.end_t - self.start_t) / (60*60*24)
        # The following are tuples of max,min,avg
        self.wait_sum       = zero
        self.wait           = [zero, zero, zero]
        self.run_sum        = zero
        self.run            = [zero, zero, zero]
        self.turnaround_sum = zero
        self.turnaround     = [zero, zero, zero]
        self.cores_sum      = zero
        self.cores          = [zero, zero, zero]
        self.nodes_sum      = zero
        self.nodes          = [zero, zero, zero]
        self.users = list()
        self.groups = list()
        self.queues = list()

    def update(self,job):
        timestamp = job.ctime
        wait_val  = np.longdouble(job.waittime)
        run_val   = np.longdouble(job.runtime)
        cores_val = np.longdouble(job.num_cores)
        nodes_val = np.longdouble(job.unique_nodes)
        if (timestamp >= self.start_t) and (timestamp <= self.end_t):
            self.num_jobs += 1

            self.wait_sum       += wait_val
            self.run_sum        += run_val
            self.turnaround_sum += (wait_val + run_val)
            self.cores_sum      += cores_val
            self.nodes_sum      += nodes_val
            

            if self.num_jobs == 1:
                self.wait[0]       = wait_val
                self.wait[1]       = wait_val

                self.run[0]        = run_val
                self.run[1]        = run_val

                self.turnaround[0] = wait_val + run_val
                self.turnaround[1] = wait_val + run_val

                self.cores[0]      = cores_val
                self.cores[1]      = cores_val

                self.nodes[0]      = nodes_val
                self.nodes[1]      = nodes_val
            else:
                self.wait[0]       = min(self.wait[0],wait_val)
                self.wait[1]       = max(self.wait[1],wait_val)

                self.run[0]        = min(self.run[0],run_val)
                self.run[1]        = max(self.run[1],run_val)

                self.turnaround[0] = min(self.turnaround[0],(wait_val + run_val))
                self.turnaround[1] = max(self.turnaround[1],(wait_val + run_val))
                
                self.cores[0]      = min(self.cores[0],cores_val)
                self.cores[1]      = max(self.cores[1],cores_val)

                self.nodes[0]      = min(self.nodes[0],nodes_val)
                self.nodes[1]      = max(self.nodes[1],nodes_val)

            self.wait[2]       = self.wait_sum / self.num_jobs
            self.run[2]        = self.run_sum / self.num_jobs
            self.turnaround[2] = self.turnaround_sum / self.num_jobs
            self.cores[2]      = self.cores_sum / self.num_jobs
            self.nodes[2]      = self.nodes_sum / self.num_jobs

            have_user = False
            if len(self.users) > 0:
                for i in self.users:
                    if i[0] == job.user:
                        have_user = True
                        i[1] += 1
                        i[2] += job.cpu_secs
            if not have_user:
                user_data = [job.user,1,run_val]
                self.users.append(user_data)

            have_group = False
            if len(self.groups) > 0:
                for i in self.groups:
                    if i[0] == job.group:
                        have_group = True
                        i[1] += 1
                        i[2] += job.cpu_secs
            if not have_group:
                group_data = [job.group,1,run_val]
                self.groups.append(group_data)

            have_queue = False
            if len(self.queues) > 0:
                for i in self.queues:
                    if i[0] == job.queue:
                        have_queue = True
                        i[1] += 1
                        i[2] += job.cpu_secs
            if not have_queue:
                queue_data = [job.queue,1,run_val]
                self.queues.append(queue_data)


    def print_info(self):
        print ("Summary statistics for %s") % self.label
        print ("  From                  : %s\n  To                    : %s (%d days total)") % \
              (time.ctime(self.start_t),time.ctime(self.end_t),self.num_days)
        print ("  Owner                 : %7s") % self.owner
        print ("  Number of jobs        : %7d") % self.num_jobs
        print ("  Jobs/day              : %7.1f") % float(self.num_jobs/self.num_days)
        print ("  Wait times (hours)    : %7.2f, %7.2f, %7.2f (min,max,avg)") % \
              (self.wait[0] * secs_to_hours,\
               self.wait[1] * secs_to_hours,\
               self.wait[2] * secs_to_hours)
        print ("  Run times (hours)     : %7.2f, %7.2f, %7.2f (min,max,avg)") % \
              (self.run[0] * secs_to_hours,\
               self.run[1] * secs_to_hours,\
               self.run[2] * secs_to_hours)
        print ("  Job turnaournd (hours): %7.2f, %7.2f, %7.2f (min,max,avg)") % \
              (self.turnaround[0] * secs_to_hours,\
               self.turnaround[1] * secs_to_hours,\
               self.turnaround[2] * secs_to_hours)
        print ("  Cores per job         : %7.2f, %7.2f, %7.2f (min,max,avg)") % \
              (self.cores[0],self.cores[1],self.cores[2])
        if self.nodes[1] > 1:
            print ("  Nodes per job         : %7.2f, %7.2f, %7.2f (min,max,avg)") % \
                  (self.nodes[0],self.nodes[1],self.nodes[2])


def CombinedSummaryTable (data_list,col_header="",\
                          user_detail=False,group_detail=False,queue_detail=False):
    # Print the header
    counter = 0
    for data in data_list:
        counter += 1
        if counter == 1:
            print ("%-15s          Average Times (h)            Cores per") % (col_header)
            print ("%-15s    Wait      Run      Turnaround         Job         Jobs") % (data.owner)
            print ("-------------------------------------------------------------------------")
        print ("%-15s%8.2f%9.2f%13.2f%15.1f%13d") % (data.label,\
                                               data.wait[2] * secs_to_hours,\
                                               data.run[2] * secs_to_hours,\
                                               data.turnaround[2] * secs_to_hours,\
                                               data.cores[2],data.num_jobs)
        if user_detail and len(data.users) > 0:
            # Sort the list by CPU time
            data.users.sort(key=lambda array: array[2],reverse=True)
            print ("  Users           Total CPU time")
            for user in data.users:
                print ("     %-15s%12.2f%41d") % (user[0],user[2] * secs_to_hours,user[1])
        if group_detail and len(data.groups) > 0:
            # Sort the list by name
            data.groups.sort(key=lambda array: array[0])
            print ("  Group           Total CPU time")
            for group in data.groups:
                print ("     %-15s%12.2f%41d") % (group[0],group[2] * secs_to_hours,group[1])
        if queue_detail and len(data.queues) > 0:
            # Sort the list by number of jobs
            data.queues.sort(key=lambda array: array[1],reverse=True)
            print ("  Queue           Total CPU time")
            for queue in data.queues:
                print ("     %-15s%12.2f%41d") % (queue[0],queue[2] * secs_to_hours,queue[1])
    print ("\n")
        

JobList = list()

# Get ALL the data on completed jobs
print ("Importing job accounting data ... "),
file_count = 0
for filename in file_list:
    file_count += 1
    datafile_name = data_dir + "/" + filename
    datafile=open(datafile_name,"r")
    for line in datafile:
        if string.find(line,";") > 0:
            data = line.split(';')
            if data[1] == "E":
                data[3] = data[3].split()
                current_job = Job(data)
                # Only include successful jobs
                if current_job.exit_status == 0:
                    JobList.append(current_job)
    datafile.close()
print("done")

stats_list = list()
user_list = list()
group_list = list()
queue_list = list()

# For 30-day stats
thirty_days_in_secs = 60*60*24*30
stats30 = SummaryStats("all","past 30 days",start_time - thirty_days_in_secs,start_time)

# For 60-day stats
sixty_days_in_secs = 60*60*24*60
stats60 = SummaryStats("all","past 60 days",start_time - sixty_days_in_secs,start_time)

# For 90-day stats
ninety_days_in_secs = 60*60*24*90
stats90 = SummaryStats("all","past 90 days",start_time - ninety_days_in_secs,start_time)

# For all of 2015
stats2015 = SummaryStats("all","2015",start_2015,end_2015)

# For all of 2014
stats2014 = SummaryStats("all","2014",start_2014,end_2014)

# For all of 2013
stats2013 = SummaryStats("all","2013",start_2013,end_2013)


stats_list = list()

for j in JobList:
    if hasattr(j,'num_cores'):
        if j.user not in user_list:
            user_list.append(j.user)
        if j.group not in group_list:
            group_list.append(j.group)
        if j.queue not in queue_list:
            queue_list.append(j.queue)

        # 30-day stats
        stats30.update(j)
        # 60-day stats
        stats60.update(j)
        # 90-day stats
        stats90.update(j)
        # 2015 stats
        stats2015.update(j)
        # 2014 stats
        stats2014.update(j)
        # 2013 stats
        stats2013.update(j)
    else:
        JobList.remove(j)

stats_list.append(stats30)
stats_list.append(stats60)
stats_list.append(stats90)
stats_list.append(stats2015)
stats_list.append(stats2014)
stats_list.append(stats2013)


print ("********************************************************************************")
print ("***************************** Summary for all jobs *****************************")
print ("********************************************************************************")
for i in stats_list:
    i.print_info()
    print ("")


print ("\n")
print ("********************************************************************************")
print ("******************************* Summary by user ********************************")
print ("********************************************************************************")
user_list.sort()
for user in user_list:
    user_data = list()
    user_stats30 = SummaryStats(user,"Past 30 days",\
                           start_time - thirty_days_in_secs,\
                           start_time)
    user_stats60 = SummaryStats(user,"Past 60 days",\
                           start_time - sixty_days_in_secs,\
                           start_time)
    user_stats90 = SummaryStats(user,"Past 90 days",\
                           start_time - ninety_days_in_secs,\
                           start_time)
    user_stats2015 = SummaryStats(user,"2015",start_2015,end_2015)
    user_stats2014 = SummaryStats(user,"2014",start_2014,end_2014)
    user_stats2013 = SummaryStats(user,"2013",start_2013,end_2013)
    for job in JobList:
        if job.user == user:
            user_stats30.update(job)
            user_stats60.update(job)
            user_stats90.update(job)
            user_stats2015.update(job)
            user_stats2014.update(job)
            user_stats2013.update(job)
    user_data.append(user_stats30)
    user_data.append(user_stats60)
    user_data.append(user_stats90)
    user_data.append(user_stats2015)
    user_data.append(user_stats2014)
    user_data.append(user_stats2013)

    CombinedSummaryTable(user_data,"User",False,False,True)

print ("\n")
print ("********************************************************************************")
print ("****************************** Summary by group ********************************")
print ("********************************************************************************")
for group in group_list:
    group_data = list()
    group_stats30 = SummaryStats(group,"Past 30 days",\
                           start_time - thirty_days_in_secs,\
                           start_time)
    group_stats60 = SummaryStats(group,"Past 60 days",\
                           start_time - sixty_days_in_secs,\
                           start_time)
    group_stats90 = SummaryStats(group,"Past 90 days",\
                           start_time - ninety_days_in_secs,\
                           start_time)
    group_stats2015 = SummaryStats(group,"2015",start_2015,end_2015)
    group_stats2014 = SummaryStats(group,"2014",start_2014,end_2014)
    group_stats2013 = SummaryStats(group,"2013",start_2013,end_2013)
    for job in JobList:
        if job.group == group:
            group_stats30.update(job)
            group_stats60.update(job)
            group_stats90.update(job)
            group_stats2015.update(job)
            group_stats2014.update(job)
            group_stats2013.update(job)
    group_data.append(group_stats30)
    group_data.append(group_stats60)
    group_data.append(group_stats90)
    group_data.append(group_stats2015)
    group_data.append(group_stats2014)
    group_data.append(group_stats2013)
    CombinedSummaryTable(group_data,"Group",True,False,True)


print ("\n")
print ("********************************************************************************")
print ("****************************** Summary by queue ********************************")
print ("********************************************************************************")
for queue in queue_list:
    queue_data = list()
    queue_stats30 = SummaryStats(queue,"Past 30 days",\
                           start_time - thirty_days_in_secs,\
                           start_time)
    queue_stats60 = SummaryStats(queue,"Past 60 days",\
                           start_time - sixty_days_in_secs,\
                           start_time)
    queue_stats90 = SummaryStats(queue,"Past 90 days",\
                           start_time - ninety_days_in_secs,\
                           start_time)
    queue_stats2015 = SummaryStats(queue,"2015",start_2015,end_2015)
    queue_stats2014 = SummaryStats(queue,"2014",start_2014,end_2014)
    queue_stats2013 = SummaryStats(queue,"2013",start_2013,end_2013)

    for job in JobList:
        if job.queue == queue:
            queue_stats30.update(job)
            queue_stats60.update(job)
            queue_stats90.update(job)
            queue_stats2015.update(job)
            queue_stats2014.update(job)
            queue_stats2013.update(job)
    queue_data.append(queue_stats30)
    queue_data.append(queue_stats60)
    queue_data.append(queue_stats90)
    queue_data.append(queue_stats2015)
    queue_data.append(queue_stats2014)
    queue_data.append(queue_stats2013)

    CombinedSummaryTable(queue_data,"Queue",True,True,False)


print ("********************************************************************************")
print ("******************************* Single Node Jobs *******************************")
print ("********************************************************************************")
single_node_data = list()
single_node_stats30 = SummaryStats("all","Past 30 days",\
                       start_time - thirty_days_in_secs,\
                       start_time)
single_node_stats60 = SummaryStats("all","Past 60 days",\
                       start_time - sixty_days_in_secs,\
                       start_time)
single_node_stats90 = SummaryStats("all","Past 90 days",\
                       start_time - ninety_days_in_secs,\
                       start_time)
single_node_stats2015 = SummaryStats("all","2015",start_2015,end_2015)
single_node_stats2014 = SummaryStats("all","2014",start_2014,end_2014)
single_node_stats2013 = SummaryStats("all","2013",start_2013,end_2013)

for job in JobList:
    if job.multinode == False:
        single_node_stats30.update(job)
        single_node_stats60.update(job)
        single_node_stats90.update(job)
        single_node_stats2015.update(job)
        single_node_stats2014.update(job)
        single_node_stats2013.update(job)
single_node_data.append(single_node_stats30)
single_node_data.append(single_node_stats60)
single_node_data.append(single_node_stats90)
single_node_data.append(single_node_stats2015)
single_node_data.append(single_node_stats2014)
single_node_data.append(single_node_stats2013)

for i in single_node_data:
    i.print_info()
    print("")
CombinedSummaryTable(single_node_data,"Single-node jobs",True,True,True)



print ("********************************************************************************")
print ("******************************** Multi-Node Jobs *******************************")
print ("********************************************************************************")
multi_node_data = list()
multi_node_stats30 = SummaryStats("all","Past 30 days",\
                                  start_time - thirty_days_in_secs,\
                                  start_time)
multi_node_stats60 = SummaryStats("all","Past 60 days",\
                                  start_time - sixty_days_in_secs,\
                                  start_time)
multi_node_stats90 = SummaryStats("all","Past 90 days",\
                                  start_time - ninety_days_in_secs,\
                                  start_time)
multi_node_stats2015 = SummaryStats("all","2015",start_2015,end_2015)
multi_node_stats2014 = SummaryStats("all","2014",start_2014,end_2014)
multi_node_stats2013 = SummaryStats("all","2013",start_2013,end_2013)

for job in JobList:
    if job.multinode == True:
        multi_node_stats30.update(job)
        multi_node_stats60.update(job)
        multi_node_stats90.update(job)
        multi_node_stats2015.update(job)
        multi_node_stats2014.update(job)
        multi_node_stats2013.update(job)
multi_node_data.append(multi_node_stats30)
multi_node_data.append(multi_node_stats60)
multi_node_data.append(multi_node_stats90)
multi_node_data.append(multi_node_stats2015)
multi_node_data.append(multi_node_stats2014)
multi_node_data.append(multi_node_stats2013)

for i in multi_node_data:
    i.print_info()
    print("")
CombinedSummaryTable(multi_node_data,"Multiple-node jobs",True,True,True)
