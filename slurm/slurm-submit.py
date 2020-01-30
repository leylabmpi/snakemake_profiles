#!/usr/bin/env python3
import os
import sys
import re
import subprocess

import numpy as np
import pandas as pd
from snakemake.utils import read_job_properties

# load
## loading job script (provided by snakemake)
job_script = sys.argv[-1]
job_properties = read_job_properties(job_script)


# getting job parameters from snakemake-generated job script
try:
    threads = job_properties['threads']
except KeyError:
    threads = 1
try:
    time = job_properties['cluster']['time']
except KeyError:
    try:
        time = job_properties['resources']['time']
    except KeyError:
        time = '00:59:00'
try:
    n = job_properties['cluster']['n']
except KeyError:
    try:
        n = job_properties['resources']['n']
    except KeyError:
        n = 1
try:
    mem = job_properties['cluster']['mem']
except KeyError:
    try:
        mem = job_properties['resources']['mem_gb_pt']
    except KeyError:
        mem = 6

# memory * cpu
try:
    mem = int(mem) * int(n)
except ValueError:
    pass

# setting cluster job log PATHs
try:
    log_base = job_properties['log'][0]
except (KeyError, IndexError) as e:
    log_base = os.path.join(os.path.abspath(os.getcwd()), 'slurm_job')
log_base = os.path.splitext(log_base)[0]
os.makedirs(os.path.split(log_base)[0], exist_ok = True)
std_out = log_base + '_slurm.out'
std_err = log_base + '_slurm.err'


# setting partition
def _time_to_minutes(time_dhms):
    """ Converting time from 'd-hh:mm:ss' to total minutes """
    x = time_dhms.split('-')
    if len(x) > 1:
        days = int(x.pop(0))
    else:
        days = 0
    x = x[0].split(':')
    hours = int(x[0])
    minutes = int(x[1])
    
    # return number of minutes
    return days * (24 * 60) + hours * 60 + minutes

def _max_job_size(job_size):
    """ Formatting job size 'X-Y' """
    job_size = job_size.split('-')
    if len(job_size) > 1:
        return int(job_size[1])
    else:
        return int(job_size[0])

def _get_partition_info():
    """ Retrieve cluster configuration for a partition. """
    # Retrieve partition info; we tacitly assume we only get one response
    cols = ['partition' , 'cpus', 'memory', 'time', 'size', 'cpusload']
    cmd = 'sinfo -e -O \"{}\"'.format(','.join(cols))
    res = subprocess.run(cmd, check=True, shell=True, stdout=subprocess.PIPE)
    regex = re.compile(r' +') 
    
    part_info = {x:[] for x in cols}
    for i,line in enumerate(res.stdout.decode().split('\n')):
        line = regex.split(line.rstrip())
        if len(line) < len(cols):
            continue
        if line[0] == 'PARTITION':
            continue            
        
        # values
        PARTITION = line[0]
        CPUS = int(line[1])
        MEMORY = int(float(line[2]) * 0.001)  # in gigabytes
        TIMELIMIT = _time_to_minutes(line[3])
        JOB_SIZE = _max_job_size(line[4])
        LOAD = float(line[5])
        # storing
        part_info['partition'].append(PARTITION)
        part_info['cpus'].append(CPUS)
        part_info['memory'].append(MEMORY)
        part_info['time'].append(TIMELIMIT)
        part_info['size'].append(JOB_SIZE)
        part_info['cpusload'].append(LOAD)
        
    part_info = pd.DataFrame.from_dict(part_info)
    
    # summarizing
    part_info = part_info.groupby('partition').agg(
        cpus = pd.NamedAgg(column='cpus', aggfunc=lambda x: x.iloc[0]),
        memory = pd.NamedAgg(column='memory', aggfunc=lambda x: x.iloc[0]),
        time = pd.NamedAgg(column='time', aggfunc=lambda x: x.iloc[0]),
        size= pd.NamedAgg(column='size', aggfunc=lambda x: x.iloc[0]),
        cpusload = pd.NamedAgg(column='cpusload', aggfunc=np.mean),
        nodes = pd.NamedAgg(column='cpusload', aggfunc=np.sum),
    )

    #part_info.to_csv(sys.stderr); sys.exit()
    return part_info

def _select_partition(part_info, time, mem):
    """ Selecting which partition to use for the job """
    partition = ''

    # if job resources not formatted as int
    try:
        time = int(time)
        mem = int(mem)
    except ValueError:
        return partition, time, mem

    # finding 'best' partition
    part_info['delta_mem'] = part_info.apply(lambda x: x['memory'] - mem, axis=1)
    part_info['delta_time'] = part_info.apply(lambda x: x['time'] - time, axis=1)
            
    # filtering
    part_info_f = part_info.loc[(part_info['memory'] >= mem) & (part_info['time'] >= time)]    
    ## which partition
    if part_info_f.shape[0] == 1:
        # just one partition that works
        partition = part_info_f['partition'][0]
    elif part_info_f.shape[0] > 1:
        # which partition is closest?
        part_info_f = part_info_f.sort_values(by=['delta_mem', 'delta_time', 
                                                  'nodes', 'cpusload'], 
                                              ascending=[True, True, False, True])
        part_info_f.reset_index(inplace=True)
        #part_info_f.to_csv(sys.stderr, index=False);
        partition = part_info_f['partition'][0] 
    else:
        # closest for memory & time (reducing resources as needed)
        part_info['delta_mem'] = part_info.apply(lambda x: abs(x['memory'] - mem), axis=1)
        part_info['delta_time'] = part_info.apply(lambda x: abs(x['time'] - time), axis=1)
        part_info = part_info.sort_values(by=['delta_mem', 'delta_time', 
                                              'nodes', 'cpusload'], 
                                          ascending=[True, True, False, True])
        part_info.reset_index(inplace=True)
        #part_info.to_csv(sys.stderr, index=False);
        partition = part_info['partition'][0] 
        mem = part_info['memory'][0] if mem > part_info['memory'][0] else mem
        time = part_info['time'][0] if time > part_info['time'][0] else time

    # formatting time if provided (assuming minutes)
    hours = int(int(time) / 60)
    minutes = int(time) % 60
    time = '{:0>2}:{:0>2}:00'.format(hours, minutes)

    # ret
    return partition, time, mem

    
## partition selection
partition_info = _get_partition_info()
partition,time,mem = _select_partition(partition_info, time, mem)
if partition != '':
    partition = '--partition {}'.format(partition)

# formatting sbatch command
cmd = "sbatch -D . {partition} --nodes 1 --tasks-per-node 1 --cpus-per-task {n}"
cmd += " --mem {mem}G --time {time} -o {std_out} -e {std_err} {job_script}"
cmd = cmd.format(partition=partition, n=n, mem=mem, time=time,
                 std_out=std_out, std_err=std_err, job_script=job_script)
sys.stderr.write('    \033[36mJob{}: {}\x1b[0m\n'.format(job_properties['jobid'], cmd))

# subprocess job: sbatch
try:
    res = subprocess.run(cmd, check=True, shell=True, stdout=subprocess.PIPE)
except subprocess.CalledProcessError as e:
    raise e

# get qsub job ID
res = res.stdout.decode()
try:
    m = re.search("Submitted batch job (\d+)", res)
    jobid = m.group(1)
    print(jobid)
except Exception as e:
    print(e)
    raise
