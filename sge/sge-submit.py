#!/usr/bin/env python3
import os
import sys
import re
import subprocess

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
        mem = 8
try:
    gpu = '-l gpu={}'.format(job_properties['cluster']['gpu'])
except KeyError:
    try:
        gpu = '-l gpu={}'.format(job_properties['resources']['gpu'])
    except KeyError:
        gpu = ''
try:
    tmpfs = '-l tmpfs={}G'.format(job_properties['cluster']['tmpfs'])
except KeyError:
    try:
        tmpfs = '-l tmpfs={}G'.format(job_properties['resources']['tmpfs'])
    except KeyError:
        tmpfs = ''
try:
    openmpi = job_properties['cluster']['openmpi']
except KeyError:
    try:
        openmpi = job_properties['resources']['openmpi']
    except KeyError:
        openmpi = 0

# setting cluster job log PATHs
try:
    log_base = job_properties['log'][0]
except (KeyError, IndexError) as e:
    log_base = os.path.join(os.path.abspath(os.getcwd()), 'no-log_jobs')
log_base = os.path.splitext(log_base)[0]
os.makedirs(os.path.split(log_base)[0], exist_ok = True)
std_out = log_base + '_sge.out'
std_err = log_base + '_sge.err'
   

# formatting time if provided (assuming minutes)
if re.match('^[0-9]+$', str(time)):
    hours = int(int(time) / 60)
    minutes = int(time) % 60 
    time = '{:0>2}:{:0>2}:00'.format(hours, minutes)

# parallel env
if openmpi == 1:
    par_env = 'openmpi'
else:
    par_env = 'parallel'

# formatting qsub command
cmd = "qsub -cwd -pe {par_env} {n} -l h_vmem={mem}G -l h_rt={time} {gpu} {tmpfs} -o {std_out} -e {std_err} {job_script}"
cmd = cmd.format(par_env=par_env, n=n, mem=mem, time=time, gpu=gpu, tmpfs=tmpfs,
                 std_out=std_out, std_err=std_err, job_script=job_script)

#sys.stderr.write(cmd + '\n')

# subprocess job: qsub
try:
    res = subprocess.run(cmd, check=True, shell=True, stdout=subprocess.PIPE)
except subprocess.CalledProcessError as e:
    raise e

# get qsub job ID
res = res.stdout.decode()
try:
    m = re.search("Your job (\d+)", res)
    jobid = m.group(1)
    print(jobid)
except Exception as e:
    print(e)
    raise
