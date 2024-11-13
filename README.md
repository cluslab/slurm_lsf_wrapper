# slurm_lsf_wrapper
### 本项目将slurm的命令包装成LSF的常用命令，使得在slurm集群下的用户可以使用LSF的常用命令行
### 目前支持的命令包括：
1. bkill
2. bjobs
3. bhosts
4. bstop
5. bresume
6. bresources
7. bqueues
8. blaunch
9. lsload
10. lsid
11. lshosts
12. bsub

### 用-h命令查看支持的option，例如：
    [root@admin01]# bsub -h
    Usage:
         bsub [options] command [arguments]
    Options:
    -h
        Brief help message

    -q <queue>
        Specify the queue that this job will run on

    -J <job_name>
        Specify the name of this job

    -o <out_path>
        Specify the stdout output path for this job

    -e <error_path>
        Specify the stderr output path for this job

    -x
        Run this job in exclusive mode.
        Job will not share nodes with other jobs.

    -cwd <work_dir>
        Specify the working directory for this job

    -n <task_num>
        Specify task number of this job

    -W <time>
        Specify the runtime(minutes) limit of the job.

    -m <hostlist>
        Space separated list of hosts that this job will run on.

    -I
        Submits an interactive job.`

### 使用方法：
将wrappers目录下的.py文件，拷贝到slurm的bin目录下，去掉.py后缀即可运行
