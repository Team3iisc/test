#!/bin/bash

# set the number of nodes and processes per node
#PBS -l select=24:mpiprocs=8

# set max wallclock time
#PBS -l walltime=100:00:00

# set name of job
#PBS -N se256-beta

# mail alert at (b)eginning, (e)nd and (a)bortion of execution
#PBS -m bea

# send mail to the following address
#PBS -M krish2100@gmail.com

# use submission environment
#PBS -V

# start job from the directory it was submitted
#cd $PBS_O_WORKDIR

# define MPI host details
#. enable_hal_mpi.sh

# run through the mpirun launcher

spark-submit --class org.test.spark.examples.preprocess --master yarn --deploy-mode cluster --executor-memory 7G --num-executors $2 ./Scalable-Project-0.1-jar-with-dependencies.jar $1 $2


