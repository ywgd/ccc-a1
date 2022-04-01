#!/bin/bash

#SBATCH --partition=physical
#SBATCH --output=1node8cores.out
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=1-0:0:00


module load python/3.7.4

mpirun -n 1 python main.py