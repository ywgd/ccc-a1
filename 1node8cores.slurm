#!/bin/bash

#SBATCH --partition=physical
#SBATCH --output=1node8cores.out
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --time=1-0:0:00

cd /home/$USER
module load python/3.7.4

mpirun -n 8 python main.py