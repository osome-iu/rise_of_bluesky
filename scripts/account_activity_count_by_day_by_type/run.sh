#!/bin/sh
#SBATCH --mail-type=ALL
#SBATCH --mail-user=seckinozgurcan@gmail.com
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8 
#SBATCH --mem=300GB  # memory in GB
#SBATCH --time=2-10:00:00  # time requested in hour:minute:second

source /N/u/oseckin/Quartz/miniconda3/etc/profile.d/conda.sh
conda activate coordination2vec
cd /N/u/oseckin/BigRed200/bluesky_brazilian_migration/scripts/account_activity_count_by_day_by_type
python account_activity_count_by_day_by_type.py