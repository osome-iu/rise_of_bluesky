#!/bin/sh
#SBATCH --mail-type=ALL
#SBATCH --mail-user=seckinozgurcan@gmail.com
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=300GB  # memory in GB
#SBATCH --time=1-10:00:00  # time requested in hour:minute:second

source /N/u/oseckin/Quartz/miniconda3/etc/profile.d/conda.sh
conda activate coordination2vec
cd /N/u/oseckin/BigRed200/bluesky_brazilian_migration/scripts/follow_unfollow
python concat_follow_unfollow_info.py