#!/bin/sh
#SBATCH --mail-type=ALL
#SBATCH --mail-user=seckinozgurcan@gmail.com
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=30GB  # memory in GB
#SBATCH --time=4-00:00:00  # time requested in hour:minute:second

source /N/u/oseckin/Quartz/miniconda3/etc/profile.d/conda.sh
conda activate coordination2vec
cd /N/u/oseckin/BigRed200/bluesky_brazilian_migration/scripts/toxicity_inference
python toxicity_inference_3.py