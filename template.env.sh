export CONDAPATH=/lustre/t1/tmp/singulani/miniconda3/bin
source $CONDAPATH/activate
conda activate hipscat

# ulimit -u 600000
ulimit -s 50000
