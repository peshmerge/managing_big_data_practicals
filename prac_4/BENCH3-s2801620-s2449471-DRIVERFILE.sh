# written by 
# Manish Mishra s2801620
# Peshmerge Morad s2449471

# This computes the top 100 hashtags and their counts for the tweets in the database at /data/doina/Twitter-Archive.org.

# This program is written in Bash + Python3

# To execute on a machine:
# chmod +755 ./BENCH3-s2801620-s2449471-DRIVERFILE.sh && ./BENCH3-s2801620-s2449471-DRIVERFILE.sh

TWEETS_DIR="/data/doina/Twitter-Archive.org/2020-01/"
TWEETS_LOC="/data/doina/Twitter-Archive.org/2020-01/0[1-2]/*/*.json.bz2"
OUTPUT_DIR='/user/s2801620/wk4_BENCH_out2020_01'
MEMORY='6G'
EXECUTORS='8'

# ^^^ This takes
# Aggregate Resource Allocation:	16225365 MB-seconds, 9009 vcore-seconds
# real    5m4.565s
# user    0m14.885s
# sys     0m3.086s
# 
# 4mins, 52sec from spark console
run_pyspark() {
    echo "Starting first run at ${TWEETS_DIR} sizes:"
    hdfs dfs -du -s -h ${TWEETS_DIR}
    echo "Using ${MEMORY} memory"
    echo "Using ${EXECUTORS} executors"
    set -x
    time spark-submit --deploy-mode cluster \
    --master yarn \
    --executor-memory ${MEMORY} \
    --conf "spark.pyspark.python=/usr/bin/python3.6" \
    --conf "spark.pyspark.driver.python=/usr/bin/python3.6" \
    --conf "spark.dynamicAllocation.maxExecutors=${EXECUTORS}" \
    BENCH3-s2801620-s2449471-MKMPM.py ${TWEETS_LOC} ${OUTPUT_DIR} 2> /dev/null
    set +x
    hdfs dfs -ls -d ${OUTPUT_DIR}*
    echo "Run finished on ${TWEETS_DIR}"
}

run_pyspark

TWEETS_DIR="/data/doina/Twitter-Archive.org/2017-01/0[1-3]"
TWEETS_LOC="/data/doina/Twitter-Archive.org/2017-01/0[1-3]/*/*.json.bz2"
OUTPUT_DIR='/user/s2801620/wk4_BENCH_out2017_01_6G'
MEMORY='6G'
EXECUTORS='8'

run_pyspark
# ^^^ This takes
# Aggregate Resource Allocation:	19263426 MB-seconds, 10697 vcore-seconds
# real    5m54.698s
# user    0m15.157s
# sys     0m3.059s
# 
# 5mins, 42sec from spark console


TWEETS_DIR="/data/doina/Twitter-Archive.org/2017-01/0[1-3]"
TWEETS_LOC="/data/doina/Twitter-Archive.org/2017-01/0[1-3]/*/*.json.bz2"
OUTPUT_DIR='/user/s2801620/wk4_BENCH_out2017_01_8G'
MEMORY='8G'
EXECUTORS='16'

run_pyspark