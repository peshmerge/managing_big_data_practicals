# written by 
# Manish Mishra s2801620
# Peshmerge Morad s2449471

# This computes the top 100 hashtags and their counts for the tweets in the database at /data/doina/Twitter-Archive.org.

# This program is written in Bash + Python3

# To execute on a machine:
# chmod +755 ./BENCH3-s2801620-s2449471-DRIVERFILE.sh && ./BENCH3-s2801620-s2449471-DRIVERFILE.sh

run_pyspark() {
    echo "Using ${EXECUTORS} executors"
    set -x
    spark-submit --deploy-mode cluster \
    --master yarn \
    --executor-memory ${MEMORY} \
    --conf "spark.pyspark.python=/usr/bin/python3.6" \
    --conf "spark.pyspark.driver.python=/usr/bin/python3.6" \
    --conf "spark.dynamicAllocation.maxExecutors=${EXECUTORS}" \
    BENCH3-s2801620-s2449471-MKMPM.py ${TWEETS_LOC} ${OUTPUT_DIR} ${EXECUTORS} &> out_${EXECUTORS}.log
    set +x
    hdfs dfs -du -s -h ${OUTPUT_DIR}*
    echo "Run finished on ${TWEETS_DIR}"
}

TWEETS_DIR="/data/doina/Twitter-Archive.org/2017-01/0[1-3]"
TWEETS_LOC="/data/doina/Twitter-Archive.org/2017-01/0[1-3]/*/*.json.bz2"
OUTPUT_DIR='/user/s2801620/wk4_BENCH_test'
MEMORY='6G'

echo "Starting run for ${TWEETS_DIR} sizes:"
hdfs dfs -du -s -h ${TWEETS_DIR}
echo "Using ${MEMORY} memory"

for EXECUTORS in 4 8 12 16 20
do
    echo "Starting with ${EXECUTORS} executors..."
    run_pyspark
    sleep 10s
done