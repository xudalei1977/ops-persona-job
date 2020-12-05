#!/bin/sh

# crowd enlargement in persona

basedir=$(cd "$(dirname "$0")"; pwd)
LIBS_DIR=${basedir}/../lib

# read Azkaban environment profile

# 批次号
batchNo=$(grep "batchNo" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "crowd enlarge batchNo : ${batchNo}"

result=$(echo ${batchNo} | grep "CROWD_ENLARGE")
if [[ ${result} == "" ]]
then
	exit 0
fi

# 种子人群批次号
seedBatchNo=$(grep "seedBatchNo" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "crowd enlarge seedBatchNo : ${seedBatchNo}"

# enlargeId
enlargeId=$(grep "enlargeId" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "crowd enlarge enlargeId : ${enlargeId}"

# enlargeCount
enlargeCount=$(grep "enlargeCount" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "crowd enlarge enlargeCount : ${enlargeCount}"

# 流程表ID
processId=$(grep "processId" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "crowd enlarge processId : ${processId}"

# 人群表ID
crowdId=$(grep "crowdId" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "crowd enlarge crowdId : ${crowdId}"

# localPath
localPath=$(grep "localPath" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "crowd enlarge localPath : ${localPath}"

# remotePath
remotePath=$(grep "remotePath" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//' | sed 's/\\//g')
echo "crowd enlarge remotePath : ${remotePath}"

# interestPath
interestPath=$(grep "interestPath" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "crowd enlarge interestPath : ${interestPath}"

# 展示字段
varColumns=$(grep "varColumns" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "crowd enlarge varColumns : ${varColumns}"

# 固定字段
fixColumns=$(grep "fixColumns" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//' | sed 's/\\//g' | sed 's/"/\\"/g')
echo "crowd enlarge fixColumns : ${fixColumns}"

# algorithm
algorithm=$(grep "algorithm" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "crowd enlarge algorithm : ${algorithm}"

# modelClass
modelClass=$(grep "modelClass" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "crowd enlarge modelClass : ${modelClass}"

# 路径
posPath=/user/dmp_app_user/model/${algorithm}/pos/${batchNo}/
resPath=/user/dmp_app_user/model/${algorithm}/res/${batchNo}/
poolPath=/user/dmp_app_user/model/pool/
echo "posPath:"$posPath
echo "resPath:"$resPath
echo "poolPath:"$poolPath
echo "modelClass:"$modelClass

kinit -kt /home/dmp_app_user/dmp_app_user.keytab dmp_app_user/cdh-slaver1.asia-east1-b.c.uplifted-agency-234209.internal@LAVAPM.COM

# 删除hdfs种子人群目录
hdfs dfs -rm -r ${posPath}
hdfs dfs -rm -r ${resPath}
seedSql="INSERT OVERWRITE DIRECTORY '${posPath}' ROW FORMAT DELIMITED FIELDS TERMINATED BY'\t' select p.* from persona.user_interest_info p inner join persona.user_info_ready seed on p.lava_id = seed.lava_id where seed.batch_no='${seedBatchNo}';"
echo "seedSql:"seedSql
hive -e "set mapred.job.queue.name=root.dmp; ${seedSql}"

# 调用lookalike scala代码
spark-submit \
  --master yarn \
  --queue root.dmp \
  --deploy-mode client \
  --keytab /home/dmp_app_user/dmp_app_user.keytab \
  --principal dmp_app_user/cdh-slaver1.asia-east1-b.c.uplifted-agency-234209.internal@LAVAPM.COM \
  --class ${modelClass} \
  --conf spark.kryoserializer.buffer.max=1G \
  --num-executors=4 \
  --executor-memory=6G \
  --executor-cores=4 \
  --driver-memory=2G \
  ${LIBS_DIR}/spark-core.jar ${posPath} ${poolPath} ${resPath} ${enlargeCount}

# 将放大的lava_id导入到persona.user_info_ready
kinit -kt /home/dmp_app_user/hive.keytab hive/cdh-slaver1.asia-east1-b.c.uplifted-agency-234209.internal@LAVAPM.COM
enlargeSql="LOAD DATA INPATH '${resPath}' OVERWRITE INTO TABLE persona.user_info_ready PARTITION (batch_no='${batchNo}');"
hive -e "set mapred.job.queue.name=root.dmp;${enlargeSql}"

# 删除目录数据
hdfs dfs -rm -r ${posPath}
hdfs dfs -rm -r ${resPath}

#参数传递
# str="{\"batchNo\":\"${batchNo}\",\"processId\":\"${processId}\",\"varId\":\"${enlargeId}\",\"crowdId\":\"${crowdId}\",\"localPath\":\"${localPath}\",\"remotePath\":\"${remotePath}\",\"interestPath\":\"${interestPath}\",\"varColumns\":\"${varColumns}\",\"fixColumns\":\"${fixColumns}\"}"
# echo -e "$str" >> ${JOB_OUTPUT_PROP_FILE}
echo -e '{"batchNo":"'"${batchNo}"'","processId":"'"${processId}"'","varId":"'"${enlargeId}"'","crowdId":"'"${crowdId}"'","localPath":"'"${localPath}"'","remotePath":"'"${remotePath}"'","interestPath":"'"${interestPath}"'","varColumns":"'"${varColumns}"'","fixColumns":"'"${fixColumns}"'"}' >> ${JOB_OUTPUT_PROP_FILE}
exit 0