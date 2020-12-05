#!/bin/sh

# upload seed crowd in persona

basedir=$(cd "$(dirname "$0")"; pwd)
LIBS_DIR=${basedir}/../lib

# read Azkaban environment profile

# 批次号
batchNo=$(grep "batchNo" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "crowd upload batchNo : ${batchNo}"

result=$(echo ${batchNo} | grep "CROWD_UPLOAD")
if [[ ${result} == "" ]]
then
	exit 0
fi

# uploadID
uploadId=$(grep "uploadId" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "crowd upload uploadId : ${uploadId}"

# 流程表ID
processId=$(grep "processId" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "crowd upload processId : ${processId}"

# 人群表ID
crowdId=$(grep "crowdId" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "crowd upload crowdId : ${crowdId}"

# localPath
localPath=$(grep "localPath" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "crowd upload localPath : ${localPath}"

# remotePath
remotePath=$(grep "remotePath" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//' | sed 's/\\//g')
echo "crowd upload remotePath : ${remotePath}"

# interestPath
interestPath=$(grep "interestPath" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "crowd upload interestPath : ${interestPath}"

# 展示字段
varColumns=$(grep "varColumns" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "crowd upload varColumns : ${varColumns}"

# 固定字段
fixColumns=$(grep "fixColumns" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//' | sed 's/\\//g' | sed 's/"/\\"/g')
echo "crowd upload fixColumns : ${fixColumns}"

# 将hdfs文件数据导入到persona.device_upload
# kinit -kt /home/dmp_app_user/dmp_app_user.keytab dmp_app_user/cdh-master.lavapm@LAVAPM.COM
# uploadSql="LOAD DATA INPATH '/user/dmp_app_user/upload/${batchNo}' OVERWRITE INTO TABLE persona.device_upload PARTITION (batch_no='${batchNo}');"
# hive -e "set mapred.job.queue.name=root.dmp;${uploadSql}"

# 调用scala代码，统计匹配数据，将匹配的lava_id导入到persona.user_info_ready
spark-submit \
  --master yarn \
  --queue root.dmp \
  --deploy-mode client \
  --keytab /home/dmp_app_user/dmp_app_user.keytab \
  --principal dmp_app_user/cdh-slaver1.asia-east1-b.c.uplifted-agency-234209.internal@LAVAPM.COM \
  --class com.lavapm.persona.LavaIDMapping \
  --conf spark.shuffle.consolidateFiles=true \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.kryoserializer.buffer.max=1G \
  --num-executors=8 \
  --executor-memory=6G \
  --executor-cores=4 \
  --driver-memory=4G \
  ${LIBS_DIR}/spark-core.jar ${batchNo}

#参数传递
# str="{\"batchNo\":\"${batchNo}\",\"processId\":\"${processId}\",\"varId\":\"${uploadId}\",\"crowdId\":\"${crowdId}\",\"localPath\":\"${localPath}\",\"remotePath\":\"${remotePath}\",\"interestPath\":\"${interestPath}\",\"varColumns\":\"${varColumns}\",\"fixColumns\":\"${fixColumns}\"}"
# echo -e "$str" >> ${JOB_OUTPUT_PROP_FILE}
echo -e '{"batchNo":"'"${batchNo}"'","processId":"'"${processId}"'","varId":"'"${uploadId}"'","crowdId":"'"${crowdId}"'","localPath":"'"${localPath}"'","remotePath":"'"${remotePath}"'","interestPath":"'"${interestPath}"'","varColumns":"'"${varColumns}"'","fixColumns":"'"${fixColumns}"'"}' >> ${JOB_OUTPUT_PROP_FILE}
exit 0