#!/bin/sh

# hive operation: retrieve the lava_id and load into persona.user_info_ready

basedir=$(cd "$(dirname "$0")"; pwd)
LIBS_DIR=${basedir}/../lib

# read Azkaban environment profile

# 批次号
batchNo=$(grep "batchNo" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "basic tag batchNo : ${batchNo}"

result=$(echo ${batchNo} | grep "BASIC_TAG")
if [[ ${result} == "" ]]
then
	exit 0
fi

# 标签ID
tagId=$(grep "tagId" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "basic tag tagId : ${tagId}"

# 查询SQL
# querySql=$(grep "querySql" ${JOB_PROP_FILE} | sed 's/\r//' | sed 's/\\//g')
querySql=$(grep "querySql" ${JOB_PROP_FILE} | sed 's/\r//')
querySql=${querySql#*=}
querySql=$(echo -ne $querySql | iconv -f utf-8)
querySql=$(echo ${querySql//\\/})
echo "basic tag querySql : ${querySql}"
echo "basic tag querySql : ${querySql}"

# 流程表ID
processId=$(grep "processId" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "basic tag processId : ${processId}"

# 人群表ID
crowdId=$(grep "crowdId" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "basic tag crowdId : ${crowdId}"

# localPath
localPath=$(grep "localPath" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "basic tag localPath : ${localPath}"

# remotePath
remotePath=$(grep "remotePath" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//' | sed 's/\\//g')
echo "basic tag remotePath : ${remotePath}"

# interestPath
interestPath=$(grep "interestPath" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "basic tag interestPath : ${interestPath}"

# 展示字段
varColumns=$(grep "varColumns" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "basic tag varColumns : ${varColumns}"

# 固定字段
fixColumns=$(grep "fixColumns" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//' | sed 's/\\//g' | sed 's/"/\\"/g')
echo "basic tag fixColumns : ${fixColumns}"

# lava_id导入到persona.user_info_ready
kinit -kt /home/dmp_app_user/hive.keytab hive/cdh-slaver1.asia-east1-b.c.uplifted-agency-234209.internal@LAVAPM.COM
sql="INSERT OVERWRITE TABLE persona.user_info_ready PARTITION (batch_no='${batchNo}') "
hive -e "set mapred.job.queue.name=root.dmp;${sql}${querySql}"

#参数传递
#str="{\"batchNo\":\"${batchNo}\",\"processId\":\"${processId}\",\"varId\":\"${tagId}\",\"crowdId\":\"${crowdId}\",\"localPath\":\"${localPath}\",\"remotePath\":\"${remotePath}\",\"interestPath\":\"${interestPath}\",\"varColumns\":\"${varColumns}\",\"fixColumns\":\"${fixColumns}\"}"
#echo -e "$str" >> ${JOB_OUTPUT_PROP_FILE}
echo -e '{"batchNo":"'"${batchNo}"'","processId":"'"${processId}"'","varId":"'"${tagId}"'","crowdId":"'"${crowdId}"'","localPath":"'"${localPath}"'","remotePath":"'"${remotePath}"'","interestPath":"'"${interestPath}"'","varColumns":"'"${varColumns}"'","fixColumns":"'"${fixColumns}"'"}' >> ${JOB_OUTPUT_PROP_FILE}
exit 0