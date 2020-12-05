#!/bin/sh

# generate the user detail and interest statistics.

basedir=$(cd "$(dirname "$0")"; pwd)
LIBS_DIR=${basedir}/../lib

# read Azkaban environment profile

# 批次号
batchNo=$(grep "batchNo" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "es details batchNo : ${batchNo}"

# varId: tagId,uploadId,enlargeId
varId=$(grep "varId" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "es details varId : ${varId}"

# 流程表ID
processId=$(grep "processId" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "es details processId : ${processId}"

# 人群表ID
crowdId=$(grep "crowdId" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "es details crowdId : ${crowdId}"

# localPath
localPath=$(grep "localPath" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "es details localPath : ${localPath}"

# remotePath
remotePath=$(grep "remotePath" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//' | sed 's/\\//g')
echo "es details remotePath : ${remotePath}"

# interestPath
interestPath=$(grep "interestPath" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "es details interestPath : ${interestPath}"

# 展示字段
varColumns=$(grep "varColumns" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "es details varColumns : ${varColumns}"

# 固定字段
fixColumns=$(grep "fixColumns" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//' | sed 's/\\//g' | sed 's/"//g' | sed 's/{//g' | sed 's/}//g')
echo "es details fixColumns : ${fixColumns}"

# 拼接展示字段
jsonStr="'{\"lava_id\":\"',b.lava_id,"
array=(${varColumns//,/ })
for column in ${array[@]}
do
	jsonColumn="'\",\"${column}\":\"',${column},"
	jsonStr=${jsonStr}${jsonColumn}
done

# 拼接固定字符
array2=(${fixColumns//,/ })
for column in ${array2[@]}
do
	name=`echo "${column}"|awk -F ':' '{print $1}'`
	value=`echo "${column}"|awk -F ':' '{print $2}'`
	jsonColumn="'\",\"${name}\":\"','${value}',"
	jsonStr=${jsonStr}${jsonColumn}
done
jsonStr=${jsonStr}"'\"}'"
echo "es details jsonStr : ${jsonStr}"

# 将数据明细以json格式放到本地
fileName="${batchNo}.txt"
reSql="select concat(${jsonStr}) as value FROM persona.user_info_ready a INNER JOIN persona.user_info b ON b.lava_id=a.lava_id and a.batch_no='${batchNo}' limit 10000;"
echo "es details reSql : ${reSql}"
kinit -kt /home/dmp_app_user/hive.keytab hive/cdh-slaver1.asia-east1-b.c.uplifted-agency-234209.internal@LAVAPM.COM
hive -e "set mapred.job.queue.name=root.dmp;
set hive.execution.engine=mr;
set hive.mapjoin.smalltable.filesize=50000000;
set hive.auto.convert.join = false; ${reSql}" > ${localPath}${fileName}

# 将文件copy到有es和logstash的远程服务器，本地服务器和远程服务器要免密
scp ${localPath}${fileName} ${remotePath}
#rm ${localPath}${fileName}

# 生成interest
interestSql="INSERT OVERWRITE TABLE persona.user_interest_tag PARTITION (batch_no='${batchNo}') select b.interest_tag FROM persona.user_info_ready a INNER JOIN persona.user_info b ON b.lava_id=a.lava_id and a.batch_no='${batchNo}';"
echo "es details interestSql : ${interestSql}"
hive -e "set mapred.job.queue.name=root.dmp;${interestSql}"

# 生成统计信息
interestHdfs=/user/dmp_app_user/interest/${batchNo}/
spark-submit \
  --master yarn \
  --queue root.dmp \
  --deploy-mode client \
  --keytab /home/dmp_app_user/dmp_app_user.keytab \
  --principal dmp_app_user/cdh-slaver1.asia-east1-b.c.uplifted-agency-234209.internal@LAVAPM.COM \
  --class com.lavapm.persona.ProcessInterestTag \
  --conf spark.shuffle.consolidateFiles=true \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.kryoserializer.buffer.max=1G \
  --num-executors=4 \
  --executor-memory=6G \
  --executor-cores=4 \
  --driver-memory=2G \
  ${LIBS_DIR}/spark-core.jar ${batchNo}

kinit -kt /home/dmp_app_user/dmp_app_user.keytab dmp_app_user/cdh-slaver1.asia-east1-b.c.uplifted-agency-234209.internal@LAVAPM.COM
echo "hdfs dfs -getmerge ${interestHdfs} ${interestPath}${batchNo}"
hdfs dfs -getmerge ${interestHdfs} ${interestPath}${batchNo}
#hdfs dfs -rm -r ${interestHdfs}

# 更新impala
impala-shell -q "INVALIDATE METADATA;" -i cdh-slaver1

#参数传递
# str="{\"batchNo\":\"${batchNo}\",\"processId\":\"${processId}\",\"varId\":\"${varId}\",\"crowdId\":\"${crowdId}\",\"interestPath\":\"${interestPath}\"}"
# echo -e "$str" >> ${JOB_OUTPUT_PROP_FILE}
echo -e '{"batchNo":"'"${batchNo}"'","processId":"'"${processId}"'","varId":"'"${varId}"'","crowdId":"'"${crowdId}"'","interestPath":"'"${interestPath}"'"}' >> ${JOB_OUTPUT_PROP_FILE}
exit 0