#!/bin/sh

# create system tag in persona

basedir=$(cd "$(dirname "$0")"; pwd)
LIBS_DIR=${basedir}/../lib

# read Azkaban environment profile

# 批次号
batchNo=$(grep "batchNo" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "system tag batchNo : ${batchNo}"

result=$(echo ${batchNo} | grep "SYSTEM_TAG")
if [[ ${result} == "" ]]
then
	exit 0
fi

# 标签ID
tagId=$(grep "tagId" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "system tag tagId : ${tagId}"

# 表名称 tableName, this table could be persona.user_info or customer sandboxkeyColumn e.g. "sandbox_cisco.sql_booking_rawdata"
tableName=$(grep "tableName" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "system tag tableName : ${tableName}"

# 关键字段 keyColumn, which is used to map to "lava_id", e.g. "mobile" 
keyColumn=$(grep "keyColumn" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "system tag keyColumn : ${keyColumn}"

# valueColumn, could be some keyword, e.g. "count"
valueColumn=$(grep "valueColumn" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "system tag valueColumn : ${valueColumn}"

# range, the audience range, e.g. "1st quarter,2nd quarter,3rd quarter,4th quarter,1st half,2nd half"
range=$(grep "dataRange" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "system tag range : ${range}"

# sample count
count=$(grep "count" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "system tag count : ${count}"

# 流程表ID
processId=$(grep "processId" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "system tag processId : ${processId}"

# 人群表ID
crowdId=$(grep "crowdId" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "system tag crowdId : ${crowdId}"

# localPath
localPath=$(grep "localPath" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "system tag localPath : ${localPath}"

# remotePath
remotePath=$(grep "remotePath" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//' | sed 's/\\//g')
echo "system tag remotePath : ${remotePath}"

# interestPath
interestPath=$(grep "interestPath" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "system tag interestPath : ${interestPath}"

# 展示字段
varColumns=$(grep "varColumns" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//')
echo "system tag varColumns : ${varColumns}"

# 固定字段
fixColumns=$(grep "fixColumns" ${JOB_PROP_FILE} | cut -d'=' -f2 | sed 's/\r//' | sed 's/\\//g' | sed 's/"/\\"/g')
echo "system tag fixColumns : ${fixColumns}"

# 路径
resPath=/user/dmp_app_user/model/systemTag/res/${batchNo}/
echo "resPath:"$resPath

kinit -kt /home/dmp_app_user/dmp_app_user.keytab dmp_app_user/cdh-slaver1.lavapm@LAVAPM.COM

# if count is blank, it uses the statistics script, otherwise, it use the sample script.
if [[ ${count} == "" ]]
then
	# clear hdfs目录
	hdfs dfs -rm -r ${resPath}
	# 调用systemTag scala代码
	# spark-submit --master yarn --queue root.dmp --deploy-mode client --keytab /home/dmp_app_user/dmp_app_user.keytab --principal dmp_app_user/cdh-master.lavapm@LAVAPM.COM --class com.lavapm.dmp.SystemTag  --conf spark.kryoserializer.buffer.max=1G --num-executors=10 --executor-memory=6G --executor-cores=4 --jars ${LIBS_DIR}/hive-hbase-handler-2.1.1-cdh6.2.0.jar ${LIBS_DIR}/spark-core.jar ${tableName} ${keyColumn} ${valueColumn} ${range} ${resPath}
	# spark-submit --master yarn --queue root.dmp --deploy-mode client --keytab /home/dmp_app_user/dmp_app_user.keytab --principal dmp_app_user/cdh-master.lavapm@LAVAPM.COM --class com.lavapm.dmp.SystemTag --conf spark.shuffle.consolidateFiles=true --conf spark.kryoserializer.buffer.max=1G --conf spark.dynamicAllocation.enabled=false --executor-cores=4 --num-executors=10 --executor-memory=6G --driver-memory=4G --jars ${LIBS_DIR}/hive-hbase-handler-2.1.1-cdh6.2.0.jar ${LIBS_DIR}/spark-core.jar ${tableName} ${keyColumn} ${valueColumn} ${range} ${resPath}
	# spark-submit --master yarn --queue root.dmp --deploy-mode client --keytab /home/dmp_app_user/dmp_app_user.keytab --principal dmp_app_user/cdh-master.lavapm@LAVAPM.COM --class com.lavapm.dmp.SystemTag --conf spark.shuffle.consolidateFiles=true --conf spark.kryoserializer.buffer.max=1G --conf spark.dynamicAllocation.enabled=false --executor-cores=4 --num-executors=10 --executor-memory=6G --driver-memory=4G --jars lib/hive-hbase-handler-2.1.1-cdh6.2.0.jar lib/spark-core.jar ${tableName} ${keyColumn} ${valueColumn} ${range} ${resPath}
	spark-submit \
	--master yarn \
	--queue root.dmp \
	--deploy-mode client \
	--keytab /home/dmp_app_user/dmp_app_user.keytab \
	--principal dmp_app_user/cdh-slaver1.asia-east1-b.c.uplifted-agency-234209.internal@LAVAPM.COM \
	--class com.lavapm.dmp.StatisticsTag \
	--conf spark.shuffle.consolidateFiles=true \
	--conf spark.kryoserializer.buffer.max=1G \
	--conf spark.dynamicAllocation.enabled=false \
	--num-executors=8 \
	--executor-memory=6G \
	--executor-cores=4 \
	--driver-memory=4G \
	--jars lib/hive-hbase-handler-2.1.1-cdh6.2.0.jar lib/spark-core.jar ${tableName} ${keyColumn} ${valueColumn} ${range} ${resPath}

	# 将匹配的lava_id导入到persona.user_info_ready
	kinit -kt /home/dmp_app_user/hive.keytab hive/cdh-slaver1.asia-east1-b.c.uplifted-agency-234209.internal@LAVAPM.COM
	loadSql="LOAD DATA INPATH '${resPath}' OVERWRITE INTO TABLE persona.user_info_ready PARTITION (batch_no='${batchNo}');"
	hive -e "set mapred.job.queue.name=root.dmp;${loadSql}"

	# 删除目录数据
	hdfs dfs -rm -r ${resPath}
else
	spark-submit \
	--master yarn \
	--queue root.dmp \
	--deploy-mode client \
	--keytab /home/dmp_app_user/dmp_app_user.keytab \
	--principal dmp_app_user/cdh-slaver1.asia-east1-b.c.uplifted-agency-234209.internal@LAVAPM.COM \
	--class com.lavapm.persona.UserSample \
	--conf spark.shuffle.consolidateFiles=true \
	--conf spark.kryoserializer.buffer.max=1G \
	--conf spark.dynamicAllocation.enabled=false \
	--executor-cores=4 \
	--num-executors=10 \
	--executor-memory=6G \
	--driver-memory=4G lib/spark-core.jar ${batchNo} ${count}
fi

#参数传递
echo -e '{"batchNo":"'"${batchNo}"'","processId":"'"${processId}"'","varId":"'"${tagId}"'","crowdId":"'"${crowdId}"'","localPath":"'"${localPath}"'","remotePath":"'"${remotePath}"'","interestPath":"'"${interestPath}"'","varColumns":"'"${varColumns}"'","fixColumns":"'"${fixColumns}"'"}' >> ${JOB_OUTPUT_PROP_FILE}
exit 0