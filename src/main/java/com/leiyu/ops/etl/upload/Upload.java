package com.leiyu.ops.etl.upload;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.leiyu.ops.etl.util.DbUtil;
import com.leiyu.ops.etl.util.FileUtil;

public class Upload {
	private static final Logger logger = LoggerFactory.getLogger(Upload.class);

	public static void init(String batchNo) {
		// process_flow table
		String queryByBatchNo = "select * from persona.process_flow where batch_no = ?";
		Map<Integer, String> params = Maps.newHashMap();
		params.put(1, batchNo);
		Map<String, Object> process_flow = DbUtil.queryOne(queryByBatchNo, params);
		String processId = process_flow.get("id").toString();
		logger.info("=========processId========" + processId);
		String crowdId = process_flow.get("crowd_id").toString();
		logger.info("=========crowdId========" + crowdId);
		String tenant = process_flow.get("tenant_name").toString();
		logger.info("=========tenant========" + tenant);
		// crowd_upload table
		String queryByCrowdId = "select * from persona.crowd_upload where crowd_id = " + crowdId;
		params.clear();
		Map<String, Object> crowd_upload = DbUtil.queryOne(queryByCrowdId, params);
		String uploadId = crowd_upload.get("id").toString();
		// fixColumns
		Map<String, String> fixMap = Maps.newHashMap();
		fixMap.put("tenant_name", tenant);
		fixMap.put("crowd_id", crowdId);
		fixMap.put("batch_no", batchNo);
		String fixColumns = JSONObject.toJSONString(fixMap);
		logger.info("=========fixColumns========" + fixColumns);
		// meta_attribute_group_relationship table
		String queryByRelCode = "select * from meta.meta_attribute_group_relationship where relationship_code = ? and tenant_id = ?";
		params.put(1, "search_result_visible");
		params.put(2, tenant);
		List<Map<String, Object>> metaList = DbUtil.queryList(queryByRelCode, params);
		List<String> list = Lists.newArrayList();
		for (Map<String, Object> meta : metaList) {
			list.add(meta.get("attribute_code").toString());
		}
		// variableColumns
		String varColumns = String.join(",", list);
		logger.info("=========varColumns========" + varColumns);
		// console
		params.clear();
		String queryParam = "select * from console.param where param_key = ?";
		params.put(1, "dmp.es.local.path");
		Map<String, Object> console = DbUtil.queryOne(queryParam, params);
		String localPath = console.get("param_value").toString();
		logger.info("=========localPath========" + localPath);
		File file = new File(localPath);
		// 如果文件夹不存在则创建
		if (!file.exists() && !file.isDirectory()) {
			file.mkdirs();
		}
		params.put(1, "dmp.es.remote.path");
		console = DbUtil.queryOne(queryParam, params);
		String remotePath = console.get("param_value").toString();
		logger.info("=========remotePath========" + remotePath);
		params.put(1, "dmp.interesttag.path");
		console = DbUtil.queryOne(queryParam, params);
		String interestPath = console.get("param_value").toString();
		logger.info("=========interestPath========" + interestPath);
		File interestFile = new File(interestPath);
		// 如果文件夹不存在则创建
		if (!interestFile.exists() && !interestFile.isDirectory()) {
			interestFile.mkdirs();
		}
		String fileName = System.getenv("JOB_OUTPUT_PROP_FILE");
		Map<String, String> contentMap = Maps.newHashMap();
		contentMap.put("processId", processId);
		contentMap.put("crowdId", crowdId);
		contentMap.put("uploadId", uploadId);
		contentMap.put("batchNo", batchNo);
		contentMap.put("localPath", localPath);
		contentMap.put("remotePath", remotePath);
		contentMap.put("interestPath", interestPath);
		contentMap.put("fixColumns", fixColumns);
		contentMap.put("varColumns", varColumns);
		// 传递参数
		String content = JSONObject.toJSONString(contentMap);
		FileUtil.writeFile(fileName, content);
	}
}
