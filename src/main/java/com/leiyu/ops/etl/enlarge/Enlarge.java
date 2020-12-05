package com.leiyu.ops.etl.enlarge;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.leiyu.ops.etl.util.DbUtil;
import com.leiyu.ops.etl.util.FileUtil;

public class Enlarge {
	private static final Logger logger = LoggerFactory.getLogger(Enlarge.class);

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
		// crowd_enlarge table
		String queryByCrowdId = "select * from persona.crowd_enlarge where crowd_id = " + crowdId;
		params.clear();
		Map<String, Object> crowd_enlarge = DbUtil.queryOne(queryByCrowdId, params);
		String enlargeId = crowd_enlarge.get("id").toString();
		String seedId = crowd_enlarge.get("seed_crowd_id").toString();
		String enlargeCount = crowd_enlarge.get("enlarge_count").toString();
		// modelClass
		String algorithm = "lookalike";
		String modelClass = "com.leiyu.ops.model.Lookalike";
		String algorithmId = crowd_enlarge.get("algorithm_id") == null ? ""
				: crowd_enlarge.get("algorithm_id").toString();
		if (!StringUtils.isBlank(algorithmId)) {
			logger.info("=========algorithmId========" + algorithmId);
			String querySriptById = "select b.script_path as script_path from console.algorithm a, console.script_info b where a.handle_script_id = b.id and a.id = " + algorithmId;
			Map<String, Object> script = DbUtil.queryOne(querySriptById, params);
			String scriptPath = script.get("script_path").toString();
			if(scriptPath.contains("similarity")){
				algorithm = "similarity";
				modelClass = "com.leiyu.ops.model.Similarity";
			}
		}
		logger.info("=========algorithm========" + algorithm);
		logger.info("=========modelClass========" + modelClass);
		// process_flow table
		String queryBatchNoByCrowdId = "select * from persona.process_flow where user_profile_status = 2 and status = 2 and crowd_id = "
				+ seedId + " order by id desc limit 1";
		Map<String, Object> process_flow2 = DbUtil.queryOne(queryBatchNoByCrowdId, params);
		String seedBatchNo = process_flow2.get("batch_no").toString();
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
		File localFile = new File(localPath);
		// 如果文件夹不存在则创建
		if (!localFile.exists() && !localFile.isDirectory()) {
			localFile.mkdirs();
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
		// model

		String fileName = System.getenv("JOB_OUTPUT_PROP_FILE");
		Map<String, String> contentMap = Maps.newHashMap();
		contentMap.put("processId", processId);
		contentMap.put("crowdId", crowdId);
		contentMap.put("seedBatchNo", seedBatchNo);
		contentMap.put("enlargeId", enlargeId);
		contentMap.put("enlargeCount", enlargeCount);
		contentMap.put("batchNo", batchNo);
		contentMap.put("localPath", localPath);
		contentMap.put("remotePath", remotePath);
		contentMap.put("interestPath", interestPath);
		contentMap.put("fixColumns", fixColumns);
		contentMap.put("varColumns", varColumns);
		contentMap.put("algorithm", algorithm);
		contentMap.put("modelClass", modelClass);
		// 传递参数
		String content = JSONObject.toJSONString(contentMap);
		FileUtil.writeFile(fileName, content);
	}
}
