package com.leiyu.ops.etl.update;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.leiyu.ops.etl.util.DbUtil;
import com.leiyu.ops.etl.util.PropertiesUtil;


/**
 * Update the status of the calculation.
 *
 * @author: scottie
 * @date:2020-02-20
 */
public class Update {

	private static final Logger logger = LoggerFactory.getLogger(Update.class);

	public static void main(String[] args) {

		// 获取azkaban的系统文件路径
		String jobPropFile = System.getenv("JOB_PROP_FILE");
		PropertiesUtil propertiesUtil = new PropertiesUtil(jobPropFile);

		// 批次号
		String batchNo = propertiesUtil.readProperty("batchNo");

		// 人群ID
		String crowdId = propertiesUtil.readProperty("crowdId");

		// 流程表ID
		String processId = propertiesUtil.readProperty("processId");

		// varId: tagId,uploadId,enlargeId
		String varId = propertiesUtil.readProperty("varId");

		// success 成功标志 true false
		Boolean success = Boolean.valueOf(propertiesUtil.readProperty("success"));

		int status = 2;
		if (!success) {
			status = -1;
		}

		if (batchNo.contains("BASIC_TAG")) {
			String updateTag = "update persona.basic_tag set status = " + status + " where id = " + varId;
			DbUtil.execute(updateTag);

		}

		if (batchNo.contains("CROWD_UPLOAD")) {
			status = success ? 5 : -1;
			String updateUpload = "update persona.crowd_upload set status = " + status + " where id = " + varId;
			DbUtil.execute(updateUpload);

		}

		if (batchNo.contains("SYSTEM_TAG")) {
			String updateUpload = "update persona.system_tag set status = " + status + " where id = " + varId;
			DbUtil.execute(updateUpload);
		}

		if (batchNo.contains("CROWD_ENLARGE")) {
			String updateEnlarge = "update persona.crowd_enlarge set status = " + status + " where id = " + varId;
			DbUtil.execute(updateEnlarge);
		}

		status = success ? 2 : -1;

		// 更新人群表
		String updateCrowd = "update persona.crowd set status = " + status + " where id = " + crowdId;
		DbUtil.execute(updateCrowd);

		// 更新流程表
		String updateFlow = "update persona.process_flow set status = " + status + " where id = " + processId;
		DbUtil.execute(updateFlow);
		String updateFlow2 = "update persona.process_flow set user_profile_status = " + status + " where id = "	+ processId;
		DbUtil.execute(updateFlow2);

		logger.info(">>>>>>>>>>>>>>>>>>" + batchNo + "<<<<<<<<<<<<<<<<<<");
		logger.info("===================complete!=============");
	}
}
