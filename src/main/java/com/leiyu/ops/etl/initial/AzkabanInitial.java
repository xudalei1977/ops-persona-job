package com.leiyu.ops.etl.initial;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.leiyu.ops.etl.enlarge.Enlarge;
import com.leiyu.ops.etl.tag.BasicTag;
import com.leiyu.ops.etl.tag.SystemTag;
import com.leiyu.ops.etl.upload.Upload;

public class AzkabanInitial {

	private static final Logger logger = LoggerFactory.getLogger(AzkabanInitial.class);

	public static void main(String[] args) {

		String batchNo = args[0];
		if (StringUtils.isBlank(batchNo)) {
			logger.error("批次号为空值");
			System.exit(1);
		}

		logger.info("==========batchNo==========" + batchNo);

		/**
		 * dispatch the initial methods by batchNo
		 * 来源：tag、upload、enlarge、system tag
		 */
		if (batchNo.contains("BASIC_TAG")) {
			BasicTag.init(batchNo);
		} else if (batchNo.contains("CROWD_ENLARGE")) {
			Enlarge.init(batchNo);
		} else if (batchNo.contains("CROWD_UPLOAD")) {
			Upload.init(batchNo);
		} else if (batchNo.contains("SYSTEM_TAG")) {
			SystemTag.init(batchNo);
		} else {
			logger.error("批次号格式异常");
			System.exit(1);
		}
	}
}
