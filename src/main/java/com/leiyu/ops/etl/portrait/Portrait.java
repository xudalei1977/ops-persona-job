package com.leiyu.ops.etl.portrait;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.leiyu.ops.etl.util.DbUtil;
import com.leiyu.ops.etl.util.FileUtil;
import com.leiyu.ops.etl.util.PropertiesUtil;


/**
 * Generate the crowd portrait.
 *
 * @author: scottie
 * @date:2020-02-20
 */
public class Portrait {
	private static final Logger logger = LoggerFactory.getLogger(Portrait.class);
	private static Connection conn;
	private static PreparedStatement pstsm;
	private static String batchNo;
	private static String crowdId;
	private static String processId;
	private static String varId;
	private static String interestPath;

	// impala
	private List<ImpalaHive2Config> impalaJdbc;
	private int currentIndex;
	private int totalServer;

	public Portrait() {

		impalaJdbc = Lists.newArrayList();

		impalaJdbc.add(new ImpalaHive2Config(
							"jdbc:hive2://cdh-slaver1:21050/default;principal=impala/cdh-slaver1.asia-east1-b.c.uplifted-agency-234209.internal@LAVAPM.COM"));
		impalaJdbc.add(new ImpalaHive2Config(
							"jdbc:hive2://cdh-slaver2:21050/default;principal=impala/cdh-slaver2.asia-east1-b.c.uplifted-agency-234209.internal@LAVAPM.COM"));
		impalaJdbc.add(new ImpalaHive2Config(
							"jdbc:hive2://cdh-slaver3:21050/default;principal=impala/cdh-slaver3.asia-east1-b.c.uplifted-agency-234209.internal@LAVAPM.COM"));
		impalaJdbc.add(new ImpalaHive2Config(
							"jdbc:hive2://cdh-slaver4:21050/default;principal=impala/cdh-slaver4.asia-east1-b.c.uplifted-agency-234209.internal@LAVAPM.COM"));

		totalServer = impalaJdbc.size();
		currentIndex = totalServer - 1;
	}

	// 轮询
	public ImpalaHive2Config round() {
		currentIndex = (currentIndex + 1) % totalServer;
		return impalaJdbc.get(currentIndex);
	}


	public static void main(String[] args) {

		// 获取azkaban的系统文件路径
		String jobPropFile = System.getenv("JOB_PROP_FILE");
		PropertiesUtil propertiesUtil = new PropertiesUtil(jobPropFile);

		// 批次号
		batchNo = propertiesUtil.readProperty("batchNo");
		logger.info("==========batchNo==========" + batchNo);

		// 人群ID
		crowdId = propertiesUtil.readProperty("crowdId");
		logger.info("==========crowdId==========" + crowdId);

		// 流程表ID
		processId = propertiesUtil.readProperty("processId");
		logger.info("==========processId==========" + processId);

		// varId: tagId,uploadId,enlargeId
		varId = propertiesUtil.readProperty("varId");
		logger.info("==========varId==========" + varId);

		// 兴趣标签统计数据文件地址
		interestPath = propertiesUtil.readProperty("interestPath");
		logger.info("==========interestPath==========" + interestPath);

		String success = "true";

		try {
			if (StringUtils.isBlank(batchNo) || StringUtils.isBlank(crowdId) || StringUtils.isBlank(processId)
					|| StringUtils.isBlank(varId) || StringUtils.isBlank(interestPath)) {
				logger.info("参数存在空值");
				success = "false";
			}

			// 查询画像展示字段
			Map<Integer, String> params = Maps.newHashMap();

			// get information from crowd table
			String queryCrowdById = "select * from persona.crowd where id = " + crowdId;
			Map<String, Object> crowd = DbUtil.queryOne(queryCrowdById, params);

			String tenant = crowd.get("tenant_name").toString();
			String create_by = crowd.get("create_by").toString();
			String creator = crowd.get("creator").toString();
			logger.info("=========tenant========" + tenant);

			// get the attributes in the portrait from meta_attribute_group_relationship
			String queryByRelCode = "select * from meta.meta_attribute_group_relationship where relationship_code = ? and tenant_id = ?";
			params.put(1, "terminal");
			params.put(2, tenant);
			List<Map<String, Object>> listTerminal = DbUtil.queryList(queryByRelCode, params);

			Map<String, List<String>> cMap = Maps.newHashMap();

			/**
			 * process all the attributes
			 * get the map like : {"persona.user_info_impala": "age_range, birth_city, birth_province, city_level, gender, ..."}
			 * get the main account table and primary key.
			 */
			String accountTable = "";
			String primarykey = "";
			for (Map<String, Object> terminal : listTerminal) {

				String attributeCode = terminal.get("attribute_code").toString();
				String objectCode = terminal.get("object_code").toString();

				// 兴趣单独处理
				if ("interest_tag".equals(attributeCode)) {
					continue;
				}

				// get the table_id
				String queryTableIdByObCode = "select a.table_id, b.object_type, b.account_id_attribute_code from meta.meta_object_table_relationship a, meta.object_code b " +
												"where a.object_code = ? and a.tenant_id = ? " +
												"and a.object_code = b.code";
				params.put(1, objectCode);
				params.put(2, tenant);
				Map<String, Object> metaObject = DbUtil.queryOne(queryTableIdByObCode, params);
				String tableId = metaObject.get("table_id").toString();

				// get the table and database name
				String queryTableById = "select * from meta.phy_table where id = " + tableId;
				params.clear();
				Map<String, Object> phyTable = DbUtil.queryOne(queryTableById, params);
				String dataBase = phyTable.get("data_source_code").toString();
				String table = phyTable.get("code").toString();

				// set the main account and primary key
				String objectType = metaObject.get("object_type").toString();
				if (objectType.equals("1")) {
					accountTable = dataBase + "." + table;
					primarykey = metaObject.get("account_id_attribute_code").toString();
				}

				// set the map
				List<String> clist = Lists.newArrayList();

				if (cMap.containsKey(dataBase + "." + table)) {
					clist = cMap.get(dataBase + "." + table);
					clist.add(attributeCode);
				} else {
					clist.add(attributeCode);
				}

				cMap.put(dataBase + "." + table, clist);
			}

			logger.info("获取人群画像：" + cMap);

			// 拼接对应的sql
			Map<String, String> portraitMap = Maps.newHashMap();
			String sql = "from persona.user_info_ready r inner join " + accountTable + " p on r.lava_id = p." + primarykey + " where r.batch_no='"
					+ batchNo + "'";

			for (Entry<String, List<String>> kV : cMap.entrySet()) {
				String dt = kV.getKey();
				for (String attCode : kV.getValue()) {
					String portraitSql = "select p." + attCode + ", count(1) as count " + sql.toString()
							+ " and trim(p." + attCode + ") !=''" + " group by p." + attCode;

					logger.info("portraitSql ========：" + portraitSql);
					portraitMap.put(dt + "." + attCode, portraitSql);
				}
			}

			// 刷新impala
			refresh();

			// 获取人群数量
			String countSql = "select count(1) as count from persona.user_info_ready where batch_no='" + batchNo + "'";
			int crowdCount = getCountFromImpala(countSql);

			// 人群数量画像
			Map<String, Map<String, Object>> portrait = Maps.newHashMap();
			getPortraitFromImpala(crowdCount, portraitMap, portrait);

			// 添加兴趣标签
			addInterestPortrait(portrait);

			// update the portrait to crowd_portrait table
			String getCrowdPortraitByCrowdId = "select * from persona.crowd_portrait where crowd_id = " + crowdId;
			params.clear();

			Map<String, Object> crowd_portrait = DbUtil.queryOne(getCrowdPortraitByCrowdId, params);
			String portraitStr = JSON.toJSONString(portrait);

			if (crowd_portrait != null && !crowd_portrait.isEmpty()) {
				String portraitId = crowd_portrait.get("id").toString();
				String updatePortrait = "update persona.crowd_portrait set portrait = '" + portraitStr + "' where id = "
						+ portraitId;
				DbUtil.execute(updatePortrait);
			} else {
				String insertPortrait = "INSERT INTO persona.crowd_portrait (crowd_id, portrait, tenant_name, create_by, creator) VALUES ("
						+ crowdId + ", '" + portraitStr + "', '" + tenant + "', '" + create_by + "', '" + creator
						+ "')";
				DbUtil.execute(insertPortrait);
			}

			// update basic_tag table
			if (batchNo.contains("BASIC_TAG")) {
				String updateTag = "update persona.basic_tag set crowd_count = " + crowdCount + " where id = " + varId;
				DbUtil.execute(updateTag);

			}

			// update crowd_upload table
			if (batchNo.contains("CROWD_UPLOAD")) {
				String updateUpload = "update persona.crowd_upload set match_count = " + crowdCount + " where id = "
						+ varId;
				DbUtil.execute(updateUpload);
			}

			// update system_tag table
			if (batchNo.contains("SYSTEM_TAG")) {
				String updateUpload = "update persona.system_tag set crowd_count = " + crowdCount + " where id = "
						+ varId;
				DbUtil.execute(updateUpload);
			}


			// update crowd table
			String updateCrowd = "update persona.crowd set crowd_count = " + crowdCount + " where id = " + crowdId;
			DbUtil.execute(updateCrowd);

			// update process_flow table
			String updateFlow = "update persona.process_flow set crowd_count = " + crowdCount + " where id = " + processId;
			DbUtil.execute(updateFlow);

		} catch (Exception e) {
			success = "false";
		}

		String fileName = System.getenv("JOB_OUTPUT_PROP_FILE");
		Map<String, String> contentMap = Maps.newHashMap();
		contentMap.put("crowdId", crowdId);
		contentMap.put("batchNo", batchNo);
		contentMap.put("processId", processId);
		contentMap.put("varId", varId);
		contentMap.put("success", success);

		// 传递参数
		String content = JSONObject.toJSONString(contentMap);
		FileUtil.writeFile(fileName, content);
	}


	// execute "invalidate metadata" in impala
	public static void refresh() {
		try {

			PropertiesUtil p = new PropertiesUtil("dmp-etl.properties");
			Class.forName(p.readResourceProperty("impaladriverClassName")).newInstance();

			// 登录Kerberos账号
			System.setProperty("java.security.krb5.conf", p.readResourceProperty("krb.conf.path"));
			Configuration configuration = new Configuration();
			configuration.set("hadoop.security.authentication", "Kerberos");
			UserGroupInformation.setConfiguration(configuration);
			UserGroupInformation.loginUserFromKeytab(p.readResourceProperty("krb.authen.user"),
													p.readResourceProperty("krb.authen.keytab"));

			// connect to impala
			conn = DriverManager.getConnection(p.readResourceProperty("impalaurl"));
			pstsm = conn.prepareStatement("INVALIDATE METADATA;");

			boolean resultSet = pstsm.execute();

		} catch (Exception e) {
			logger.info("impala refresh fail:" + e);
		} finally {
			try {
				if (pstsm != null) {
					pstsm.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// get the count of the crowd
	public static int getCountFromImpala(String sql) {

		int count = 0;

		try {
			PropertiesUtil p = new PropertiesUtil("dmp-etl.properties");
			logger.info("CrowdCountSql:=============" + sql);

			Class.forName(p.readResourceProperty("impaladriverClassName")).newInstance();

			// 登录Kerberos账号
			System.setProperty("java.security.krb5.conf", p.readResourceProperty("krb.conf.path"));
			Configuration configuration = new Configuration();
			configuration.set("hadoop.security.authentication", "Kerberos");

			UserGroupInformation.setConfiguration(configuration);
			UserGroupInformation.loginUserFromKeytab(p.readResourceProperty("krb.authen.user"),
													p.readResourceProperty("krb.authen.keytab"));
			conn = DriverManager.getConnection(p.readResourceProperty("impalaurl"));

			logger.info("getCountFromImpala sql : " + sql);

			pstsm = conn.prepareStatement(sql);
			ResultSet resultSet = pstsm.executeQuery();

			while (resultSet.next()) {
				count = resultSet.getInt(1);
			}

		} catch (Exception e) {
			logger.info("getCountFromImpala fail:" + e);
		} finally {
			try {
				if (pstsm != null) {
					pstsm.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		logger.info("COUNT:=============" + count);
		return count;
	}


	// get the portrait of the crowd
	public static void getPortraitFromImpala(int crowdCount, Map<String, String> portraitSql,
												Map<String, Map<String, Object>> portrait) {
		try {
			PropertiesUtil p = new PropertiesUtil("dmp-etl.properties");

			if (crowdCount == 0) {
				return;
			}

			Portrait r = new Portrait();

			logger.info("getPortraitFromImpala columnsList= " + portraitSql);

			for (Entry<String, String> entry : portraitSql.entrySet()) {

				// Kerberos认证连接impala
				System.setProperty("java.security.krb5.conf", p.readResourceProperty("krb.conf.path"));
				Configuration configuration = new Configuration();
				configuration.set("hadoop.security.authentication", "Kerberos");

				UserGroupInformation.setConfiguration(configuration);
				UserGroupInformation.loginUserFromKeytab(p.readResourceProperty("krb.authen.user"),
														p.readResourceProperty("krb.authen.keytab"));

				conn = DriverManager.getConnection(r.round().getJdbcUrl());

				// execute the query in impala
				String column = entry.getKey().split("\\.")[2];
				logger.info("Portraitcolumn:=============" + column);
				logger.info("PortraitSql:=============" + entry.getValue());

				pstsm = conn.prepareStatement(entry.getValue());
				ResultSet resultSet = pstsm.executeQuery();

				Map<String, Object> countMap = Maps.newHashMap();

				while (resultSet.next()) {
					countMap.put(resultSet.getString(1), resultSet.getInt(2));
				}

				portrait.put(column, countMap);
			}

		} catch (Exception e) {
			logger.info("getPortraitFromImpala fail:" + e);
		} finally {
			try {
				if (pstsm != null) {
					pstsm.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		logger.info("getPortraitFromImpala portrait:=============" + portrait);
	}


	// transform the interest from file into Map
	private static void addInterestPortrait(Map<String, Map<String, Object>> portraitCount) throws Exception {

		Map<String, Object> interest = Maps.newHashMap();
		logger.info("interestPath:=============" + (interestPath + batchNo));

		String encoding = "UTF-8";
		File file = new File(interestPath + batchNo);

		// 判断文件是否存在
		if (file.exists() && file.isFile()) {

			InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);// 考虑到编码格式
			BufferedReader bufferedReader = new BufferedReader(read);
			String lineTxt = null;

			while ((lineTxt = bufferedReader.readLine()) != null) {

				String[] result = lineTxt.split(":");

				if (result[0].contains("-")) {
					String firstTag = result[0].split("-")[0];
					Map<String, Object> secondTag = Maps.newConcurrentMap();
					if (portraitCount.containsKey(firstTag)) {
						secondTag = (Map<String, Object>) portraitCount.get(firstTag);
					}

					secondTag.put(result[0].split("-")[1], Integer.valueOf(result[1]));
					portraitCount.put(firstTag, secondTag);
				} else {
					interest.put(result[0], Integer.valueOf(result[1]));
				}
			}

			bufferedReader.close();
			read.close();
		} else {
			logger.info("interestPath + batchNo 不存在");
			throw new IOException("interestPath + batchNo 不存在");
		}

		portraitCount.put("interest_tag", interest);

		// 删除兴趣批次文件
		if (file.exists() && file.isFile()) {
			file.delete();
		}
	}
}
