package com.leiyu.ops.etl.util;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

/**
 * jdbc CDH test!
 *
 */
public class CDHUtil {
	public static Logger logger = Logger.getLogger(CDHUtil.class);
	private static Connection conn;
	private static PreparedStatement pstsm;

	public static Map<String, Map<String, Object>> getPortraitFromImpala(String crowdCountSql,
			Map<String, String> portraitSql) throws Exception {
		PropertiesUtil p = new PropertiesUtil("dmp-core.properties");
		Map<String, Map<String, Object>> map = Maps.newHashMap();
		int totalCount = getCountFromImpala(crowdCountSql);
		if (totalCount == 0) {
			return map;
		}
		Class.forName(p.readProperty("impaladriverClassName")).newInstance();
		// 登录Kerberos账号
		System.setProperty("java.security.krb5.conf", p.readProperty("krb.conf.path"));
		Configuration configuration = new Configuration();
		configuration.set("hadoop.security.authentication", "Kerberos");
		UserGroupInformation.setConfiguration(configuration);
		UserGroupInformation.loginUserFromKeytab(p.readProperty("krb.authen.user"),
				p.readProperty("krb.authen.keytab"));
		conn = DriverManager.getConnection(p.readProperty("impalaurl"));
		logger.info("getPortraitFromImpala columnsList= " + portraitSql);
		for (Entry<String, String> entry : portraitSql.entrySet()) {
			String column = entry.getKey().split("\\.")[2];
			logger.info("Portraitcolumn:=============" + column);
			logger.info("PortraitSql:=============" + entry.getValue());
			pstsm = conn.prepareStatement(entry.getValue());
			ResultSet resultSet = pstsm.executeQuery();
			Map<String, Object> portraitMap = Maps.newHashMap();
			while (resultSet.next()) {
				double scaleDouble = 0;
				double scale = 0.00000;
				int scaleInt = 0;
				if (totalCount > 0) {
					scale = (double) resultSet.getInt(2) * 100 / totalCount;
					scaleDouble = new BigDecimal(scale).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
					BigDecimal bd = new BigDecimal(scaleDouble).setScale(0, BigDecimal.ROUND_HALF_UP);
					scaleInt = Integer.parseInt(bd.toString());
				}
				portraitMap.put(resultSet.getString(1), scaleInt);
			}
			map.put(column, portraitMap);
		}
		if (conn != null) {
			conn.close();
		}
		logger.info("getPortraitFromImpala PortraitMap:=============" + map);
		return map;
	}

	public static int getCountFromImpala(String sql) throws Exception {
		PropertiesUtil p = new PropertiesUtil("dmp-core.properties");
		logger.info("CrowdCountSql:=============" + sql);
		int count = 0;
		Class.forName(p.readProperty("impaladriverClassName")).newInstance();
		// 登录Kerberos账号
		System.setProperty("java.security.krb5.conf", p.readProperty("krb.conf.path"));
		Configuration configuration = new Configuration();
		configuration.set("hadoop.security.authentication", "Kerberos");
		UserGroupInformation.setConfiguration(configuration);
		UserGroupInformation.loginUserFromKeytab(p.readProperty("krb.authen.user"),
				p.readProperty("krb.authen.keytab"));
		conn = DriverManager.getConnection(p.readProperty("impalaurl"));
		logger.info("getCountFromImpala sql : " + sql);
		pstsm = conn.prepareStatement(sql);
		ResultSet resultSet = pstsm.executeQuery();
		while (resultSet.next()) {
			count = resultSet.getInt(1);
		}
		if (conn != null) {
			conn.close();
		}
		logger.info("COUNT:=============" + count);
		return count;
	}

	/**
	 * 获取Hdfs 指定目录下所有文件
	 * 
	 * @param uri
	 *            hdfs远端连接url
	 * @param remotePath
	 *            hdfs远端目录路径
	 * @param conf
	 * @throws Exception
	 */
	public static void getHdfsFileList(Configuration conf, String uri, String remotePath) throws Exception {

		FileSystem fs = FileSystem.get(new URI(uri), conf);
		RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path(remotePath), true);
		while (iter.hasNext()) {
			LocatedFileStatus status = iter.next();
			System.out.println(status.getPath().toUri().getPath());
		}
		fs.close();
	}

	/**
	 * 往hdfs上传文件
	 */
	public static void addFileToHdfs(String localPath, String hdfsPath, String fileName) throws Exception {
		PropertiesUtil rb = new PropertiesUtil("dmp-core.properties");
		String user = rb.readResourceProperty("krb.authen.user");
		String keytab = rb.readResourceProperty("krb.authen.keytab");
		String krb5File = rb.readResourceProperty("krb.conf.path");
		System.setProperty("java.security.krb5.conf", krb5File);
		Configuration conf = new Configuration();
		conf.set("hadoop.security.authentication", "kerberos");
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromKeytab(user, keytab);
		String uri = rb.readResourceProperty("hdfs.uri");
		FileSystem fileSystem = FileSystem.get(new URI(uri), conf);

		Path src = new Path(localPath);
		boolean isExists = fileSystem.exists(new Path(hdfsPath));
		if (!isExists) {
			fileSystem.mkdirs(new Path(hdfsPath));
		}
		Path dst = new Path(hdfsPath + "/" + fileName);
		fileSystem.copyFromLocalFile(src, dst);
		fileSystem.close();
	}

	/**
	 * 往hdfs上传文件
	 */
	public static List<String> readHdfs(String dir) throws Exception {
		PropertiesUtil rb = new PropertiesUtil("dmp-core.properties");
		String user = rb.readResourceProperty("krb.authen.user");
		String keytab = rb.readResourceProperty("krb.authen.keytab");
		String krb5File = rb.readResourceProperty("krb.conf.path");
		System.setProperty("java.security.krb5.conf", krb5File);
		Configuration conf = new Configuration();
		conf.set("hadoop.security.authentication", "kerberos");
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromKeytab(user, keytab);
		String uri = rb.readResourceProperty("hdfs.uri");
		FileSystem fileSystem = FileSystem.get(new URI(uri), conf);
		if (StringUtils.isBlank(dir)) {
			return null;
		}
		if (!dir.startsWith("/")) {
			dir = "/" + dir;
		}
		List<String> result = new ArrayList<String>();
		FileStatus[] stats = null;
		try {
			stats = fileSystem.listStatus(new Path(dir));
			for (FileStatus file : stats) {
				if (file.isFile() && file.getLen() != 0) {
					result.add(file.getPath().toUri().getPath());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		fileSystem.close();
		return result;

	}

	/**
	 * 往hdfs上传文件
	 */
	public static void addFileToHdfs2(String localPath, String fileName, String batchNo) throws Exception {
		PropertiesUtil rb = new PropertiesUtil("dmp-core.properties");
		String user = rb.readResourceProperty("krb.authen.user");
		String keytab = rb.readResourceProperty("krb.authen.keytab");
		String krb5File = rb.readResourceProperty("krb.conf.path");
		System.setProperty("java.security.krb5.conf", krb5File);
		Configuration conf = new Configuration();
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
		conf.set("hadoop.rpc.protection", "authentication");
		conf.set("dfs.namenode.kerberos.principal", "hdfs/_HOST@LAVAPM.COM");
		URI uri = new URI(rb.readResourceProperty("hdfs.uri"));
		// 添加hdfs-site.xml到conf
		conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		// 添加core-site.xml到conf
		conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromKeytab(user, keytab);
		FileSystem fileSystem = FileSystem.get(uri, conf);

		Path src = new Path(localPath);
		String hdfsUploadPath = rb.readResourceProperty("file.upload.hdfs.path") + batchNo;
		boolean isExists = fileSystem.exists(new Path(hdfsUploadPath));
		if (!isExists) {
			fileSystem.mkdirs(new Path(hdfsUploadPath));
		}
		Path dst = new Path(hdfsUploadPath + "/" + fileName);
		fileSystem.copyFromLocalFile(src, dst);
		fileSystem.close();
	}
}
