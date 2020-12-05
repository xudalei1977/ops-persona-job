package com.leiyu.ops.etl.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbUtil {
	private static final Logger logger = LoggerFactory.getLogger(DbUtil.class);

	public static int execute(String sql) {
		logger.info("update sql: " + sql);
		Connection conn = JDBCUtils.getConnection();
		int i = 0;
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement(sql);
			i = pstmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("update error:", e);
		} finally {
			try {
				if (conn != null) {
					conn.close();
				}
				if (pstmt != null) {
					pstmt.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error("update conn close error:", e);
			}

		}
		return i;
	}

	public static Map<String, Object> queryOne(String sql, Map<Integer, String> params) {
		logger.info("query sql: " + sql);
		Map<String, Object> map = new TreeMap<String, Object>();
		Connection conn = JDBCUtils.getConnection();
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement(sql);
			for (int i = 1; i <= params.size(); i++) {
				pstmt.setString(i, params.get(i));
			}
			rs = pstmt.executeQuery();
			ResultSetMetaData md = rs.getMetaData();
			int columnCount = md.getColumnCount();
			while (rs.next()) {
	            for (int i = 1; i <= columnCount; i++) {
	                map.put(md.getColumnName(i), rs.getObject(i));
	            }
	        }
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("query error:", e);
		} finally {
			try {
				if (conn != null) {
					conn.close();
				}
				if (pstmt != null) {
					pstmt.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error("query conn close error:", e);
			}

		}
		return map;
	}
	
	public static List<Map<String, Object>> queryList(String sql, Map<Integer, String> params) {
		logger.info("query list sql: " + sql);
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Connection conn = JDBCUtils.getConnection();
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement(sql);
			for (int i = 1; i <= params.size(); i++) {
				pstmt.setString(i, params.get(i));
			}
			rs = pstmt.executeQuery();
			ResultSetMetaData md = rs.getMetaData();
			int columnCount = md.getColumnCount();
			while (rs.next()) {
				Map<String, Object> rowData = new HashMap<String, Object>();
	            for (int i = 1; i <= columnCount; i++) {
	                rowData.put(md.getColumnName(i), rs.getObject(i));
	            }
	            list.add(rowData);
	        }
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("query list error:", e);
		} finally {
			try {
				if (conn != null) {
					conn.close();
				}
				if (pstmt != null) {
					pstmt.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error("query list conn close error:", e);
			}

		}
		return list;
	}
}
