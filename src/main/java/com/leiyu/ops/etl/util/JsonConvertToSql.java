package com.leiyu.ops.etl.util;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;

public class JsonConvertToSql {
	public static void main(String[] args) {
		String jsonStr = "{\"Activity\":[{\"end_date\":\"2019-05-23\",\"filters\":[{\"column_name\":\"count\",\"compare_operator\":\">\",\"value\":\"1\"},{\"column_name\":\"is_imp\",\"compare_operator\":\"=\",\"value\":\"1\"}],\"table_name\":\"persona.activity_nba\",\"column_join\":\"\",\"start_date\":\"2019-05-14\"},{\"column_join\":\"And\",\"end_date\":\"2019-05-15\",\"filters\":[{\"column_name\":\"count\",\"compare_operator\":\"<\",\"value\":\"4\"},{\"column_name\":\"country\",\"compare_operator\":\"=\",\"value\":\"1\"}],\"table_name\":\"persona.activity_kanglebao\",\"start_date\":\"2019-05-14\"}],\"Property\":{\"1\":{\"column_join\":\"\",\"column_name\":\"adx_active\",\"compare_operator\":\"=\",\"table_name\":\"persona.user_info\",\"value\":\"1\"},\"2\":{\"column_join\":\"And\",\"filters\":[{\"column_join\":\"\",\"column_name\":\"age\",\"compare_operator\":\"=\",\"table_name\":\"persona.user_info\",\"value\":\"11\"},{\"column_join\":\"And\",\"column_name\":\"interest_tag\",\"compare_operator\":\"=\",\"table_name\":\"persona.user_info\",\"value\":\"健康医疗\"}]},\"3\":{\"column_join\":\"And\",\"column_name\":\"age_range\",\"compare_operator\":\"=\",\"table_name\":\"persona.user_info\",\"value\":\"19~24\"}},\"relation\":\"And\"}";
		System.out.println(jsonToSql(jsonStr));
	}

	public static String jsonToSql(String jsonStr) {
		Map<String, Object> basic_tag = JSON.parseObject(jsonStr, Map.class);
		String relation = basic_tag.get("relation").toString().toLowerCase();
		StringBuilder propertySql = new StringBuilder();
		if (basic_tag.containsKey("Property")
				&& !JSON.parseObject(basic_tag.get("Property").toString(), Map.class).isEmpty()) {
			Map<String, Map<String, Object>> property = JSON.parseObject(basic_tag.get("Property").toString(),
					Map.class);
			property = sortByKeyAesc(property);
			propertySql.append("select p.lava_id from persona.user_info_impala p where ");
			for (Map.Entry<String, Map<String, Object>> entry : property.entrySet()) {
				Map<String, Object> tableValue = entry.getValue();
				String column_join = tableValue.get("column_join").toString().toLowerCase();
				if (column_join.equals("and")) {
					propertySql.append("and ");
				}
				if (column_join.equals("or")) {
					propertySql.append("or ");
				}
				if (tableValue.containsKey("filters")) {
					propertySql.append("( ");
					List<Map<String, String>> filters = JSON.parseObject(tableValue.get("filters").toString(),
							List.class);
					for (Map<String, String> filter : filters) {
						column_join = filter.get("column_join").toString().toLowerCase();
						if (column_join.equals("and")) {
							propertySql.append("and ");
						}
						if (column_join.equals("or")) {
							propertySql.append("or ");
						}
						String column_name = filter.get("column_name").toString();
						String compare_operator = filter.get("compare_operator").toString();
						String value = filter.get("value").toString();
						if ("interest_tag".equals(column_name)) {
							propertySql.append("p.interest_tag rlike '" + value + "' ");
						} else {
							propertySql.append("p." + column_name + " " + compare_operator + " '" + value + "' ");
						}
					}
					propertySql.append(") ");
				} else {
					String column_name = tableValue.get("column_name").toString();
					String compare_operator = tableValue.get("compare_operator").toString();
					String value = tableValue.get("value").toString();
					if ("interest_tag".equals(column_name)) {
						propertySql.append("p.interest_tag rlike '" + value + "' ");
					} else {
						propertySql.append("p." + column_name + " " + compare_operator + " '" + value + "' ");
					}
				}
			}
		}
		StringBuilder acSql = new StringBuilder();
		if (basic_tag.containsKey("Activity")
				&& !JSON.parseObject(basic_tag.get("Activity").toString(), List.class).isEmpty()) {
			List<Map<String, Object>> activity = JSON.parseObject(basic_tag.get("Activity").toString(), List.class);
			char tableAlias = 'a';
			Map<String, String> acMap = Maps.newHashMap();
			Map<String, String> opMap = Maps.newHashMap();
			for (Map<String, Object> aMap : activity) {
				StringBuilder activitySql = new StringBuilder();
				String column_join = aMap.get("column_join").toString().toLowerCase();
				String table_name = aMap.get("table_name").toString();
				String alias = table_name.split("[_]")[1];
				String start_date = aMap.get("start_date").toString();
				String end_date = aMap.get("end_date").toString();
				opMap.put(String.valueOf(tableAlias), column_join);
				if (!StringUtils.isBlank(start_date) && !StringUtils.isBlank(end_date)) {
					activitySql.append(alias + ".dt between '" + start_date + "' and '" + end_date + "' ");
				}

				if (!JSON.parseObject(aMap.get("filters").toString(), List.class).isEmpty()) {
					List<Map<String, String>> filters = JSON.parseObject(aMap.get("filters").toString(), List.class);
					boolean is_count = false;
					String countValue = "0";
					String countOperator = "";
					for (Map<String, String> filter : filters) {
						String column_name = filter.get("column_name").toString();
						String compare_operator = filter.get("compare_operator").toString();
						String value = filter.get("value").toString();
						if ("count".equals(column_name)) {
							is_count = true;
							countValue = value;
							countOperator = compare_operator;
						} else {
							activitySql.append(
									"and " + alias + "." + column_name + " " + compare_operator + " '" + value + "' ");
						}
					}
					if (is_count) {
						StringBuilder countSql = new StringBuilder("select " + tableAlias + ".lava_id from ( select "
								+ alias + ".lava_id , count(1) count from " + table_name + " " + alias
								+ " where 1=1 and ");
						activitySql = countSql.append(activitySql)
								.append("group by " + alias + ".lava_id having count ( " + alias + ".lava_id ) "
										+ countOperator + " " + countValue + " ) " + tableAlias + " ");
					} else {
						StringBuilder countSql = new StringBuilder(
								"select " + alias + ".lava_id from " + table_name + " " + alias + " where 1=1 and ");
						activitySql = countSql.append(activitySql);
					}
				}
				acMap.put(String.valueOf(tableAlias), activitySql.toString());
				tableAlias = (char) (tableAlias + 1);
			}
			if (!acMap.isEmpty()) {
				acMap = sortByKeyAesc(acMap);
				opMap = sortByKeyAesc(opMap);
			}
			int i = 1;
			for (Map.Entry<String, String> entry : acMap.entrySet()) {
				String key = entry.getKey();
				String asql = entry.getValue();
				String temp = "a" + i;
				String alias = "aa" + i;
				String lastAlias = "aa" + (i - 1);
				if (i == 1) {
					acSql.append(asql);
				} else {
					if (i == 2) {
						lastAlias = "a";
					}
					String operator = opMap.get(key);
					if ("and".equals(operator)) {
						acSql.append("inner join ( ").append(asql)
								.append(") " + temp + " on " + lastAlias + ".lava_id = " + temp + ".lava_id ");
					}
					if ("or".equals(operator)) {
						acSql.append("union select " + temp + ".lava_id from ( ").append(asql)
								.append(") " + temp + " ");
					}
					StringBuilder tempBuilder = new StringBuilder();
					tempBuilder.append("select " + alias + ".lava_id from ( ");
					acSql = tempBuilder.append(acSql);
					acSql.append(") " + alias + " ");
				}
				i++;
			}
		}
		if (!StringUtils.isBlank(propertySql) && !StringUtils.isBlank(acSql)) {
			if ("or".equals(relation)) {
				return propertySql.append("union ").append(acSql).toString();
			}
			if ("and".equals(relation)) {
				StringBuilder all = new StringBuilder();
				return all.append("select pro.lava_id from ( ").append(propertySql).append(") pro inner join ( ")
						.append(acSql).append(") ac on pro.lava_id = ac.lava_id ").toString();
			}
		}
		if (StringUtils.isBlank(propertySql) && StringUtils.isBlank(acSql)) {
//			return "select lava_id from persona.user_info where interest_tag != '' limit 1000000";
			return "select lava_id from persona.user_info";
		}
		return propertySql.append(acSql).toString();
	}

	public static <K extends Comparable<? super K>, V> Map<K, V> sortByKeyDesc(Map<K, V> map) {
		Map<K, V> result = new LinkedHashMap<>();

		map.entrySet().stream().sorted(Map.Entry.<K, V>comparingByKey().reversed())
				.forEachOrdered(e -> result.put(e.getKey(), e.getValue()));
		return result;
	}

	public static <K extends Comparable<? super K>, V> Map<K, V> sortByKeyAesc(Map<K, V> map) {
		Map<K, V> result = new LinkedHashMap<>();

		map.entrySet().stream().sorted(Map.Entry.<K, V>comparingByKey())
				.forEachOrdered(e -> result.put(e.getKey(), e.getValue()));
		return result;
	}
}
