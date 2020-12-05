package com.leiyu.ops.etl.portrait;

public class ImpalaHive2Config {
	private String JdbcUrl;
	private int weight;

	public ImpalaHive2Config(String JdbcUrl) {
		super();
		this.JdbcUrl = JdbcUrl;
	}

	public ImpalaHive2Config(String JdbcUrl, int weight) {
		this.JdbcUrl = JdbcUrl;
		this.weight = weight;
	}

	public String getJdbcUrl() {
		return JdbcUrl;
	}

	public void setJdbcUrl(String jdbcUrl) {
		JdbcUrl = jdbcUrl;
	}

	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

	@Override
	public String toString() {
		return "Hive2Config [JdbcUrl=" + JdbcUrl + ", weight=" + weight + "]";
	}
}
