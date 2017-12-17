package core_package.QueryTemplates;

public class Query {
	private String SQL;
	public String getSQL() {
		return SQL;
	}
	
	private Double complexity;
	public Double getComplexity() {
		return complexity;
	}
	
	public Query(String SQL, Double complexity) {
		this.SQL = SQL;
		this.complexity = complexity;
	}
}
