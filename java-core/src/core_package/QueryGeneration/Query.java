package core_package.QueryGeneration;

public class Query {
	private String Description;
	public String getDescription() {
		return Description;
	}
	
	private String SQL;
	public String getSQL() {
		return SQL;
	}
	
	private Double complexity;
	public Double getComplexity() {
		return complexity;
	}
	
	public Query(String SQL, Double complexity, String description) {
		this.SQL = SQL;
		this.complexity = complexity;
		this.Description = description;
	}
}
