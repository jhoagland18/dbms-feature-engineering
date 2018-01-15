package core_package.QueryGeneration;

import java.util.HashMap;

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

	private HashMap<String, Double> rows;
	private double corrToDependent;
	
	public Query(String SQL, Double complexity, String description) {
		this.SQL = SQL;
		this.complexity = complexity;
		this.Description = description;

	}

	public HashMap<String, Double> getRows() {
		return rows;
	}

	public void setRows(HashMap<String, Double> inputRows) {

		rows = new HashMap<String, Double>(inputRows);
	}

	public void setCorrelationToDependent(double corr) {
		corrToDependent = corr;
	}

	public double getCorrelationToDependent() {
		return corrToDependent;
	}
}
