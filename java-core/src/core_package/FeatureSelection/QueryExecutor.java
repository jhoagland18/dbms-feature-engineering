package core_package.FeatureSelection;

import java.util.HashMap;

public class QueryExecutor implements Runnable {

    private final String query;
    private double correlationToDependant;

    public QueryExecutor(String query, HashMap dependant) {
        this.query = query;
    }

    @Override
    public void run() {
        correlationToDependant = executeQuery();

    }

    private double executeQuery() {
        //execute query

        return 0;
    }

    public String getQuery() {
        return query;
    }

    public double getCorrelation() {
        return correlationToDependant;
    }
}
