package core_package.FeatureSelection;

import core_package.SchemaBuilder.DatabaseConnection;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

public class QueryExecutor implements Runnable {

    private final String query;
    private double correlationToDependant;
    private QueryExecutorController queryExecutorController;
    private HashMap<String,Double> target;

    private DatabaseConnection conn;

    public QueryExecutor(String query, HashMap<String, Double> target, QueryExecutorController qec) {
        this.queryExecutorController = qec;
        conn = queryExecutorController.checkoutConnection();
        query = query.substring(query.indexOf("SELECT"));
        this.query = query.substring(0,query.lastIndexOf('\''));
    }

    @Override
    public void run() {
        ArrayList<Double> predictor = new ArrayList<Double>();
        ArrayList<Double> dependant = new ArrayList<Double>();
        ResultSet rs;

        try {
             rs = conn.query(query);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        try {
            String key = "";
            while(rs.next()) {
                key = rs.getString(1);
                if(target.containsKey(key)) {
                    predictor.add(Double.parseDouble(rs.getString(2)));
                    dependant.add(target.get(key));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        double[] dpredictor = new double[predictor.size()];
        for (int i = dpredictor.length-1; i >= 0; i--) {
            dpredictor[i] = predictor.remove(i);                // java 1.5+ style (outboxing)
        }

        double[] ddependant = new double[dependant.size()];
        for (int i = ddependant.length-1; i >= 0; i--) {
            ddependant[i] = dependant.remove(i);                // java 1.5+ style (outboxing)
        }


        PearsonsCorrelation pc = new PearsonsCorrelation();
        double corr = pc.correlation(dpredictor,ddependant);
        System.out.println("Corr is "+corr);
        if(corr > .2) {

        }


        queryExecutorController.returnConnection(conn);
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
