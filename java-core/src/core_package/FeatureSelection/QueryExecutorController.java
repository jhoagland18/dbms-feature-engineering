package core_package.FeatureSelection;

//import sun.plugin.com.ParameterListCorrelator;

import core_package.Exception.NoSuchDatabaseTypeException;
import core_package.Schema.Attribute;
import core_package.Schema.Table;
import core_package.SchemaBuilder.DatabaseConnection;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class QueryExecutorController {

    private ExecutorService executor;

    private ArrayList<Future> futures = new ArrayList<java.util.concurrent.Future>();
    private ArrayList<QueryExecutor> queries = new ArrayList<QueryExecutor>();

    private ArrayList<DatabaseConnection> connections = new ArrayList<DatabaseConnection>();

    private HashMap<String,Double> target = new HashMap();
    private String dbConnectionType;

    public QueryExecutorController(int numThreads, String targetTablePK, String targetColName, String dbConnectionType) {
        this.dbConnectionType = dbConnectionType;
        try {
            DatabaseConnection conn = DatabaseConnection.getConnectionForDBType(dbConnectionType);
            ResultSet rs = conn.query("SELECT ["+targetTablePK + "], [" + targetColName +
            "] FROM "+"Purchases");

            while(rs.next()) {
                target.put(rs.getString(1),Double.parseDouble(rs.getString(2)));
            }

            for(int i=0; i<numThreads; i++) {
                    connections.add(DatabaseConnection.getConnectionForDBType(dbConnectionType));

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        executor = Executors.newFixedThreadPool(numThreads);
    }

    public boolean addQuery(String query) {
        if(executor.isShutdown())
            return false;

        QueryExecutor qe = new QueryExecutor(query, target, this);

        queries.add(qe);
        futures.add(executor.submit(qe));

        return true;
    }

    public void shutdownExecutor() throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }

    public DatabaseConnection checkoutConnection() {
        synchronized (connections) {
            System.out.println("Checking out connection. Remaining: "+connections.size());
            if(connections.size()!=0) {
                return connections.remove(0);
            } else {
                try {
                    return DatabaseConnection.getConnectionForDBType(dbConnectionType);
                } catch (NoSuchDatabaseTypeException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }

    public void returnConnection(DatabaseConnection conn) {
        synchronized (connections) {
            System.out.println("Returning connection "+connections.size());
            connections.add(conn);
        }
    }

    public void addNewPotentialFeature(String sql, HashMap<String, Double> values, double correlation) {

    }


}
