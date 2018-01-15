package core_package.FeatureSelection;

//import sun.plugin.com.ParameterListCorrelator;

import core_package.Environment;
import core_package.Exception.NoSuchDatabaseTypeException;
import core_package.QueryGeneration.Query;
import core_package.Schema.Attribute;
import core_package.Schema.Table;
import core_package.SchemaBuilder.DatabaseConnection;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class QueryExecutorController {

    private ExecutorService executor;

    private ArrayList<Future> futures = new ArrayList<java.util.concurrent.Future>();
    private ArrayList<QueryExecutor> executors = new ArrayList<QueryExecutor>();

    private ArrayList<DatabaseConnection> connections = new ArrayList<DatabaseConnection>();

    private ArrayList<Query> savedQueries = new ArrayList<>();

    private HashMap<String,Double> target = new HashMap();
    private String dbConnectionType;



    public QueryExecutorController(int numThreads, String targetTablePK, String targetColName, String dbConnectionType, ArrayList<Query> queries) {
        this.dbConnectionType = dbConnectionType;
        try {
            DatabaseConnection conn = DatabaseConnection.getConnectionForDBType(dbConnectionType);
            ResultSet rs = conn.query("SELECT ["+targetTablePK + "], [" + targetColName +
            "] FROM "+"Purchases");

            Double value = 0.0;
            while(rs.next()) {
                 value = rs.getDouble(2);
                if(!rs.wasNull()) {
                    target.put(rs.getString(1),value);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        executor = Executors.newFixedThreadPool(numThreads);

        System.out.println("Total number of features to compare: "+queries.size());
        for(int i=0; i<numThreads; i++) {

            int listStart = i*(queries.size()/numThreads);

            int listEnd = 0;
            if(i==numThreads-1) {
            listEnd = queries.size();
            } else {
                listEnd = (i + 1) * (queries.size() / numThreads);
            }

            List<Query> partition = queries.subList(listStart,listEnd);
            System.out.println("Creating new QE: "+listStart+", "+listEnd);
            QueryExecutor qe = null;
            try {
                qe = new QueryExecutor(new ArrayList<Query>(partition), target, this);
            } catch (NoSuchDatabaseTypeException e) {
                e.printStackTrace();
            }

            futures.add(executor.submit(qe));
        }

        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("number of saved queries to filter through: "+savedQueries.size());

        for(Query q: savedQueries) {
            System.out.println(q.getSQL());
        }

        for (int i = 0; i < savedQueries.size(); i++) {
            Query q1 = savedQueries.get(i);

            for(int j=i+1; j<savedQueries.size(); j++) {
                Query q2 = savedQueries.get(j);

                double corrToFeature = QueryExecutor.getCorrelationToFeature(q1,q2);
                System.out.println("Corr to feature is "+corrToFeature);

                if(corrToFeature> Environment.maxFeatureCorrelation) {
                    if(q1.getCorrelationToDependent()>q2.getCorrelationToDependent()) {
                        savedQueries.remove(j);
                        j--;
                    } else {
                        savedQueries.remove(i);
                        i--;
                        break;
                    }
                }
            }

        }

        System.out.println("Final Queries: "+savedQueries.size());

        for(Query q: savedQueries) {

            System.out.println("\nCorr: "+q.getCorrelationToDependent()+"\nSQL:\n"+q.getSQL());
        }

    }

    public void shutdownExecutor() throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }

    public DatabaseConnection checkoutConnection() {
        synchronized (connections) {
//            System.out.println("Checking out connection. Remaining: "+connections.size());
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


    public synchronized void addNewPotentialFeatures(ArrayList<Query> qs) {
        savedQueries.addAll(qs);
    }


}
