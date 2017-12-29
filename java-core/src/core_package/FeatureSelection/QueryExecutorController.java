package core_package.FeatureSelection;

import core_package.Pathfinding.Pathfinder;
import sun.plugin.com.ParameterListCorrelator;

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

    private HashMap dependant;

    public QueryExecutorController(int numThreads, HashMap dependant) {
        this.dependant = dependant;

        executor = Executors.newFixedThreadPool(numThreads);
    }

    public boolean addQuery(String query) {
        if(executor.isShutdown())
            return false;

        QueryExecutor qe = new QueryExecutor(query, dependant);

        queries.add(qe);
        futures.add(executor.submit(qe));

        return true;
    }

    public void testForCorrelation() {

        try {
            shutdownExecutor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private void shutdownExecutor() throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }


}
