package core_package;

import java.util.ArrayList;
import java.util.concurrent.*;


public class PathfinderController {

    final int numThreads;
    final int maxLength;

    private ExecutorService executor;

    private int totalThreads=0;

    private ArrayList<java.util.concurrent.Future> futures = new ArrayList<java.util.concurrent.Future>();

    public PathfinderController(int numThreads, int maxLength) {
        this.numThreads = numThreads;
        Main.printVerbose("creating controller with maxlength of "+maxLength);
        this.maxLength = maxLength;
        executor = Executors.newFixedThreadPool(numThreads);
    }

    public void createPaths(Table targetTable, ArrayList<Path> toReturn) {
        for (Relationship rel : targetTable.getRelationships()) {
            Path p2 = new Path();
            p2.addRelationship(rel);
            try {
                Pathfinder pf = new Pathfinder(this, toReturn, "Thread "+countThreads(), p2);
                futures.add(executor.submit(pf));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Main.printVerbose("Spawning new pathfinding thread");
        }

        synchronized (futures) {

            for (int i = 0; i < futures.size(); i++) {
                try {
                    futures.get(i).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }

        executor.shutdown();

        try {
            executor.awaitTermination(1,TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public synchronized void enqueue(Pathfinder pf) {
        futures.add(executor.submit(pf));
    }

    public synchronized int getMaxLength() {
        return maxLength;
    }

    public synchronized int countThreads() {
        return totalThreads++;
    }
}