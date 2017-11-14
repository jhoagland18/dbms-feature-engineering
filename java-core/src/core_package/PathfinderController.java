package core_package;

import com.sun.corba.se.impl.orbutil.closure.Future;

import java.sql.Time;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.*;

import static java.lang.Thread.sleep;

public class PathfinderController {

    final int numThreads;
    final int maxLength;

    private ExecutorService executor;
    private int activeThreads=0;

    private int totalThreads=0;

    private ArrayList<java.util.concurrent.Future> futures = new ArrayList<java.util.concurrent.Future>();

    public PathfinderController(int numThreads, int maxLength) {
        this.numThreads = numThreads;
        this.maxLength = maxLength;
        executor = Executors.newFixedThreadPool(numThreads);
    }

    public void createPaths(Table targetTable, ArrayList<Path> toReturn) {
        int threadCounter=0;
        for (Relationship rel : targetTable.getRelationships()) {
            Path p2 = new Path();
            p2.addRelationship(rel);
            try {
                Pathfinder pf = new Pathfinder(this, toReturn, "Thread "+countThreads(), p2);
                futures.add(executor.submit(pf));
                threadCounter++;
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

/*
    public synchronized void ifAllThreadsCompleteShutdown() {

        for(int i=0; i<futures.size(); i++) {
            if(futures.get(i).isDone()) {
                futures.remove(i);
                i--;
            }
        }

        Main.printVerbose("futures size "+futures.size() + " "+futures.toString());

        if(futures.size()==1) {
            Main.printVerbose("shutting down pathfinding");
            executor.shutdown();

            try {
                executor.awaitTermination(1,TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    */

    public synchronized int countThreads() {
        return totalThreads++;
    }
}