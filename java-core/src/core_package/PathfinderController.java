package core_package;

import java.util.ArrayList;
import java.util.Queue;

public class PathfinderController {

    final int numThreads;
    final sun.misc.Queue<Path> partialQueue = new sun.misc.Queue();
    final int maxLength;
    private Pathfinder[] threads;
    private boolean[] threadsWaiting;

    public PathfinderController(int numThreads, int maxLength) {
        this.numThreads = numThreads;
        this.maxLength = maxLength;
        threads = new Pathfinder[numThreads];
        threadsWaiting = new boolean[numThreads];
    }

    public void createPaths(Table targetTable) {
        for (Relationship rel : targetTable.getRelationships()) {
            Path p2 = new Path();
            p2.addRelationship(rel);
            partialQueue.enqueue(p2);
            Main.printVerbose("Spawning new pathfinding thread");
        }
    }

    public void startThreads(ArrayList<Path> toReturn) {

        int numStartedThreads = 0;
        while (numStartedThreads < numThreads) {
            try {
                Pathfinder pf = new Pathfinder(this, toReturn, "thread"+numStartedThreads, numStartedThreads);
                threads[numStartedThreads] = pf;
                threadsWaiting[numStartedThreads] = false;
                numStartedThreads++;
                Main.printVerbose("Creating thread "+numStartedThreads +" of "+numThreads);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        for(int i=0; i<threads.length; i++) {
            if(threads[i]!=null) {
                try {
                    threads[i].join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public synchronized Path dequeue() throws InterruptedException {
        return partialQueue.dequeue();
    }

    public synchronized boolean enqueue(Path path) {
        partialQueue.enqueue(path);
        return true;
    }

    public synchronized int getMaxLength() {
        return maxLength;
    }

    public synchronized boolean isQueueEmpty() {
        return partialQueue.isEmpty();
    }

    public boolean allThreadsWaiting(Pathfinder pf) {

        if (numThreads == 1) {
            return true;
        }

        int count=0;

        Main.printVerbose("Current thread is "+pf.getName());

        for (int i = 0; i < threadsWaiting.length; i++) {
            if(threadsWaiting[i]==true)
                count++;

        }

        if(count>=numThreads-2)
            return true;
        else
            return false;
    }

    public void endPathFinding() {
        for (int i = 0; i < threads.length; i++) {
            if (threads[i] != null) {
                threads[i].stopThread();
            }
        }
    }

    public synchronized int getNumberOfThreadsWaiting() {
        int num = 0;
        for (int i = 0; i < threadsWaiting.length; i++) {
            if(threadsWaiting[i]==true)
                num++;
        }
        return num;
    }

    public synchronized int getNumberOfThreadsActive() {
        int num = 0;
        for (int i = 0; i < threads.length; i++) {
            if(threads[i]!=null) {
                num++;
            }
        }
        return num;
    }

    public void setWaiting(Pathfinder pf) {
        threadsWaiting[pf.getIndex()]=true;
    }

    public void setActive(Pathfinder pf) {
        threadsWaiting[pf.getIndex()]=false;
    }
}
