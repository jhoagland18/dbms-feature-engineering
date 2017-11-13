package core_package;

import java.util.ArrayList;

public class Pathfinder implements Runnable {

    private final ArrayList<Path> toReturn;
    private final int max_length;
    private final PathfinderController controller;
    private final int index;

    private Thread pf;
    private Path startPartial;
    private boolean run=true;

    public Pathfinder(PathfinderController controller, ArrayList<Path> toReturn, String name, int index) throws InterruptedException {
        synchronized (toReturn) {
            this.toReturn = toReturn;
        }

        this.controller = controller;
        this.max_length = controller.getMaxLength();
        this.index=index;

        pf = new Thread(this, name);
        pf.start();
    }

    @Override
    public void run() {
        try {
            buildPartials();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void buildPartials() throws InterruptedException { //adds paths to list parameter up to max_length
        //Main.printVerbose(pf.getName()+" is starting new buildpartials");

        Main.printVerbose(pf.getName()+" Number of threads waiting: " + controller.getNumberOfThreadsWaiting()+" out of "+controller.getNumberOfThreadsActive());

        /*
        if(controller.isQueueEmpty()) {
                pf.yield();
                Main.printVerbose(pf.getName()+ " Queue empty");
            if(controller.isQueueEmpty()) {
                Main.printVerbose(pf.getName()+" Queue really empty");
                if(!controller.allThreadsWaiting(this)) {
                    synchronized (controller) {
                        Main.printVerbose(pf.getName()+" now waiting");
                        controller.wait();
                    }
                } else {
                    Main.printVerbose(pf.getName()+" All threads are waiting");
                    controller.endPathFinding();
                    synchronized (controller) {
                        controller.notifyAll();
                    }
                }
            } else {
                Main.printVerbose(pf.getName()+" Queue wasnt really empty.");
            }
        }
        */

        synchronized (controller) {
            if(controller.isQueueEmpty()) {
                Main.printVerbose(pf.getName()+" Queue empty");
                if(!controller.allThreadsWaiting(this)) {
                    synchronized (controller) {
                        Main.printVerbose(pf.getName()+" now waiting");
                        controller.setWaiting(this);
                        controller.wait();
                    }
                } else {
                    Main.printVerbose(pf.getName()+" All threads are waiting");
                    controller.endPathFinding();
                    synchronized (controller) {
                        controller.notifyAll();
                    }
                }
            }
            controller.setActive(this);
        }

        if(!run) {
            Main.printVerbose("terminating thread");
            return;
        }

        Main.printVerbose(pf.getName() + " starting on new path");

        Path partial = null;
        try {
            partial = controller.dequeue();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if(partial!=null) {

            Main.printVerbose(pf.getName() + " partial: " + partial.toString()); //check path at run

            if (partial.getRelationships().size() == max_length - 1) { //if path is at max_length, add to toReturn and end recursive loop
                synchronized (toReturn) {
                    toReturn.add(partial);
                }
                Main.printVerbose(pf.getName() + " recursively calling buildpartials");
                buildPartials();
                return;
            }

            if (partial.getRelationships().size() != 0) {
                if (partial.getLastRelationship().getTables()[0].getRelationships().size() < 2
                        && partial.getLastRelationship().getTables()[1].getRelationships().size() < 2) {
                    //if the two tables in the last added relationship only link to each other, return
                    synchronized (toReturn) {
                        toReturn.add(partial);
                    }
                    Main.printVerbose(pf.getName() + " recursively calling buildpartials");
                    buildPartials();
                    return;
                }
            }

            Table nextRelTable = null;

            //Main.printVerbose("Number of relationships in partial: " + partial.getRelationships().size());

            if (partial.getRelationships().size() > 1) { //if more 1 relationship in path
                Relationship secondToLast = partial.getSecondToLastRelationship();
                Relationship last = partial.getLastRelationship();

    /*
    The block of code below identifies which table to build off of for the next relationship in partial.
     */

                if (last.getTables()[0] != secondToLast.getTables()[0] && last.getTables()[0] != secondToLast.getTables()[1]) { //check if table 1 was used in the previous relationship link
                    nextRelTable = last.getTables()[0];
                    createNewPaths(nextRelTable, partial);
                } else if (last.getTables()[1] != secondToLast.getTables()[0] && last.getTables()[1] != secondToLast.getTables()[1]) { //check if table 2 was used in the previous relationship link
                    nextRelTable = last.getTables()[1];
                    createNewPaths(nextRelTable, partial);
                }
            } else if (partial.getRelationships().size() == 1) { //if only one relationship in partial, build paths off of each table.
                nextRelTable = partial.getLastRelationship().getTables()[0];
                createNewPaths(nextRelTable, partial);

                nextRelTable = partial.getLastRelationship().getTables()[1];
                createNewPaths(nextRelTable, partial);
            }
            Main.printVerbose(pf.getName() + " recursively calling buildpartials");
        } else {
            Main.printVerbose(pf.getName() + " path was null");
        }
    buildPartials();
    }

    public void createNewPaths(Table nextRelTable, Path partial) {
        //iterates through relationships of next table in relationship and recursively calls createPaths to generate all partials

        for (Relationship rel : nextRelTable.getRelationships()) {
            if (partial.getLength() > 0) { //if partial has more than one element
                if (rel != partial.getLastRelationship()) { //add all relationships that are not the previous one used in partial
                    Path p2 = new Path(partial);
                    p2.addRelationship(rel);
                    controller.enqueue(p2);
                    Main.printVerbose("notifying next thread");
                }
            } else { //if partial only has one element, add all relationships possible
                Path p2 = new Path(partial);
                p2.addRelationship(rel);
                controller.enqueue(p2);
                Main.printVerbose("notifying next thread");
            }
            synchronized (controller) {
                controller.notifyAll();
            }
        }
    }

    public Thread.State getState() {
        return pf.getState();
    }

    public void stopThread() {
        run=false;
    }

    public void join() throws InterruptedException {
        pf.join();
    }

    public String getName() {
        return pf.getName();
    }

    public int getIndex() {
        return index;
    }

}