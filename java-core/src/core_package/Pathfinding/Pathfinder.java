package core_package.Pathfinding;

import core_package.*;
import core_package.Schema.Relationship;
import core_package.Schema.Table;

import java.util.ArrayList;

public class Pathfinder implements Runnable {

    private final ArrayList<Path> toReturn;
    private final int max_length;
    private final PathfinderController controller;

    private Thread pf;
    private Path partial;

    public Pathfinder(PathfinderController controller, ArrayList<Path> toReturn, String name, Path partial) throws InterruptedException {
        synchronized (toReturn) {
            this.toReturn = toReturn;
        }

        this.controller = controller;
        this.max_length = controller.getMaxLength();
        this.partial=partial;

        pf = new Thread(this, name);
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
        Main.printVerbose(pf.getName() + " partial: " + partial.toString()+" length "+partial.getLength()); //check path at run

        if (partial.getLength() == max_length) { //if path is at max_length, add to toReturn and end recursive loop
            synchronized (toReturn) {
                toReturn.add(partial);
            }
            return;
        }


        Table nextRelTable = null;

        if (partial.getRelationships().size() > 1) { //if more than 1 relationship in path
            Relationship secondToLast = partial.getSecondToLastRelationship();
            Relationship last = partial.getLastRelationship();

            /*
            The block of code below identifies which table to build off of for the next relationship in partial.
            */

            if (last.getTables()[0] != secondToLast.getTables()[0] && last.getTables()[0] != secondToLast.getTables()[1]) { //check if table 1 was used in the previous relationship link
                nextRelTable = last.getTables()[0];
            } else if (last.getTables()[1] != secondToLast.getTables()[0] && last.getTables()[1] != secondToLast.getTables()[1]) { //check if table 2 was used in the previous relationship link
                nextRelTable = last.getTables()[1];
            }

            if (nextRelTable.getRelationships().size() > 1) {
                createNewPaths(nextRelTable);
            } else { //if previous relationship cannot be built off of.
                synchronized (toReturn) {
                    toReturn.add(partial);
                }
                return;
            }

        } else if (partial.getLength() == 1) { //if only one relationship in partial, build paths off of each table.

            for(Table t : partial.getLastRelationship().getTables()) {
                if(t!=controller.getTargetTable())
                    if(t.getRelationships().size()>1)
                        createNewPaths(t);
                    else
                        synchronized (toReturn) {
                            toReturn.add(partial);
                        }
            }
        }
    }

    public void createNewPaths(Table nextRelTable) {
        //iterates through relationships of next table in relationship and recursively calls createPaths to generate all partials

       for (Relationship rel : nextRelTable.getRelationships()) {
           if (rel != partial.getLastRelationship()) { //add all relationships that are not the previous one used in partial
               Path p2 = new Path(this.partial);
               p2.addRelationship(rel);
               Main.printVerbose(pf.getName()+" submitting "+p2.toString());
               try {
                   controller.enqueue(new Pathfinder(this.controller, this.toReturn, "Thread "+controller.countThreads(), p2));
               } catch (InterruptedException e) {
                   e.printStackTrace();
               }
           }
       }

    }

    public String getName() {
        return pf.getName();
    }


}