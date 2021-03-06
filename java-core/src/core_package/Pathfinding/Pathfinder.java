package core_package.Pathfinding;

import core_package.*;
import core_package.Schema.Relationship;
import core_package.Schema.Table;

import java.util.ArrayList;

public class Pathfinder implements Runnable {

    private final ArrayList<Path> toReturn;
    private final int max_length;
    private final Table targetTable;
    private final PathfinderController controller;

    private Thread pf;
    private Path partial;

    public Pathfinder(PathfinderController controller, ArrayList<Path> toReturn, String name, Path partial) throws InterruptedException {
        synchronized (toReturn) {
            this.toReturn = toReturn;
        }

        this.controller = controller;
        this.max_length = controller.getMaxLength();
        this.targetTable=controller.getTargetTable();
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
        //Main.printVerbose(pf.getName() + " partial: " + partial.toString()+" length "+partial.getLength()); //print path at run

        if (partial.getLength() == max_length) { //if path is at max_length, add to toReturn and end recursive loop
            synchronized (toReturn) {
                toReturn.add(partial);
            }
            return;
        }

        if(partial.getLength()>1 &&
                partial.getLastRelationship().getCardinality()==Relationship.ONE_TO_MANY &&
                partial.getSecondToLastRelationship() == partial.getLastRelationship()) {
            partial.removeLastRelationship();
            synchronized (toReturn) {
                toReturn.add(partial);
            }
            return;
        }

        if(partial.getLength()>1) {
            createNewPaths(partial.getLastLink().getLastTable());
        } else {
            Table nextRelTable = null;

            if(partial.getLastRelationship().getTables()[0]!=targetTable) {
                nextRelTable = partial.getLastRelationship().getTables()[0];
            } else if(partial.getLastRelationship().getTables()[1]!=targetTable) {
                nextRelTable = partial.getLastRelationship().getTables()[1];
            }

            createNewPaths(nextRelTable);
        }


    }

    public void createNewPaths(Table nextRelTable) {
        //iterates through relationships of next table in relationship and recursively calls createPaths to generate all partials

       for (Relationship rel : nextRelTable.getRelationships()) {
           //if (rel != partial.getLastRelationship()) { //add all relationships that are not the previous one used in partial

           Path p2 = new Path(this.partial);

            Table nextLink = null;

            if(rel.getTables()[0]!=nextRelTable)
                nextLink = rel.getTables()[0];
            else if(rel.getTables()[1]!=nextRelTable)
                nextLink = rel.getTables()[1];

           p2.addRelationship(rel, nextLink);

           //Main.printVerbose(pf.getName()+" submitting "+p2.toString());

           try {
               controller.enqueue(new Pathfinder(this.controller, this.toReturn, "Thread "+controller.countThreads(), p2));
           } catch (InterruptedException e) {
               e.printStackTrace();
           }
       //}
       }

    }

    public String getName() {
        return pf.getName();
    }


}