package core_package.Pathfinding;

import java.util.ArrayList;

import core_package.Schema.*;

public class PathGenerator {
	
	public static ArrayList<Path> GeneratePaths(int maxDepth, Table target) {
		ArrayList<Path> finalResult = new ArrayList<>();
		for (int d = 2;d <= maxDepth;d++) {
				Path path = new Path(target);
				ArrayList<Path> result = new ArrayList<>();
				GeneratePaths(d, path,result);
				finalResult.addAll(result);
		}
		return finalResult;
	}
	
	public static void GeneratePaths(int depth, Path curPath, ArrayList<Path> curResult) {
		if (curPath.getLength() == depth) {
			curResult.add(curPath);
		}
		else {
			for (Relationship r_out : curPath.getHead().getRelationships()) {
				// avoid a->toN->b->to1->a
				if (curPath.getLength() >= 2 && 
						curPath.getTables().get(curPath.getTables().size()-2) == r_out.getTable2() &&
						r_out.getRelationshipType() == RelationshipType.To1 &&
						curPath.getRelationships().get(curPath.getRelationships().size() - 1).getRelationshipType() == RelationshipType.ToN)
					continue;
				Path newPath = curPath.Copy();
				newPath.addRelationship(r_out);
				// return all ways to finish the new path
				ArrayList<Path> subsetOfPaths = new ArrayList<>();
				GeneratePaths(depth, newPath, subsetOfPaths);
				curResult.addAll(subsetOfPaths);
			}
		}			
	}
}
