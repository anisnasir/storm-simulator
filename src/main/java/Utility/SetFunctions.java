package Utility;

import java.util.HashSet;

public class SetFunctions {
	public static int intersection (HashSet<String> set1, HashSet<String> set2) {
		HashSet<String> a;
		HashSet<String> b;
		int counter = 0;
		if(set1 == null || set2 == null)
			return 0;
		if (set1.size() <= set2.size()) {
			a = set1;
			b = set2; 
		} else {
			a = set2;
			b = set1;
		}
		for (String e : a) {
			if (b.contains(e)) {
				counter++;
			} 
		}
		return counter;
	}
}
