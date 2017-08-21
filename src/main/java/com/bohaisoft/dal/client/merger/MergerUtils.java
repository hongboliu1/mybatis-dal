package com.bohaisoft.dal.client.merger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class MergerUtils {
	@SuppressWarnings("rawtypes")
	public static Collection select(Collection inputCollection, IMergerFilter filter) {
		List answer = new ArrayList(inputCollection.size());
		select(inputCollection, filter, answer);
		return answer;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void select(Collection inputCollection, IMergerFilter filter, Collection outputCollection) {
		if (inputCollection != null && filter != null) {
			for (Iterator iter = inputCollection.iterator(); iter.hasNext();) {
				Object item = iter.next();
				if (filter.evaluate(item)) {
					outputCollection.add(item);
				}
			}
		}
	}
}
