package thinkbig.pig.udf;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

/**
 * This class takes a bag and an index, and returns the tuple at the index in the bag.
 * If the index is a negative number, it selects the element at that index from the end.
 * E.g. -1 would be the last element, -2 the second-to-last, etc.
 * 
 * Example:
 * a = LOAD 'data' AS (mybag:{T:(str:chararray, n:int)});
 * first = FOREACH a GENERATE IndexInBag(mybag, 0);
 * last = FOREACH a GENERATE IndexInBag(mybag, -1);
 * 
 * If the index is out of bounds (index >= size or index < -size), will return null.
 * 
 * 
 * @author Joe Kelley
 *
 */
public class IndexInBag extends EvalFunc<Tuple> {

	@Override
	public Tuple exec(Tuple tuple) throws IOException {
		if (tuple == null || tuple.size() != 2) {
			throw new IOException("Illegal arguments: expecting bag, int");
		}
		if (tuple.get(0) instanceof DataBag && tuple.get(1) instanceof Integer) {
			DataBag bag = (DataBag) tuple.get(0);
			int index = (Integer) tuple.get(1);
			if (index >= 0) {
				return getAtIndex(bag, index);
			} else {
				return getFromEnd(bag, index);
			}
		} else {
			throw new IOException("Illegal arguments: expecting bag, int");
		}
	}

	private Tuple getFromEnd(DataBag bag, int index) throws IOException {
		Iterator<Tuple> iter = bag.iterator();
		int size = 0;
		while (iter.hasNext()) {
			iter.next();
			size++;
		}
		int absoluteLocation = size + index;
		if (absoluteLocation < 0) return null;
		return getAtIndex(bag, absoluteLocation);
	}

	private Tuple getAtIndex(DataBag bag, int index) throws IOException {
		Iterator<Tuple> iter = bag.iterator();
		for (int i = 0; i < index; i++) {
			if (!iter.hasNext()) return null;
			iter.next();
		}
		if (!iter.hasNext()) return null;
		return iter.next();
	}

}
