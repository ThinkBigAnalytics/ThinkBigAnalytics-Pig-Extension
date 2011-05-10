package thinkbig.pig.udf;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.*;
import org.junit.Test;

public class TupleToBagTest {
    TupleToBag ttb = new TupleToBag();

    @Test
    public void emptyTuple() throws IOException {        
        Tuple emptyTuple = makeTuple();
        assertEquals(BagFactory.getInstance().newDefaultBag(), ttb.exec(emptyTuple)); 
    }
    
    @Test
    public void simpleTuple() throws IOException {
        Tuple topLevelTuple = makeTuple("hello");
        
        DataBag topLevelBag = BagFactory.getInstance().newDefaultBag();
        topLevelBag.add(topLevelTuple);
        assertEquals(topLevelBag, ttb.exec(topLevelTuple));
    }

    @Test
    public void nestedTuple() throws IOException {
        Tuple topLevelTuple = makeTuple(makeTuple(123L, "str"), makeTuple("one"));        
        
        DataBag topLevelBag = BagFactory.getInstance().newDefaultBag();
        topLevelBag.add(makeTuple(123L));
        topLevelBag.add(makeTuple("str"));
        topLevelBag.add(makeTuple("one"));
        assertEquals(topLevelBag, ttb.exec(topLevelTuple));
    }
    
    static Tuple makeTuple(Object... obj) {
        Tuple tuple = TupleFactory.getInstance().newTuple(obj.length);
        for (int i=0; i<obj.length; i++) {
            try {
                tuple.set(i, obj[i]);
            }
            catch (ExecException e) {                
                throw new RuntimeException(e);
            }
        }
        return tuple;
    }
}
