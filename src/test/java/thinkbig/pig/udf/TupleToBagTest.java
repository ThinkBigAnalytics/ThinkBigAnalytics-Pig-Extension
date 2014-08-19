/**
 * Copyright (C) 2010-2014 Think Big Analytics, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
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
