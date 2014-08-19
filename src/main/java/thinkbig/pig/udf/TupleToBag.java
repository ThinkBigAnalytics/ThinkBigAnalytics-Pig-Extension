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

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;

/**
 * This class takes a list of tuples and puts their CONTENTS into a bag. This is different
 * from TOBAG, which wraps its arguments in a tuple.
 * 
 * Output schema:
 * The output schema for this udf depends on the schema of its arguments.
 * If all the contents of the tupleshave same type and same inner 
 * schema (for bags/tuple columns), then the udf output schema would be a bag 
 * of tuples having a column of the type and inner-schema (if any) of the 
 * arguments. 
 * If the arguments are of type tuple/bag, then their innerschema, including
 * the alias names should match.
 * If these conditions are not met the output schema will be a bag with null 
 * inner schema.
 *  
 *  example 1 
 *  grunt> describe a;
 *  a: {a0: int,a1: int}
 *  grunt> b = foreach a generate TupleToBag(a0,a1);
 *  grunt> describe b;
 *  b: {{int}}
 *  
 *  example 2
 *  grunt> describe a;
 *  a: {a0: (x: int),a1: (x: int)}
 *  grunt> b = foreach a generate TOBAG(a0,a1);                                    
 *  grunt> describe b;                                                             
 *  b: {{(x: int)}}
 *  
 *  example 3
 *  grunt> describe a;                                                             
 *  a: {a0: (x: int),a1: (y: int)}
 * -- note that the inner schema is different because the alises (x & y) are different
 *  grunt> b = foreach a generate TOBAG(a0,a1);                                    
 *  grunt> describe b;                                                             
 *  b: {{NULL}}
 *  
 *  
 * 
 */
public class TupleToBag extends EvalFunc<DataBag> {

    public DataBag exec(Tuple input) throws IOException {
        try {
            DataBag bag = BagFactory.getInstance().newDefaultBag();

            for (int i = 0; i < input.size(); i++) {
                final Object object = input.get(i);
                if (object instanceof Tuple) {
                    for (int j = 0; j < ((Tuple)object).size(); j++) {
                        Tuple tp2 = TupleFactory.getInstance().newTuple(1);
                        tp2.set(0, ((Tuple)object).get(j));
                        bag.add(tp2);
                    }
                } else {
                    Tuple tp2 = TupleFactory.getInstance().newTuple(1);
                    tp2.set(0, object);
                    bag.add(tp2);                    
                }
            }

            return bag;
        } catch (Exception ee) {
            throw new RuntimeException("Error while creating a bag", ee);
        }
    }
}

