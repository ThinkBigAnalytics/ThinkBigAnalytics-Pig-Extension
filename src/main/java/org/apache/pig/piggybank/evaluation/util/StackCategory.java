/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.piggybank.evaluation.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * <dl>
 * <dt><b>Syntax:</b></dt>
 * <dd><code>String StackCategory(Tuple stackFrameArray)</code>.</dd>
 * <dt><b>Input:</b></dt>
 * <dt><b>Output:</b></dt>
 * </dl>
 */

public class StackCategory extends EvalFunc<String> {

	private static final Log LOG = LogFactory.getLog(StackCategory.class);

	@Override
	public String exec(Tuple t) throws IOException {
		/*
		 * if (t.size()!=1) { String msg =
		 * "StackCategory : Only 1 parameters are allowed."; throw new
		 * IOException(msg); }
		 */
		if (t.get(0) == null)
			return null;
		LOG.debug(t.toDelimitedString(","));

		List<String> frames = new ArrayList<String>();
		try {
			// unpack arguments
			DefaultDataBag db = (DefaultDataBag) t.get(0);

			// loop through array of stack frames that make up a stack trace
			for (Tuple tuple : db) {
				frames.add((String)tuple.get(0));
			}
		} catch (Exception e) {
			LOG.error(e.getMessage());
			return null;
		}

		StackTrace stack = new StackTrace(frames);
		return stack.getCategory();
	}

	String mExpression = null;
	Pattern mPattern = null;

	@Override
	public Schema outputSchema(Schema input) {
		try {
			return new Schema(new Schema.FieldSchema(getSchemaName(this
					.getClass().getName().toLowerCase(), input),
					DataType.CHARARRAY));
		} catch (Exception e) {
			return null;
		}
	}

	/*
	 * @Override public List<FuncSpec> getArgToFuncMapping() throws
	 * FrontendException { List<FuncSpec> funcList = new ArrayList<FuncSpec>();
	 * Schema s = new Schema(); s.add(new Schema.FieldSchema(null,
	 * DataType.CHARARRAY)); s.add(new Schema.FieldSchema(null,
	 * DataType.CHARARRAY)); funcList.add(new
	 * FuncSpec(this.getClass().getName(), s)); return funcList; }
	 */
}
