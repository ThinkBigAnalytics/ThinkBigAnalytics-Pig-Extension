package org.apache.pig.piggybank.evaluation.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

class StackTrace {
	private final List<String> frames;
	public StackTrace(List<String> frames) {
		this.frames = frames;
	}
	
	public static final String UNKNOWN_CATEGORY = "unknown";
	
	public String getCategory() {
		for (Rule rule : RULES) {
			if (rule.condition.matches(frames)) {
				return rule.category;
			}
		}
		return UNKNOWN_CATEGORY;
	}
	
	private static interface Condition {
		public boolean matches(List<String> frames);
	}
	
	private static final class Rule {
		public final Condition condition;
		public final String category;
		public Rule(Condition condition, String category) {
			this.condition = condition;
			this.category = category;
		}
	}
	
	private static enum Check {
		STARTS_WITH, ENDS_WITH, CONTAINS, REGEX
	}
	
	private static enum Region {
		TOP, ALL, BOTTOM
	}
	
	private static final class And implements Condition {
		private final Condition[] conditions;
		public And(Condition... conditions) {
			this.conditions = conditions;
		}
		@Override
		public boolean matches(List<String> frames) {
			for (Condition condition : conditions) {
				if (!condition.matches(frames)) {
					return false;
				}
			}
			return true;
		}
	}
	
	private static final class StringSearch implements Condition {
		private final String str;
		private final Check check;
		private final Region region;
		private final Pattern regex;
		public StringSearch(Region region, Check check, String str) {
			this.check = check;
			this.str = str;
			this.region = region;
			if (check.equals(Check.REGEX)) {
				regex = Pattern.compile(str);
			} else {
				regex = null;
			}
		}
		
		public boolean matches(List<String> frames) {
			List<String> framesToCheck = getFramesToCheck(frames);
			for (String frame : framesToCheck) {
				if (check(frame)) {
					return true;
				}
			}
			return false;
		}
		
		private List<String> getFramesToCheck(List<String> frames) {
			switch (region) {
			case TOP: return frames.subList(0, 1);
			case BOTTOM: return frames.subList(frames.size() - 1, frames.size());
			case ALL: return frames;
			default: return null;
			}
		}
		
		private boolean check(String frame) {
			switch (check) {
			case STARTS_WITH: return frame.startsWith(str);
			case ENDS_WITH: return frame.endsWith(str);
			case CONTAINS: return frame.contains(str);
			case REGEX: return regex.matcher(str).find();
			default: return false;
			}
		}
	}

	
	@SuppressWarnings("serial")
	private static final List<Rule> RULES = new ArrayList<Rule>() {{
		// to be removed/refactored
		add(new Rule(new StringSearch(Region.BOTTOM, Check.CONTAINS, "java.nio"), "io"));
		add(new Rule(new StringSearch(Region.BOTTOM, Check.CONTAINS, "java.io"), "io"));
		add(new Rule(new StringSearch(Region.BOTTOM, Check.CONTAINS, "java.net"), "io"));
		add(new Rule(new StringSearch(Region.BOTTOM, Check.CONTAINS, "com.sun.org.apache.xerces"), "parser"));
		add(new Rule(new StringSearch(Region.BOTTOM, Check.CONTAINS, "java.net.URI$Parser"), "parser"));
		add(new Rule(new StringSearch(Region.BOTTOM, Check.CONTAINS, "com.sun.org.apache.xml.internal.serializer"), "parser"));
		add(new Rule(new StringSearch(Region.BOTTOM, Check.CONTAINS, "java.lang.ref.Finalizer"), "gc"));
		add(new Rule(new StringSearch(Region.BOTTOM, Check.CONTAINS, "org.apache.hadoop"), "hadoop"));
		add(new Rule(new StringSearch(Region.BOTTOM, Check.STARTS_WITH, "thinkbig.profiler"), "profiler"));
		add(new Rule(new StringSearch(Region.BOTTOM, Check.STARTS_WITH, "org.apache.pig"), "pig"));
		add(new Rule(new StringSearch(Region.BOTTOM, Check.STARTS_WITH, "com."), "user"));
		
		// "keepers"
		add(new Rule(new StringSearch(Region.ALL, Check.CONTAINS, "ReduceTask$ReduceCopier$ShuffleRamManager"), "IO.Shuffle"));
		add(new Rule(new StringSearch(Region.ALL, Check.CONTAINS, "MapTask$MapOutputBuffer.sortAndSpill"), "IO.Sort")); // might want to look for more here...
		
		add(new Rule(new StringSearch(Region.ALL, Check.STARTS_WITH, "org.apache.commons.logging"), "IO.Logging"));
		
		add(new Rule(new StringSearch(Region.TOP, Check.STARTS_WITH, "org.sqlite"), "IO.SQL.SQLite"));
		
		add(new Rule(new StringSearch(Region.ALL, Check.CONTAINS, "DFSClient$DFSOutputStream"), "IO.HDFS.Write"));
		add(new Rule(new StringSearch(Region.ALL, Check.CONTAINS, "DFSClient$DFSInputStream.read"), "IO.HDFS.Read"));
		
		add(new Rule(new StringSearch(Region.ALL, Check.CONTAINS, "MapContext.nextKeyValue("), "CPU.Map_Input"));
		add(new Rule(new StringSearch(Region.ALL, Check.CONTAINS, "NewDirectOutputCollector.write("), "CPU.Map-Only_Write"));
		
		add(new Rule(new StringSearch(Region.ALL, Check.CONTAINS, "MapTask.run"), "CPU.Mapper"));
		add(new Rule(new StringSearch(Region.ALL, Check.CONTAINS, "ReduceTask.run"), "CPU.Reducer"));
	}};
}
