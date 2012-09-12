package org.apache.pig.piggybank.evaluation.util;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

class StackFrame {
	private final String frameText;
	public StackFrame(String frame) {
		this.frameText = frame;
	}
	
	public static final String UNKNOWN_CATEGORY = "unknown";
	
	public String getCategory() {
		for (StackCategoryRule rule : RULES.keySet()) {
			if (rule.matches(this)) {
				return RULES.get(rule);
			}
		}
		return UNKNOWN_CATEGORY;
	}
	
	private static enum Check {
		STARTS_WITH, ENDS_WITH, CONTAINS, REGEX
	}
	
	private static final class StackCategoryRule {
		public final String str;
		public final Check check;
		public StackCategoryRule(Check check, String str) {
			this.check = check;
			this.str = str;
		}
		
		public boolean matches(StackFrame stack) {
			switch (check) {
			case STARTS_WITH: return stack.frameText.startsWith(str);
			case ENDS_WITH: return stack.frameText.endsWith(str);
			case CONTAINS: return stack.frameText.contains(str);
			case REGEX: return Pattern.matches(str, stack.frameText);
			default: return false;
			}
		}
	}

	
	@SuppressWarnings("serial")
	private static final Map<StackCategoryRule, String> RULES = new HashMap<StackCategoryRule, String>() {{
		put(new StackCategoryRule(Check.CONTAINS, "java.nio"), "io");
		put(new StackCategoryRule(Check.CONTAINS, "java.io"), "io");
		put(new StackCategoryRule(Check.CONTAINS, "java.net"), "io");
		put(new StackCategoryRule(Check.CONTAINS, "com.sun.org.apache.xerces"), "parser");
		put(new StackCategoryRule(Check.CONTAINS, "java.net.URI$Parser"), "parser");
		put(new StackCategoryRule(Check.CONTAINS, "com.sun.org.apache.xml.internal.serializer"), "parser");
		put(new StackCategoryRule(Check.CONTAINS, "java.lang.ref.Finalizer"), "gc");
		put(new StackCategoryRule(Check.CONTAINS, "org.apache.hadoop"), "hadoop");
		put(new StackCategoryRule(Check.CONTAINS, "thinkbig.profiler"), "profiler");
		put(new StackCategoryRule(Check.CONTAINS, "org.apache.pig"), "pig");
		put(new StackCategoryRule(Check.STARTS_WITH, "com."), "user");
	}};
}
