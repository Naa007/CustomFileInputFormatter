package com.cisco.fileparser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

public class TestComponentParser {

	public static void main(String[] args) throws IOException {
		String ComponentNmae = "";
		String SoruceBaseName = "";
		int diffindex = 0;
		String ComponentValue = "";
		TestComponentParser parser = new TestComponentParser();
		String source = parser
				.readFile("C:\\Users\\nmudunur\\Desktop\\samplediff.txt");
		if ((source != null) && !("".equals(source))) {
			String[] saSourceLines = source.split("\n");
			String line = null;
			int iTotalLineCount = saSourceLines.length;
			Pattern pattern1 = Pattern.compile("^Component:\\s"); // Stars with
																	// 5*'s

			Set<String> setOfComponents = new HashSet<String>();

			for (int next = 0; iTotalLineCount > next; next++) {
				line = saSourceLines[next];
				if (pattern1.matcher(line).find()) {
					ComponentNmae = line.substring(10).trim();
					if( line != null && !StringUtils.isEmpty(line)) {
						if(line.indexOf("@") !=-1) {
							ComponentValue = ComponentNmae.split("@")[0];
						}else if (line.indexOf("(") !=-1){
							ComponentValue = ComponentNmae.split("\\(")[0];
						}
						setOfComponents.add(ComponentValue);
					}
				}
			}
			System.out.println(setOfComponents.toString());
		}
	}

	private String readFile(String file) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = null;
		StringBuilder stringBuilder = new StringBuilder();
		String ls = System.getProperty("line.separator");

		try {
			while ((line = reader.readLine()) != null) {
				stringBuilder.append(line);
				stringBuilder.append(ls);
			}

			return stringBuilder.toString();
		} finally {
			reader.close();
		}
	}
}
