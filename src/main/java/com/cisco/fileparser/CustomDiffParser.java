package com.cisco.fileparser;

/*
 * 
 * 
 */
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CustomDiffParser extends Configured implements Tool {

	public static class WholeFileInputFormat extends
			FileInputFormat<NullWritable, BytesWritable> {
		
		@Override
		protected boolean isSplitable(FileSystem fs, Path filename) {
			return false;
		}
		
		@Override
		public RecordReader<NullWritable, BytesWritable> getRecordReader(
				InputSplit split, JobConf job, Reporter reporter)
				throws IOException {
			return new WholeFileRecordReader((FileSplit) split, job);
		}
	}

	public static class WholeFileRecordReader implements RecordReader<NullWritable, BytesWritable> {
		
		private FileSplit fileSplit;
		private Configuration conf;
		private boolean processed = false;

		public WholeFileRecordReader(FileSplit fileSplit, Configuration conf)
				throws IOException {
			this.fileSplit = fileSplit;
			this.conf = conf;
		}

		public NullWritable createKey() {
			return NullWritable.get();
		}

		public BytesWritable createValue() {
			return new BytesWritable();
		}

		public long getPos() throws IOException {
			return processed ? fileSplit.getLength() : 0;
		}

		public float getProgress() throws IOException {
			return processed ? 1.0f : 0.0f;
		}

		public boolean next(NullWritable key, BytesWritable value)
				throws IOException {
			if (!processed) {
				byte[] contents = new byte[(int) fileSplit.getLength()];
				Path file = fileSplit.getPath();
				FileSystem fs = file.getFileSystem(conf);
				FSDataInputStream in = null;
				try {
					in = fs.open(file);
					IOUtils.readFully(in, contents, 0, contents.length);
					value.setCapacity(contents.length);
					value.set(contents, 0, contents.length);
				} finally {
					IOUtils.closeStream(in);
				}
				processed = true;
				return true;
			}
			return false;
		}

		public void close() throws IOException {
			// do nothing
		}
	}

	public static class FileParserMapper extends MapReduceBase implements
			Mapper<NullWritable, BytesWritable, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		InputStream is = null;
		byte[] words;

		public void map(NullWritable key, BytesWritable value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			words = value.getBytes();
			int size = words.length;
			byte[] b = new byte[size];
			is = new ByteArrayInputStream(words);
			is.read(b);
			extractData(new Text(b).toString(), output);
			b = null;
			words = null;
			is = null;
		}

		public OutputCollector<Text, IntWritable> extractData(String file,
				OutputCollector<Text, IntWritable> output) throws IOException {
			if(!StringUtils.isEmpty(file) && file != null){
				String endOfRecord = "11110007778889990001111";
				String records[] = file.split(endOfRecord);
				for (int i = 0; i < records.length; i++) {
					if (!StringUtils.isEmpty(records[i]) && records[i] != null && records[i].trim().indexOf(',') != -1) {
						String reviewId = records[i].substring(0,records[i].indexOf(',',1));
						String createDate = records[i].substring(records[i].indexOf(',')+1,records[i].indexOf(',')+20);
						HashMap<String, Object> resultMap = new HashMap<String, Object>();
						try{
							//resultMap = getLocs(Long.parseLong(reviewId.trim()), createDate.trim(), records[i]);
							resultMap = getComponents(Long.parseLong(reviewId.trim()),  createDate, records[i]);
						}catch(Exception e){
							e.printStackTrace();
						}
//						String result = resultMap.get("reveiwId") + "," + resultMap.get("createDate") + "," +resultMap.get("addLocs") + "," + resultMap.get("delLocs") + "," + resultMap.get("modLocs") + ",";
						String result = resultMap.get("reveiwId") + "," + resultMap.get("createDate") + "," + resultMap.get("components");
						reviewId = null;
						createDate = null;
						resultMap = null;
						output.collect(new Text(result), new IntWritable());
						result = null;
					}
				}
				records = null;
				endOfRecord = null;
			}
			return output;
		}

		private HashMap<String, Object> getComponents(Long reviewId, String createDate, String source) {
				
				String ComponentNmae = "";
	            String SoruceBaseName = "";
	            int diffindex = 0;
	            String ComponentValue = "";
					
				HashMap<String, Object> hashMap = null;
				hashMap = new HashMap<String, Object>();
	
				if ((source != null) && !("".equals(source))) {
					String[] saSourceLines = source.split("\n");
					String line = null;
					int iTotalLineCount = saSourceLines.length;
					Pattern pattern1 = Pattern.compile("^Component:\\s"); 
					
					StringBuffer setOfComponents = new StringBuffer("");
	
					for (int next = 0; iTotalLineCount > next; next++) {
						line = saSourceLines[next];
						if (pattern1.matcher(line).find()) {
							ComponentNmae = line.substring(10).trim();
							if( line != null && !StringUtils.isEmpty(line)) {
								if(line.indexOf("@") !=-1 && line.indexOf("(") == -1) {
									ComponentValue = ComponentNmae.split("@")[0];
								}else if (line.indexOf("(") !=-1 && line.indexOf("@") ==-1){
									ComponentValue = ComponentNmae.split("\\(")[0];
								}else if (line.indexOf("@") !=-1 && line.indexOf("(") !=-1){
									if( line.indexOf("@") > line.indexOf("(")){
										ComponentValue = ComponentNmae.split("@")[0];
									}else {
										ComponentValue = ComponentNmae.split("\\(")[0];
									}
								}
								setOfComponents.append(ComponentValue).append(";");
							}
						}
					}
	
					hashMap.put("reveiwId", reviewId);
					hashMap.put("createDate", createDate);
					hashMap.put("components", setOfComponents.toString());
					saSourceLines = null;
					source = null;
				}
				return hashMap;
			}
	

		
		private HashMap<String, Object> getLocs(Long reviewId, String createDate, String source) {
			
			int skip = 1;
			int context = 2;
			int newState = 3;
			int comment = 4;
			int state = skip;
			int deleted = 0;
			int added = 0;
			int nchanged = 0;
			int ostate = 0;
			int comment_added = 0; 
			int blank_added = 0;
			
			HashMap<String, Object> hashMap = null;
			hashMap = new HashMap<String, Object>();

			if ((source != null) && !("".equals(source))) {
				String[] saSourceLines = source.split("\n");
				String line = null;
				int iTotalLineCount = saSourceLines.length;
				Pattern pattern1 = Pattern.compile("^\\*\\*\\*\\*\\*"); // Stars with 5*'s
				Pattern pattern2 = Pattern.compile("^\\*\\*\\* "); // Starts with 5*'s
				Pattern pattern3 = Pattern.compile("^--- "); // Starts with 3-'s
				Pattern pattern4 = Pattern.compile("^- "); // Starts with -
				Pattern pattern5 = Pattern.compile("^! "); // Starts with !
				Pattern pattern6 = Pattern.compile("^\\+ [ \t]*$"); // Starts with +
				Pattern pattern7 = Pattern.compile("^[ \t]*\\/\\*"); // Begin block comment
				Pattern pattern8 = Pattern.compile("\\*\\/");
				Pattern pattern9 = Pattern.compile("^\\+ [ \t]*\\/\\*");
				Pattern pattern10 = Pattern.compile("^[ \t]*\\*\\/"); 
				// Pattern pattern11 =  Pattern.compile("^\\+ [ \t]*.*\\*\\/");//End block comment
				Pattern pattern11 = Pattern.compile(".*\\*\\/"); // End of block comment
				Pattern pattern12 = Pattern.compile("^\\+ [ \t]*\\*"); // Content of block comment
				Pattern pattern13 = Pattern.compile("^\\+ ");
				// Added for GIT support / unified diff support
				Pattern pattern14 = Pattern.compile("^@@ -"); // Blocks start with @@ -pattern
				Pattern pattern15 = Pattern.compile("^\\+{3} "); // Starts with + but not with +++ which indicates the block index
				Pattern pattern16 = Pattern.compile("^\\+");
				Pattern pattern17 = Pattern.compile("^\\-");

				int unified = 6;

				for (int next = 0; iTotalLineCount > next; next++) {
					line = saSourceLines[next];
					if (pattern1.matcher(line).find()) {
						state = context;

					} else if (pattern14.matcher(line).find()) {
						state = unified;
					} else if (state == skip) {
					} else if ((pattern2.matcher(line).find())
							&& (state == context)) {
					} else if ((pattern3.matcher(line).find())
							&& (state == context)) {
						state = newState;
					} else if ((pattern4.matcher(line).find())
							&& (state == context)) {
						deleted++;
					} else if ((pattern5.matcher(line).find())
							&& (state == context)) {
						nchanged++;
					} else if (state == context) {
					} else if ((pattern5.matcher(line).find())
							&& (state == newState)) {
						nchanged++;
					} else if (state == context) {
					} else if (pattern6.matcher(line).find()) {
						blank_added++;
					} else if ((pattern7.matcher(line).find())
							&& !((pattern8.matcher(line)).find())) {
						ostate = state;
						state = comment;
					} else if (((pattern9.matcher(line)).find())
							&& !(pattern8.matcher(line).find())) {
						ostate = state;
						state = comment;
						comment_added++;
					} else if ((pattern9.matcher(line).find())
							&& (pattern8.matcher(line).find())) {
						comment_added++;
					} else if ((pattern10.matcher(line).find())
							&& (state == comment)) {
						state = ostate;
					} else if ((pattern11.matcher(line).find())
							&& (state == comment)) {
						state = ostate;
						comment_added++;
					} else if ((pattern12.matcher(line).find())
							&& (state == comment)) {
						comment_added++;
					} else if ((pattern13.matcher(line).find())
							&& (state == comment)) {
						comment_added++;
					} else if ((pattern13.matcher(line).find())
							&& (state == newState)) {
						added++;
					} else if (pattern16.matcher(line).find()
							&& state == unified
							&& !pattern15.matcher(line).find()) {
						added++;
					} else if (pattern17.matcher(line).find()
							&& state == unified
							&& !pattern3.matcher(line).find()) {
						deleted++;
					}
				}

				hashMap.put("reveiwId", reviewId);
				hashMap.put("createDate", createDate);
				hashMap.put("addLocs", (long) added);
				hashMap.put("delLocs", (long) deleted);
				hashMap.put("modLocs", (long) Math.ceil((nchanged * 0.5)));
				saSourceLines = null;
				source = null;
			}
			return hashMap;
		}

	}

	public static class FileParserReducer extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	/* </Task1> */
	/*
	 * <run>
	 */
	public int run(String[] args) throws Exception {

		if (args.length != 3) {
			printUsage();
			return 1;
		}

		String endOfRecord = args[0];
		String inputPath = args[1];
		String outputPath = args[2];

		deleteOldOutput(outputPath);

		JobConf conf = new JobConf(CustomDiffParser.class);
//		FileOutputFormat.setCompressOutput(conf, true);
//		FileOutputFormat.setOutputCompressorClass(conf, org.apache.hadoop.io.compress.SnappyCodec.class);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileInputFormat.setInputPaths(conf, new Path(inputPath));

		/* conf: Task1 */
		conf.setJobName("PRRQ-LOCGenerator");
		conf.set("mapreduce.map.java.opts","-Xmx8000m");
		conf.set("mapreduce.reduce.java.opts","-Xmx8000m");
		conf.set("mapreduce.reduce.memory.mb","8192");
		conf.set("mapreduce.map.memory.mb","8192");
		conf.set("mapreduce.task.timeout", "4200000");
		
		
		/* Output: Key:Text -> Value:Integer */
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setOutputFormat(TextOutputFormat.class);
		/* Input: Key.Text -> Value:Text */
		conf.setInputFormat(WholeFileInputFormat.class);
		conf.setMapperClass(FileParserMapper.class);
		conf.setReducerClass(FileParserReducer.class);
		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new CustomDiffParser(), args);
		System.exit(res);
	}

	private void printUsage() {
		//System.out.println("usage: [recordSeparator] [input-path] [output-path]");
		return;
	}

	private void deleteOldOutput(String outputPath) throws IOException {
		// Delete the output directory if it exists already
		Path outputDir = new Path(outputPath);
		FileSystem.get(getConf()).delete(outputDir, true);
	}
}