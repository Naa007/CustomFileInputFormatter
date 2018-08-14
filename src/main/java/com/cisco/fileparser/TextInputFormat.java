package com.cisco.fileparser;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;


public class TextInputFormat extends FileInputFormat<LongWritable, Text> {
	 
	  @Override
	  public RecordReader<LongWritable, Text>
	    createRecordReader(InputSplit split,
	                       TaskAttemptContext context) {
	 
	// Hardcoding this value as “.”
	// You can add any delimiter as your requirement
	 
	    String delimiter = ".";
	    byte[] recordDelimiterBytes = null;
	    if (null != delimiter)
	      recordDelimiterBytes = delimiter.getBytes();
	    return new LineRecordReader(recordDelimiterBytes);
	  }
	 
	  @Override
	  protected boolean isSplitable(JobContext context, Path file) {
	    CompressionCodec codec =
	      new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
	    return codec == null;
	  }
	}