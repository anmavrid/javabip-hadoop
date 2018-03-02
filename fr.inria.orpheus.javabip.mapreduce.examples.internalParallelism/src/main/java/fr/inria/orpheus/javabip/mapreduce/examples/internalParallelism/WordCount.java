/**
 * This is an adaptation of the WordCount (from
 * ${HADOOP_HOME}/hadoop/src/examples/org/apache/hadoop/examples/WordCount) with
 * having the option of executing it with the coordination managed by Hadoop or
 * JavaBIP.
 * 
 * @author Stefanos Skalistis
 */

package fr.inria.orpheus.javabip.mapreduce.examples.internalParallelism;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.javabip.api.BIPEngine;
import org.javabip.api.BIPGlue;
import org.javabip.engine.factory.EngineFactory;
import org.javabip.glue.GlueBuilder;


import akka.actor.ActorSystem;

public class WordCount {

	private static boolean executeWithJavaBIP = true;
	private static int noMappers = 3;
	private static int noReducers = 2;
	private static int mapParallelisaation = 2;
	private static int reduceParallelisation = 3;

	private static File inputFolder;
	private static File outputFolder;

	public static final String MAP_PARALLELISATION = "orpheus.mapper.parallelisation";
	public static final String REDUCER_PARALLELISATION = "orpheus.reducer.parallelisation";
	public static final String EXECUTION_MODE = "orpheus.execution.mode";

	public static void main(String[] args) throws Exception {

		parseArgs(args);
		sanityCheck();

		Configuration conf = new Configuration();
		conf.setInt(MRJobConfig.NUM_MAPS, noMappers);
		conf.setInt(MRJobConfig.NUM_REDUCES, noReducers);
		conf.setInt(MAP_PARALLELISATION, mapParallelisaation);
		conf.setInt(REDUCER_PARALLELISATION, reduceParallelisation);
		conf.set(EXECUTION_MODE, (executeWithJavaBIP ? "bip" : "hadoop"));
		Job job = Job.getInstance(conf, "orpheus.wordcount");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		if (executeWithJavaBIP) {
			invokeJavaBIP();
		} else {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}

	private static void invokeJavaBIP() {
		ActorSystem system = ActorSystem.create("JavaBIP MapReduce");
		EngineFactory engineFactory = new EngineFactory(system);
		BIPGlue glue = createGlue("src/main/resources/config/Glue.xml");

		Map<String, String> componentIDsToNames = new HashMap<String, String>();
		Map<String, String> componentIDsToCardinalities = new HashMap<String, String>();
		Map<String, String> componentIDsToInstanceIDs = new HashMap<String, String>();

		componentIDsToCardinalities.put("fr.inria.orpheus.mappers.Tokenizer", String.valueOf(noMappers));
		componentIDsToNames.put("fr.inria.orpheus.mappers.Tokenizer", "TokenizerMapper");
		componentIDsToCardinalities.put("fr.inria.orpheus.reducers.IntSum", String.valueOf(noReducers));
		componentIDsToNames.put("fr.inria.orpheus.reducers.IntSum", "IntSumReducer");


		BIPEngine engine = engineFactory.create("Hadoop", glue);

		String mapperIDs = "";
		for (int i = 0; i < noMappers; i++) {
			TokenizerMapper mapper = new TokenizerMapper();
			engine.register(mapper, String.valueOf(i), true);
			mapperIDs += i + ",";
		}
		mapperIDs = mapperIDs.substring(0, mapperIDs.length() - 1); // remove last ','

		componentIDsToInstanceIDs.put("fr.inria.orpheus.mappers.Tokenizer", mapperIDs);

		String reducerIDs = "";
		for (int i = 0; i < noReducers; i++) {
			IntSumReducer reducer = new IntSumReducer();
			engine.register(reducer, String.valueOf(i), true);
			mapperIDs += i + ",";
		}
		reducerIDs = reducerIDs.substring(0, reducerIDs.length() - 1); // remove last ','

		componentIDsToInstanceIDs.put("fr.inria.orpheus.reducers.IntSum", mapperIDs);

		engine.start();
		engine.execute();

//		while (engine.isEngineExecuting()) {
//			try {
//				Thread.sleep(2000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//
//			engine.stop();
//			engineFactory.destroy(engine);
//
//		}
		system.shutdown();

	}

	private static BIPGlue createGlue(String bipGlueFilename) {
		BIPGlue bipGlue = null;

		InputStream inputStream;
		try {
			inputStream = new FileInputStream(bipGlueFilename);

			bipGlue = GlueBuilder.fromXML(inputStream);

		} catch (FileNotFoundException e) {

			e.printStackTrace();
		}
		return bipGlue;
	}

	private static void parseArgs(String[] args) {
		if (args.length < 2) {
			System.err.println("Invalid number of arguments; at least input/output folders are required");
			System.exit(-1);
		}

		inputFolder = new File(args[0]);
		if (!inputFolder.exists()) {
			System.err.println("Input path \"" + args[0] + "\" does not exist.");
			System.exit(-1);
		} else if (!inputFolder.isDirectory()) {
			System.err.println("Input path \"" + args[0] + "\" in not a directory.");
			System.exit(-1);
		}

		outputFolder = new File(args[1]);
		if (outputFolder.exists()) {
			System.err.println("Output path \"" + args[1] + "\" exists and will be overwritten.");
			try {
				FileUtils.deleteDirectory(outputFolder);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(-10);
			}
		}

		if (args.length >= 3) {
			if (args[2].compareToIgnoreCase("bip") == 0)
				executeWithJavaBIP = true;
			else if (args[2].compareToIgnoreCase("hadoop") == 0)
				executeWithJavaBIP = false;
			else {
				System.err.println("Invalid input \"" + args[2] + "\". Input must be either \"Hadoop\" or \"BIP\"");
				System.exit(-1);
			}
		}

		for (int i = 3; i < args.length; i++) {
			if (args[i].startsWith("-M=") || args[i].startsWith("-mappers="))
				noMappers = Integer.parseInt(args[i].substring(args[i].indexOf('=') + 1));
			else if (args[i].startsWith("-R=") || args[i].startsWith("-reducers="))
				noReducers = Integer.parseInt(args[i].substring(args[i].indexOf('=') + 1));
			else if (args[i].startsWith("-MP=") || args[i].startsWith("-mapFactor="))
				mapParallelisaation = Integer.parseInt(args[i].substring(args[i].indexOf('=') + 1));
			else if (args[i].startsWith("-RP=") || args[i].startsWith("-reduceFactor="))
				reduceParallelisation = Integer.parseInt(args[i].substring(args[i].indexOf('=') + 1));
			else {
				System.out.println("Input argument \"" + args[i] + "\" was ignored.");
			}

		}

		return;
	}

	private static void sanityCheck() {
		// Sanity check
		if (noMappers * mapParallelisaation != noReducers * reduceParallelisation) {
			System.err.println("Factors are imbalanced...exiting.");
			System.exit(-2);
		}
	}
}