package fr.inria.orpheus.javabip.mapreduce.examples.internalParallelism;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.javabip.annotations.ComponentType;
import org.javabip.annotations.Port;
import org.javabip.annotations.Ports;
import org.javabip.annotations.Transition;
import org.javabip.api.PortType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ComponentType(initial = "start", name = "fr.inria.orpheus.reducers.IntSum")
@Ports({ @Port(name = "in", type = PortType.enforceable), @Port(name = "out", type = PortType.spontaneous) })
public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private static Logger classLogger;
	private String id;
	private IntWritable result = new IntWritable();
	private int parallelizationFactor;

	@Transition(name = "init", source = "start", target = "ready", guard = "")
	@Override
	protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		id = context.getConfiguration().get(JobContext.TASK_ATTEMPT_ID);
		parallelizationFactor = context.getConfiguration().getInt(WordCount.REDUCER_PARALLELISATION, 0);
	}

	@Transition(name = "in", source = "ready", target = "done", guard = "")
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	}

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		String mode = context.getConfiguration().get(WordCount.EXECUTION_MODE);
		if (mode.equals("bip")) {
			// BIP should invoke reduce
			// Thus calling the last transition, should block until the done state has been reached
			fin();
			cleanup(context);
		} else if (mode.equals("hadoop")) {
			try {
				while (context.nextKey()) {

					reduce(context.getCurrentKey(), context.getValues(), context);
					// If a back up store is used, reset it
					Iterator<IntWritable> iter = context.getValues().iterator();
					if (iter instanceof ReduceContext.ValueIterator) {
						((ReduceContext.ValueIterator<IntWritable>) iter).resetBackupStore();
					}
				}
			} finally {
				cleanup(context);
			}
		}
	}

	@Transition(name = "out", source = "done", target = "fin", guard = "")
	public void fin() {
	}

	{
		classLogger = LoggerFactory.getLogger(this.getClass());
	}
}