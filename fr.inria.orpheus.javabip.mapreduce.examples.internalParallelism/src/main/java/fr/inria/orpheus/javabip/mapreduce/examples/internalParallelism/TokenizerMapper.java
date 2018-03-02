package fr.inria.orpheus.javabip.mapreduce.examples.internalParallelism;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import org.javabip.annotations.ComponentType;
import org.javabip.annotations.Port;
import org.javabip.annotations.Ports;
import org.javabip.annotations.Transition;
import org.javabip.api.PortType;

@ComponentType(initial = "start", name = "fr.inria.orpheus.mappers.Tokenizer")
@Ports({ @Port(name = "in", type = PortType.spontaneous), @Port(name = "out", type = PortType.enforceable) })
public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static String bipName = TokenizerMapper.class.getAnnotation(ComponentType.class).name();
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public TokenizerMapper() {
		super();
		
	}
	
	@Transition(name = "init", source = "start", target = "ready", guard = "")
	void configure() {
		
	}

	@Transition(name = "in", source = "ready", target = "done", guard = "")
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			context.write(word, one);
		}
	}

	@Transition(name = "out", source = "done", target = "fin", guard = "")
	void fin() {
	}

}

// public class TokenizerMapper {
// private Logger logger = LoggerFactory.getLogger(TokenizerMapper.class);
// private JSONLogger loggerJSON = JSONLogger.getLogger();
//
// private String componentPath = "/f/Q/A";
// private String componentType = "TokenizerMapper";
// private Map<String, String> stateToPath = new HashMap<String, String>();
// private Map<String, String> transitionToPath = new HashMap<String, String>();
//
// public TokenizerMapper() {
// this.stateToPath.put("done", "/f/Q/A/A");
// this.stateToPath.put("fin", "/f/Q/A/T");
// this.stateToPath.put("ready", "/f/Q/A/a");
// this.stateToPath.put("start", "/f/Q/A/L");
// this.transitionToPath.put("in", "/f/Q/A/q");
// this.transitionToPath.put("init", "/f/Q/A/l");
// this.transitionToPath.put("out", "/f/Q/A/m");
// }
// }
