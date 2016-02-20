package bigdata.sea.inmemoryjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NameZipApplication extends Configured implements Tool {
	public static final String FRIENDS = "friends";
	public static final String USER_A = "userA";
	public static final String USER_B = "userB";
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new NameZipApplication(),
				args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			System.out.println("usage: [input] [output] [userA] [userB]");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		conf.set(USER_A, args[2]);
		conf.set(USER_B, args[3]);
		conf.set(FRIENDS, args[4]);
		
		Job job = new Job(conf, "nameZip");
        
        job.setJarByClass(NameZipApplication.class);
        job.setMapperClass(NameZipMapper.class);
        job.setReducerClass(NameZipReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
		if(job.waitForCompletion(true))
			return 1;
		return 0;
	}

}
