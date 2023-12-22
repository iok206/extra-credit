import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Month {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration ();

		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Month.class);
		job.setMapperClass(MonthMapper.class);

		job.setCombinerClass(MonthReducer.class);
		job.setReducerClass(MonthReducer.class);

		job.setNumReduceTasks(1);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}