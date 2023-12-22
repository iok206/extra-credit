import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InjuredMapper extends Mapper<Object, Text, Text, IntWritable> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

		String[] line = value.toString().split(",", -1);

		if(line[0] != null && line[0].length() == 1){

			context.write(new Text(line[2]), new IntWritable(Integer.parseInt(line[0])));

		}
	
	}

}
