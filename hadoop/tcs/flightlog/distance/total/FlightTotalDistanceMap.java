package distance.total;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class FlightTotalDistanceMap extends Mapper<LongWritable, Text, Text, IntWritable>{
	private IntWritable cDist = new IntWritable();
    private Text cKey = new Text();
    
    @Override
    public void map(LongWritable key, Text value, Context context) 
    		throws IOException, InterruptedException {
        String line = value.toString();
        String fields[] = line.split(",");
        
        cKey.set(fields[9]);
        cDist.set(Integer.parseInt(fields[18]));
        context.write(cKey, cDist);
    }
}
