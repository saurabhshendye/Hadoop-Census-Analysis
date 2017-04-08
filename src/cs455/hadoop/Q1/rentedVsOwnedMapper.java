/*
 * Created by Saurabh on 3/28/2017.
 */

package cs455.hadoop.Q1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class rentedVsOwnedMapper extends Mapper<LongWritable, Text, Text, Text>
{
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
            String line = value.toString();
            int partNo = Integer.parseInt(line.substring(24, 28));
            if (partNo == 2)
            {
                int lineSummary = Integer.parseInt(line.substring(10, 13));
                if(lineSummary == 100)
                {
                    String state = line.substring(8, 10);
                    String owned = line.substring(1803, 1812);
                    String rented = line.substring(1812, 1821);
                    context.write(new Text(state), new Text(owned + ":" + rented));
                }
            }
    }

}
