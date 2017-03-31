/*
 * Created by Saurabh on 3/31/2017.
 */

package cs455.hadoop.Q8;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class oldPeopleCountMapper extends Mapper<LongWritable, Text, Text, Text>
{
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException
    {
        String line = value.toString();
        int partNo = Integer.parseInt(line.substring(24, 27));
        if (partNo == 1)
        {
            int lineSummary = Integer.parseInt(line.substring(10, 12));
            if(lineSummary == 100)
            {
                // Reading the state (output key)
                String state = line.substring(8, 9);

                // Reading the counts (output value)
                String persons = line.substring(300, 308);
                String oldPeopleCount = line.substring(1065, 1073);

                // Writing the output
                context.write(new Text(state), new Text(persons + ":" + oldPeopleCount));
            }
        }
    }
}
