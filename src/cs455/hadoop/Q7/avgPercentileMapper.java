/*
 * Created by Saurabh on 4/2/2017.
 */

package cs455.hadoop.Q7;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class avgPercentileMapper extends Mapper<LongWritable, Text, Text, Text>
{

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException
    {
        String line = value.toString();
        int partNo = Integer.parseInt(line.substring(24, 27));
        if (partNo == 2)
        {
            int lineSummary = Integer.parseInt(line.substring(10, 12));
            if(lineSummary == 100)
            {
                // Reading the state (output key)
                String state = line.substring(8, 9);

                // Reading the counts (output value)
                String bigString = toString(line);

                // Writing the output
                context.write(new Text(state), new Text(bigString));
            }
        }
    }

    private static String toString(String line)
    {
        String bigString = "";

        bigString = bigString + line.substring(2388, 2396) + ":";
        bigString = bigString + line.substring(2397, 2405) + ":";
        bigString = bigString + line.substring(2406, 2414) + ":";
        bigString = bigString + line.substring(2415, 2423) + ":";
        bigString = bigString + line.substring(2424, 2432) + ":";
        bigString = bigString + line.substring(2433, 2441) + ":";
        bigString = bigString + line.substring(2442, 2450) + ":";
        bigString = bigString + line.substring(2451, 2459) + ":";
        bigString = bigString + line.substring(2460, 2468);

        return bigString;
    }

}
