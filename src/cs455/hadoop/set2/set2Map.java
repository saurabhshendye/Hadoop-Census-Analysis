/*
 * Created by Saurabh on 4/12/2017.
 */

package cs455.hadoop.set2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class set2Map extends Mapper<LongWritable, Text, Text, Text>
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
                // Reading the state (output key)
                String state = line.substring(8, 10);

                // Reading the counts (output value)
                String bigString = toString(line);

                // Writing the output
                context.write(new Text(state), new Text("part-2" + "/" + bigString));
            }
        }
        else if (partNo == 1)
        {
            int lineSummary = Integer.parseInt(line.substring(10, 13));
            if(lineSummary == 100)
            {
                // Reading the state (output key)
                String state = line.substring(8, 10);

                // Reading the counts (output value)
                String persons = line.substring(300, 309);
                String oldPeopleCount = line.substring(1065, 1074);

                // Writing the output
                context.write(new Text(state), new Text("part-1" + "/" + persons + ":" + oldPeopleCount));
            }
        }
    }

    private static String toString(String line)
    {
        String bigString = "";

        bigString = bigString + line.substring(2388, 2397) + ":";
        bigString = bigString + line.substring(2397, 2406) + ":";
        bigString = bigString + line.substring(2406, 2415) + ":";
        bigString = bigString + line.substring(2415, 2424) + ":";
        bigString = bigString + line.substring(2424, 2433) + ":";
        bigString = bigString + line.substring(2433, 2442) + ":";
        bigString = bigString + line.substring(2442, 2451) + ":";
        bigString = bigString + line.substring(2451, 2460) + ":";
        bigString = bigString + line.substring(2460, 2469);

        return bigString;
    }
}
