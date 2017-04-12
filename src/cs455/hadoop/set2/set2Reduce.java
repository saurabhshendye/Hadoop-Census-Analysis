/*
 * Created by Saurabh on 4/12/2017.
 */

package cs455.hadoop.set2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class set2Reduce extends Reducer<Text, Text, Text, Text>
{
    private static long [] q8summary = new long[2];
    private static double q8percent = 0.0d;
    private static String q8state;

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
        for (Text value: values)
        {
            String [] byParts = value.toString().split("/");
            if (byParts[0].equals("part-1"))
            {
                String [] parts = byParts[1].split(":");
                q8addToArray(parts);
            }
        }

        if (q8percent < q8findPercent())
        {
            q8percent = q8findPercent();
            q8state = key.toString();
        }
    }

//--------------Q8 Methods--------------//
    private static void q8addToArray(String [] parts)
    {
        for (int i = 0; i < q8summary.length; i++)
        {
            q8summary[i] = q8summary[i] + Long.parseLong(parts[i]);
        }
    }

    private static double q8findPercent()
    {
//        float percent = summary[1]/(summary[0] + summary[1]);
        return q8summary[1] * 100.0d/(q8summary[0]);
    }
}
