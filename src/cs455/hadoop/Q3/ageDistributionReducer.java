/*
 * Created by Saurabh on 4/2/2017.
 */

package cs455.hadoop.Q3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ageDistributionReducer extends Reducer<Text, Text, Text, Text>
{
    private static long [] summary = new long[8];

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
        for (Text value: values)
        {
//            String [] byParts = value.toString().split()
        }
    }
}
