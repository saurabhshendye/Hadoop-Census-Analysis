/*
 * Created by Saurabh on 3/31/2017.
 */

package cs455.hadoop.Q6;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class rentMedianReducer extends Reducer<Text, Text, Text, Text>
{
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
        for (Text value: values)
        {

        }

    }

}
