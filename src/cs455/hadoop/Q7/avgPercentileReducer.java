/*
 * Created by Saurabh on 4/2/2017.
 */

package cs455.hadoop.Q7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class avgPercentileReducer extends Reducer <Text, Text, Text, Text>
{
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {

    }
}
