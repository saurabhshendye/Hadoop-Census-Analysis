/*
 * Created by Saurabh on 4/12/2017.
 */

package cs455.hadoop.Q9;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class q9Reduce extends Reducer<Text, Text, Text, Text>
{
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {

    }

}
