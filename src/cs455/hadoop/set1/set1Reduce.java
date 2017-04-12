/*
 * Created by Saurabh on 4/11/2017.
 */
package cs455.hadoop.set1;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class set1Reduce extends Reducer<Text, Text, Text, Text>
{
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {

    }

}
