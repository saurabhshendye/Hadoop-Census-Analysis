/*
 * Created by Saurabh on 3/30/2017.
 */

package cs455.hadoop.Q5;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class houseValueMedianReducer extends Reducer<Text, Text, Text, Text>
{

    public void reduce(Text key, Iterable<Text> value, Context context)
        throws IOException, InterruptedException
    {

    }

}
