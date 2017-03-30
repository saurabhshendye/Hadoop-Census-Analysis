package cs455.hadoop.Q4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Saurabh on 3/30/2017.
 */

public class urbanVsRuralReducer extends Reducer<Text, Text, Text, Text>
{
    public void reduce (Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
    {
        for (Text value: values)
        {

        }
    }

}
