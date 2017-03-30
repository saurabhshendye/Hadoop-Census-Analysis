/*
 * Created by Saurabh on 3/30/2017.
 */

package cs455.hadoop.Q4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class urbanVsRuralReducer extends Reducer<Text, Text, Text, Text>
{
    public void reduce (Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
    {
        long urban = 0;
        long rural = 0;

        for (Text value: values)
        {
            String strValue = value.toString();

            String [] byParts = strValue.split(":");

            urban = urban + Long.parseLong(byParts[0]);
            rural = rural + Long.parseLong(byParts[1]);
        }

        context.write(key, new Text("Urban: " + urban + "\n" + "Rural: " + rural));
    }

}
