/*
 * Created by Saurabh on 3/31/2017.
 */

package cs455.hadoop.Q8;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class oldPeopleCountReducer extends Reducer<Text, Text, Text, Text>
{
    private static long [] summary = new long[2];

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
        zeroInitialization();
        for (Text value: values)
        {
            String strValue = value.toString();

            String [] byParts = strValue.split(":");

        }
    }

    private static void zeroInitialization()
    {
        for (int i = 0; i < summary.length; i++)
        {
            summary[i] = 0;
        }
    }

    public void cleanup(Context context)
    {

    }



}
