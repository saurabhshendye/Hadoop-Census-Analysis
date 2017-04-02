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
    private static double percent = 0.0d;
    private static String state;

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
        zeroInitialization();
        for (Text value: values)
        {
            String strValue = value.toString();

            String [] byParts = strValue.split(":");
            addToArray(byParts);
        }


        if (percent < findPercent())
        {
            percent = findPercent();
            state = key.toString();
        }
    }

    private static double findPercent()
    {
//        float percent = summary[1]/(summary[0] + summary[1]);
        return summary[1]/(summary[0]);
    }

    private static void addToArray(String [] parts)
    {
        for (int i = 0; i < summary.length; i++)
        {
            summary[i] = summary[i] + Long.parseLong(parts[i]);
        }
    }

    private static void zeroInitialization()
    {
        for (int i = 0; i < summary.length; i++)
        {
            summary[i] = 0;
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException
    {
        context.write(new Text(state), new Text(Double.toString(percent)));
    }
}
