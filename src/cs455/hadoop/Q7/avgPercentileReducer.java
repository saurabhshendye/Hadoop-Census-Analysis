/*
 * Created by Saurabh on 4/2/2017.
 */

package cs455.hadoop.Q7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class avgPercentileReducer extends Reducer <Text, Text, Text, Text>
{
    private static long [] summary = new long[9];

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
        for (Text value: values)
        {
            String [] byParts = value.toString().split(":");
            addToArray(byParts);
        }
    }

    private void addToArray(String [] parts)
    {
        for (int i = 0; i < summary.length; i++)
        {
            summary[i] = summary[i] + Long.parseLong(parts[i]);
        }
    }

    private static long summarySum()
    {
        long sum = 0;
        for (long value: summary)
        {
            sum = sum + value;
        }

        return sum;
    }

    private static long findRoomCount()
    {
//        long [] count = new long[9];
        long count = 0;
        for (int i = 0; i < summary.length; i++)
        {
//            count[i] = summary[i] * (i+1);
            count = count + (summary[i] * (i+1));
        }

        return count;
    }


    public void cleanup(Context context) throws IOException, InterruptedException
    {
        long total = summarySum();
        long count = findRoomCount();

        double averageRooms = count * 1.0d /total;

        double percentile = 0.95d * averageRooms;

        context.write(new Text("Percentile"), new Text(Double.toString(percentile)));
    }
}
