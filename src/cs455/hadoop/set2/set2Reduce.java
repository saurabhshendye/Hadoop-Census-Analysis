/*
 * Created by Saurabh on 4/12/2017.
 */

package cs455.hadoop.set2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;


public class set2Reduce extends Reducer<Text, Text, Text, Text>
{
    private static long [] q8summary = new long[2];
    private static double q8percent = 0.0d;
//    private static double q8percent;
    private static String q8state;

    private static long [] q7summary = new long[9];
    private static ArrayList<String> q7array = new ArrayList<String>();

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
        zeroInitialization();
        for (Text value: values)
        {
            String [] byParts = value.toString().split("/");
            if (byParts[0].equals("part-1"))
            {
                String [] parts = byParts[1].split(":");
                q8addToArray(parts);
            }
            else if (byParts[0].equals("part-2"))
            {
                String [] parts = byParts[1].split(":");
                q7addToArray(parts);
            }
        }

        if (q8percent < q8findPercent())
        {
            q8percent = q8findPercent();
            q8state = key.toString();
        }

        long total = q7summarySum();
        long count = q7findRoomCount();

        double averageRooms = count * 1.0d /total;
        q7array.add(Double.toString(averageRooms));

    }

//--------------Q8 Methods--------------//
    private static void zeroInitialization()
    {
        for (int i = 0; i < q8summary.length; i++)
        {
            q8summary[i] = 0;
        }

        for (int i = 0; i < q7summary.length; i++)
        {
            q7summary[i] = 0;
        }
    }

    private static void q8addToArray(String [] parts)
    {
        for (int i = 0; i < q8summary.length; i++)
        {
            q8summary[i] = q8summary[i] + Long.parseLong(parts[i]);
        }
    }

    private static double q8findPercent()
    {
//        float percent = summary[1]/(summary[0] + summary[1]);
        return q8summary[1] * 100.0d/(q8summary[0]);
    }

//--------------Q8 Methods end --------------//

//--------------Q7 Methods--------------//
    private void q7addToArray(String [] parts)
    {
        for (int i = 0; i < q7summary.length; i++)
        {
            q7summary[i] = q7summary[i] + Long.parseLong(parts[i]);
        }
    }

    private static long q7summarySum()
    {
        long sum = 0;
        for (long value: q7summary)
        {
            sum = sum + value;
        }

        return sum;
    }

    private static long q7findRoomCount()
    {
//        long [] count = new long[9];
        long count = 0;
        for (int i = 0; i < q7summary.length; i++)
        {
//            count[i] = summary[i] * (i+1);
            count = count + (q7summary[i] * (i+1));
        }

        return count;
    }


    public void cleanup(Context context) throws IOException, InterruptedException
    {
//        long total = q7summarySum();
//        long count = q7findRoomCount();
//
//        double averageRooms = count * 1.0d /total;
//
//        double percentile = 0.95d * averageRooms;

        double [] states = new double[q7array.size()];

        for (int i = 0; i < states.length; i++)
        {
            states[i] = Double.parseDouble(q7array.get(i));
        }

        Arrays.sort(states);
        double percentile = states[48];

        context.write(new Text("Question 7 and 8"), new Text("Question-7: " +
                                                        Double.toString(percentile) + "\n"
                                                        + "Question-8: " + q8state + " : " + q8percent ));
    }
}
