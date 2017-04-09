/*
 * Created by Saurabh on 4/2/2017.
 */

package cs455.hadoop.Q2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class marriageAnalysisReducer extends Reducer<Text, Text, Text, Text>
{
    private static long [] summary = new long[4];
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
        zeroInitialization();
        for (Text value: values)
        {
            String [] byParts = value.toString().split(":");
            addToArray(byParts);
        }

        String results = findStats();

        context.write(key, new Text(results));
    }

    private static void zeroInitialization()
    {
        for (int i = 0; i < summary.length; i++)
        {
            summary[i] = 0;
        }
    }

    private static void addToArray(String [] parts)
    {
        for (int i = 0; i < summary.length; i++)
        {
            summary[i] = summary[i] + Long.parseLong(parts[i]);
        }
    }

    private static String findStats()
    {
        double malePercent = summary[2] * 100.0d/summary[0];
        double femalePercent = summary[3] * 100.0d/summary[1];

        String results = "Un-Married Male : " +  Double.toString(malePercent) + "\n"
                        + "Un-Married Female: " + Double.toString(femalePercent);

        return results;
    }

}
