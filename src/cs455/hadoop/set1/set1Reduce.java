/*
 * Created by Saurabh on 4/11/2017.
 */
package cs455.hadoop.set1;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class set1Reduce extends Reducer<Text, Text, Text, Text>
{
    private static long [] q2Summary = new long[4];
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
        zeroInitialization();
        for (Text value: values)
        {
            String strValue = value.toString();
            String [] byParts = strValue.split("/");

            if (byParts[0].equals("part-1"))
            {

            }
            else if (byParts[0].equals("part-2"))
            {
                String [] Q2 = byParts[1].split(":");
                q2AddToArray(Q2);
            }
        }

        String results = findStats();

    }

    private static String findStats()
    {
        double malePercent = q2Summary[2] * 100.0d/q2Summary[0];
        double femalePercent = q2Summary[3] * 100.0d/q2Summary[1];

        String results = "Un-Married Male : " +  Double.toString(malePercent) + "\n"
                + "Un-Married Female: " + Double.toString(femalePercent);

        return results;
    }

    private static void q2AddToArray(String [] parts)
    {
        for (int i = 0; i < q2Summary.length; i++)
        {
            q2Summary[i] = q2Summary[i] + Long.parseLong(parts[i]);
        }
    }

    private static void zeroInitialization()
    {
        for (int i = 0; i < q2Summary.length; i++)
        {
            q2Summary[i] = 0;
        }
    }

}
