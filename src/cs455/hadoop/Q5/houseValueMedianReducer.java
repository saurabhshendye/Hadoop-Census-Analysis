/*
 * Created by Saurabh on 3/30/2017.
 */

package cs455.hadoop.Q5;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;

public class houseValueMedianReducer extends Reducer<Text, Text, Text, Text>
{

    private static long [] summary = new long[20];
    private static HashMap<String, String> valueMap = new HashMap<String, String>();



    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
        setupHashmap();
        zeroInitialization();
        for (Text value : values )
        {
            String strValue = value.toString();

            String [] byParts = strValue.split(":");
            addAllValues(byParts);
        }

        long total = totalForCurrentKey();
        long median = findMedian(total);
        String index = findRange(median);

        String range = valueMap.get(index);

        context.write(key, new Text(range));
    }


    private static void zeroInitialization()
    {
        for (int i = 0; i < summary.length; i++)
        {
            summary[i] = 0;
        }
    }

    private static void addAllValues(String [] parts)
    {
        for (int i = 0; i < parts.length; i++)
        {
            long temp = Long.parseLong(parts[i]);
            summary[i] = summary[i] + temp;
        }
    }

    private static long totalForCurrentKey()
    {
        long total = 0;
        for (long aSummary : summary) {
            total = total + aSummary;
        }

        return total;
    }

    private static long findMedian(long total)
    {
        long median;
        if (total % 2 == 0)
        {
            median = total /2;
        }
        else
        {
            median = 1 + (total/2);
        }
        return median;
    }

    private static String findRange(long median)
    {
        long sum = 0;
        for (int i = 0; i<summary.length; i++)
        {
            if (sum< median)
            {
                sum = sum + summary[i];
            }
            else
            {
                return Integer.toString(i);
            }

        }
        return Integer.toString(summary.length -1);
    }

    private static void setupHashmap()
    {
        valueMap.put("0", "Less than $15,000");
        valueMap.put("1", "$15,000 - $19,999");
        valueMap.put("2", "$20,000 - $24,999");
        valueMap.put("3", "$25,000 - $29,999");
        valueMap.put("4", "$30,000 - $34,999");
        valueMap.put("5", "$35,000 - $39,999");
        valueMap.put("6", "$40,000 - $44,999");
        valueMap.put("7", "$45,000 - $49,999");
        valueMap.put("8", "$50,000 - $59,999");
        valueMap.put("9", "$60,000 - $74,999");
        valueMap.put("10", "$75,000 - $99,999");
        valueMap.put("11", "$100,000 - $124,999");
        valueMap.put("12", "$125,000 - $149,999");
        valueMap.put("13", "$150,000 - $174,999");
        valueMap.put("14", "$175,000 - $199,999");
        valueMap.put("15", "$200,000 - $249,999");
        valueMap.put("16", "$250,000 - $299,999");
        valueMap.put("17", "$300,000 - $399,999");
        valueMap.put("18", "$400,000 - $499,999");
        valueMap.put("19", "$500,000 or more");
    }

}
