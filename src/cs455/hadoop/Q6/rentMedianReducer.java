/*
 * Created by Saurabh on 3/31/2017.
 */

package cs455.hadoop.Q6;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class rentMedianReducer extends Reducer<Text, Text, Text, Text>
{
    private static HashMap<String, String> valueMap = new HashMap<String, String>();
    private static long [] summary = new long[17];

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
        setupHashmap();
        zeroInitialization();

        for (Text value: values)
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

    private static long totalForCurrentKey()
    {
        long total = 0;
        for (long aSummary : summary) {
            total = total + aSummary;
        }

        return total;
    }

    private static void addAllValues(String [] parts)
    {
        for (int i = 0; i < parts.length; i++)
        {
            long temp = Long.parseLong(parts[i]);
            summary[i] = summary[i] + temp;
        }
    }

    private static void zeroInitialization()
    {
        for (int i = 0; i < summary.length; i++)
        {
            summary[i] = 0;
        }
    }


    private static void setupHashmap()
    {
        valueMap.put("0", "Less than $100");
        valueMap.put("1", "$100 to $149");
        valueMap.put("2", "$150 to $199");
        valueMap.put("3", "$200 to $249");
        valueMap.put("4", "$250 to $299");
        valueMap.put("5", "$300 to $349");
        valueMap.put("6", "$350 to $399");
        valueMap.put("7", "$400 to $449");
        valueMap.put("8", "$450 to $499");
        valueMap.put("9", "$500 to $549");
        valueMap.put("10", "$550 to $ 599");
        valueMap.put("11", "$600 to $649");
        valueMap.put("12", "$650 to $699");
        valueMap.put("13", "$700 to $749");
        valueMap.put("14", "$750 to $999");
        valueMap.put("15", "$1000 or more");
        valueMap.put("16", "No cash rent");
    }

}
