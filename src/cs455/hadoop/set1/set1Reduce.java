/*
 * Created by Saurabh on 4/11/2017.
 */
package cs455.hadoop.set1;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;

public class set1Reduce extends Reducer<Text, Text, Text, Text>
{
    private static long [] q2Summary = new long[4];
    private static long [] q3summary = new long[8];
    private static long [] q5summary = new long[20];
    private static HashMap<String, String> q5valueMap = new HashMap<String, String>();


    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
        // for Q1
        long owned = 0;
        long rented = 0;

        // for Question 2 , Question 3 and Question 5
        zeroInitialization();

        // for Question 4
        long urban = 0;
        long rural = 0;

        // for Question 5
        q5setupHashMap();

        for (Text value: values)
        {
            String strValue = value.toString();
            String [] byParts = strValue.split("/");

            if (byParts[0].equals("part-1"))
            {
                // Question 1
                String [] Q1 = byParts[1].split(":");

                owned = owned + Long.parseLong(Q1[0]);
                rented = rented + Long.parseLong(Q1[1]);

                // Question 4
                String [] Q4 = byParts[2].split(":");

                urban = urban + Long.parseLong(Q4[0]);
                rural = rural + Long.parseLong(Q4[1]);

                // Question 5
                String [] Q5 = byParts[3].split(":");
                q5addAllValues(Q5);

            }
            else if (byParts[0].equals("part-2"))
            {
                String [] Q2 = byParts[1].split(":");
                q2AddToArray(Q2);

                String [] Q3 = byParts[2].split(":");
                q3addToArray(Q3);

            }
        }

        // Question 1
        double rentPercent = rented * 100d/(owned + rented);
        double ownedPercent = owned * 100d/(owned + rented);

        // Question 2
        String q2Results = q2findStats();

        // Question 3
        String [] q3Results = q3getResults();

        // Question 5
        long total = q5totalForCurrentKey();
        long median = q5findMedian(total);

        String index = q5findRange(median);

        String range = q5valueMap.get(index);



    }

//-----------Q2 Methods-----------//
    private static String q2findStats()
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

        for (int i = 0; i < q3summary.length; i++)
        {
            q3summary[i] = 0;
        }

        for (int i = 0; i < q5summary.length; i++)
        {
            q5summary[i] = 0;
        }
    }
//-----------Q2 Methods end-----------//

//-----------Q3 Methods-----------//
    private static void q3addToArray(String [] parts)
    {
        for (int i = 0; i < q3summary.length; i++)
        {
            q3summary[i] = q3summary[i] + Long.parseLong(parts[i]);
        }
    }

    private static String [] q3getResults()
    {
        String [] results = new String[6];

        double under18MaleD = q3summary[0] * 100.0d/q3summary[6];
        double from19to29MaleD = q3summary[1] * 100.0d /q3summary[6];
        double from30to39MaleD = q3summary[2] * 100.0d/q3summary[6];

        double under18FemaleD = q3summary[3] * 100.0d /q3summary[7];
        double from19to29FemaleD = q3summary[4] * 100.0d /q3summary[7];
        double from30to39FemaleD = q3summary[5] * 100.0d /q3summary[7];

        results[0] = Double.toString(under18MaleD);
        results[1] = Double.toString(from19to29MaleD);
        results[2] = Double.toString(from30to39MaleD);

        results[3] = Double.toString(under18FemaleD);
        results[4] = Double.toString(from19to29FemaleD);
        results[5] = Double.toString(from30to39FemaleD);

        return results;
    }
//-----------Q3 Methods end-----------//

//-----------Q5 Methods-----------//

    private static void q5addAllValues(String [] parts)
    {
        for (int i = 0; i < parts.length; i++)
        {
            long temp = Long.parseLong(parts[i]);
            q5summary[i] = q5summary[i] + temp;
        }
    }

    private static long q5totalForCurrentKey()
    {
        long total = 0;
        for (long aSummary : q5summary) {
            total = total + aSummary;
        }

        return total;
    }

    private static long q5findMedian(long total)
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

    private static String q5findRange(long median)
    {
        long sum = 0;
        for (int i = 0; i<q5summary.length; i++)
        {
            if (sum< median)
            {
                sum = sum + q5summary[i];
            }
            else
            {
                return Integer.toString(i);
            }

        }
        return Integer.toString(q5summary.length -1);
    }

    private static void q5setupHashMap()
    {
        q5valueMap.put("0", "Less than $15,000");
        q5valueMap.put("1", "$15,000 - $19,999");
        q5valueMap.put("2", "$20,000 - $24,999");
        q5valueMap.put("3", "$25,000 - $29,999");
        q5valueMap.put("4", "$30,000 - $34,999");
        q5valueMap.put("5", "$35,000 - $39,999");
        q5valueMap.put("6", "$40,000 - $44,999");
        q5valueMap.put("7", "$45,000 - $49,999");
        q5valueMap.put("8", "$50,000 - $59,999");
        q5valueMap.put("9", "$60,000 - $74,999");
        q5valueMap.put("10", "$75,000 - $99,999");
        q5valueMap.put("11", "$100,000 - $124,999");
        q5valueMap.put("12", "$125,000 - $149,999");
        q5valueMap.put("13", "$150,000 - $174,999");
        q5valueMap.put("14", "$175,000 - $199,999");
        q5valueMap.put("15", "$200,000 - $249,999");
        q5valueMap.put("16", "$250,000 - $299,999");
        q5valueMap.put("17", "$300,000 - $399,999");
        q5valueMap.put("18", "$400,000 - $499,999");
        q5valueMap.put("19", "$500,000 or more");
    }

}
