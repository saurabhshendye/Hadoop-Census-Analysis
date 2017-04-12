/*
 * Created by Saurabh on 4/12/2017.
 */

package cs455.hadoop.Q9;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;


public class q9Reduce extends Reducer<Text, Text, Text, Text>
{
    private static long [] maleFemale = new long[2];
    private static long [] hispMaleFemale = new long[2];
    private static long [] age = new long[31];
    private static long persons;
    private static HashMap<String, String> valueMap = new HashMap<String, String>();


    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
        initialize();
        setupHashmap();
        for (Text value: values)
        {
            String strValue = value.toString();
            String [] byParts = strValue.split("/");
            assignValues(byParts);
        }

        double femalePercent = maleFemale[1] *100.0d/(maleFemale[0] + maleFemale[1]);
        double malePercent = maleFemale[0] *100.0d/(maleFemale[0] + maleFemale[1]);

        double hispMale = hispMaleFemale[0] *100.0d/(hispMaleFemale[0] + hispMaleFemale[1]);
        double hispFemale = hispMaleFemale[1] *100.0d/(hispMaleFemale[0] + hispMaleFemale[1]);
        double hispPercent = (hispMaleFemale[0] + hispMaleFemale[1]) *100.0d/persons;

        long total = totalForCurrentKey();
        long median = findMedian(total);
        String index = findRange(median);

        String range = valueMap.get(index);

        context.write(new Text(key), new Text("\nState Population: " + Long.toString(persons) + "\n"
                                        + "State Gender Distribution: "  + "Female: " + femalePercent + " "
                                        + "Male: " + malePercent + "\n"
                                        + "State Hispanic Gender Distribution: " + "Hispanic Female: " + hispFemale
                                        + " " + "Hispanic Male: " + hispMale + "\n"
                                        + "Percent of Hispanic People: " + hispPercent + "\n"
                                        + "Median Age: " + range));



    }

    private static void setupHashmap()
    {
        valueMap.put("0", "Under 1 year");
        valueMap.put("1", "1 and 2 years");
        valueMap.put("2", "3 and 4 years");
        valueMap.put("3", "5 years");
        valueMap.put("4", "6 years");
        valueMap.put("5", "7 to 9 years");
        valueMap.put("6", "10 and 11 years");
        valueMap.put("7", "12 and 13 years");
        valueMap.put("8", "14 years");
        valueMap.put("9", "15 years");
        valueMap.put("10", "16 years");
        valueMap.put("11", "17 years");
        valueMap.put("12", "18 years");
        valueMap.put("13", "19 years");
        valueMap.put("14", "20 years");
        valueMap.put("15", "21 years");
        valueMap.put("16", "22 to 24 years");
        valueMap.put("17", "25 to 29 years");
        valueMap.put("18", "30 to 34 years");
        valueMap.put("19", "35 to 39 years");
        valueMap.put("20", "40 to 44 years");
        valueMap.put("21", "45 to 49 years");
        valueMap.put("22", "50 to 54 years");
        valueMap.put("23", "55 tot 59 years");
        valueMap.put("24", "60 and 61 years");
        valueMap.put("25", "62 to 64 years");
        valueMap.put("26", "65 to 69 years");
        valueMap.put("27", "70 to 74 years");
        valueMap.put("28", "75 to 79 years");
        valueMap.put("29", "80 to 84 years");
        valueMap.put("30", "85 years and over");

    }

    private static String findRange(long median)
    {
        long sum = 0;
        for (int i = 0; i<age.length; i++)
        {
            if (sum< median)
            {
                sum = sum + age[i];
            }
            else
            {
                return Integer.toString(i);
            }

        }
        return Integer.toString(age.length -1);
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
        for (long aSummary : age) {
            total = total + aSummary;
        }

        return total;
    }

    private static void initialize()
    {
        for (int i = 0; i < maleFemale.length; i++)
        {
            maleFemale[i] = 0;
        }
        for (int i = 0; i < age.length; i++)
        {
            age[i] = 0;
        }
        for (int i = 0; i < hispMaleFemale.length; i++)
        {
            hispMaleFemale[i] = 0;
        }

        persons = 0;
    }

    private static void assignValues(String [] parts)
    {
        String [] mf = parts[0].split(":");
        String [] hispMf = parts[2].split(":");
        String [] age_temp = parts[3].split(":");

        persons += Long.parseLong(parts[1]);


        for (int i = 0; i < maleFemale.length; i++)
        {
            maleFemale[i] +=  Long.parseLong(mf[i]);
        }
        for (int i = 0; i < age.length; i++)
        {
            age[i] += Long.parseLong(age_temp[i]);
        }
        for (int i = 0; i < hispMaleFemale.length; i++)
        {
            hispMaleFemale[i] += Long.parseLong(hispMf[i]);
        }
    }

}
