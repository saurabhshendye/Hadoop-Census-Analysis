/*
 * Created by Saurabh on 4/12/2017.
 */

package cs455.hadoop.Q9;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class q9Map extends Mapper<LongWritable, Text, Text, Text>
{
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException
    {
        String line = value.toString();
        int partNo = Integer.parseInt(line.substring(24, 28));

        if (partNo == 1)
        {
            int lineSummary = Integer.parseInt(line.substring(10, 13));
            if(lineSummary == 100)
            {
                // Gender Percentage
                String state = line.substring(8, 10);
                String male = line.substring(363, 372);
                String female = line.substring(372, 381);

                // Population
                String persons = line.substring(300, 309);
                String hispanicMale = hispanicMale(line);
                String hispanicFemale = hispanicFeMale(line);

                // Median Age
                String ageSummary = toString(line);

                context.write(new Text(state), new Text(male + ":" + female +
                                                        "/" + persons + "/" + hispanicMale
                                                        + ":" + hispanicFemale +  "/" + ageSummary));

            }
        }

    }

    private static String toString(String line)
    {
        String bigString = "";

        int num = 795;
        while(num < 1057)
        {
            bigString = bigString + line.substring(num, (num+9)) + ":";
            num += 9;
        }

        bigString = bigString + line.substring(1065, 1074);

        return bigString;
    }

    private static String hispanicMale(String line)
    {
        long total = 0;

        int num = 3864;
        while (num < 4135)
        {
            total = total + Long.parseLong(line.substring(num, num + 9));
            num += 9;
        }

        return Long.toString(total);

    }

    private static String hispanicFeMale(String line)
    {
        long total = 0;

        int num = 4143;
        while (num < 4414)
        {
            total = total + Long.parseLong(line.substring(num, num + 9));
            num += 9;
        }

        return Long.toString(total);
    }
}
