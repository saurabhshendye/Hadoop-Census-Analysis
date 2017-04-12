/*
 * Created by Saurabh on 4/12/2017.
 */

package cs455.hadoop.Q9;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class q9Reduce extends Reducer<Text, Text, Text, Text>
{
    private static long [] maleFemale = new long[2];
    private static long [] hispMaleFemale = new long[2];
    private static long [] age = new long[31];
    private static long persons;


    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
        initialize();
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


        context.write(new Text(key), new Text("\nState Population: " + Long.toString(persons) + "\n"
                                        + "State Gender Distribution: "  + "Female: " + femalePercent + " "
                                        + "Male: " + malePercent + "\n"
                                        + "State Hispanic Gender Distribution: " + "Hispanic Female: " + hispFemale
                                        + " " + "Hispanic Male: " + hispMale + "\n"
                                        + "Percent of Hispanic People: " + hispPercent
                                        + "Median Age: "));

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
