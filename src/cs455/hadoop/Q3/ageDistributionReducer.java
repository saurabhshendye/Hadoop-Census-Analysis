/*
 * Created by Saurabh on 4/2/2017.
 */

package cs455.hadoop.Q3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ageDistributionReducer extends Reducer<Text, Text, Text, Text>
{
    private static long [] summary = new long[8];

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
        for (Text value: values)
        {
            String [] byParts = value.toString().split("/");
            addToArray(byParts);
        }

        String [] results = getResults();

        context.write(key, new Text("Under 18 Male: " + results[0]
                                    + "Males From 19 to 29: " + results[1]
                                    + "Males From 30 to 39: " + results[2]
                                    + "Under 18 Females: " + results[3]
                                    + "Females 19 to 29: " + results[4]
                                    + "Females 30 to 39: " + results[5]));

    }

    private static void addToArray(String [] parts)
    {
        for (int i = 0; i < summary.length; i++)
        {
            summary[i] = summary[i] + Long.parseLong(parts[i]);
        }
    }

    private static String [] getResults()
    {
        String [] results = new String[6];

        double under18MaleD = summary[0]/summary[6];
        double from19to29MaleD = summary[1]/summary[6];
        double from30to39MaleD = summary[2]/summary[6];

        double under18FemaleD = summary[3]/summary[7];
        double from19to29FemaleD = summary[4]/summary[7];
        double from30to39FemaleD = summary[5]/summary[7];

        results[0] = Double.toString(under18MaleD);
        results[1] = Double.toString(from19to29MaleD);
        results[2] = Double.toString(from30to39MaleD);

        results[3] = Double.toString(under18FemaleD);
        results[4] = Double.toString(from19to29FemaleD);
        results[5] = Double.toString(from30to39FemaleD);

        return results;
    }
}
