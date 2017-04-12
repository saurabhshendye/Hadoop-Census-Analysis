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
    private static long [] q3summary = new long[8];

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
        // for Q1
        long owned = 0;
        long rented = 0;

        // for Question 2 and Question 3
        zeroInitialization();

        // for Question 4
        long urban = 0;
        long rural = 0;

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
}
