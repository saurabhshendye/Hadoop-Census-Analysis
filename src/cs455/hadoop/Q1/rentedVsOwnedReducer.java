/*
 * Created by Saurabh on 3/28/2017.
 */

package cs455.hadoop.Q1;


import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class rentedVsOwnedReducer extends Reducer<Text, Text, Text, Text>
{
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
        double rentPercent = 0.0d;
        double ownedPercent = 0.0d;

        long owned = 0;
        long rented = 0;

        for (Text T: values)
        {
            String strValue = T.toString();
            String [] byParts = strValue.split(":");

            owned = owned + Long.parseLong(byParts[0]);
            rented = rented + Long.parseLong(byParts[1]);

        }

        rentPercent = rented/(owned + rented);
        rentPercent = rentPercent * 100;

        ownedPercent = owned/(owned + rented);
        ownedPercent = ownedPercent * 100;

        context.write(key, new Text("Owned: " + ownedPercent + "\n" + "Rented: " + rentPercent));
    }


}
