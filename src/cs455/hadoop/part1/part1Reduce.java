/*
 * Created by Saurabh on 4/11/2017.
 */

package cs455.hadoop.part1;

import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.soap.Text;
import java.io.IOException;


public class part1Reduce extends Reducer<Text, Text, Text, Text>
{
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException,InterruptedException
    {
        for (Text value: values)
        {
            String strValue = value.toString();
            String [] byParts = strValue.split("/");


        }

    }
}
