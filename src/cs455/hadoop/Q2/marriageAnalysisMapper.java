/*
 * Created by Saurabh on 4/2/2017.
 */

package cs455.hadoop.Q2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class marriageAnalysisMapper extends Mapper<LongWritable, Text, Text, Text>
{
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException
    {
        String line = value.toString();
        int partNo = Integer.parseInt(line.substring(24, 27));
        if (partNo == 1)
        {
            int lineSummary = Integer.parseInt(line.substring(10, 12));
            if(lineSummary == 100)
            {
                String state = line.substring(8, 9);
                String Male = line.substring(363, 371);
                String Female = line.substring(372, 380);
                String unmarriedMale = line.substring(4422, 4430);
                String unmarriedFemale = line.substring(4467, 4475);

                context.write(new Text(state), new Text(Male + ":" + Female + ":" +
                                    unmarriedMale + ":" + unmarriedFemale));
            }
        }
    }
}
