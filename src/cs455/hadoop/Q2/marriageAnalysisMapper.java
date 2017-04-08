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
        int partNo = Integer.parseInt(line.substring(24, 28));
        if (partNo == 1)
        {
            int lineSummary = Integer.parseInt(line.substring(10, 13));
            if(lineSummary == 100)
            {
                String state = line.substring(8, 10);
                String Male = line.substring(363, 372);
                String Female = line.substring(372, 381);
                String unmarriedMale = line.substring(4422, 4431);
                String unmarriedFemale = line.substring(4467, 4476);

                context.write(new Text(state), new Text(Male + ":" + Female + ":" +
                                    unmarriedMale + ":" + unmarriedFemale));
            }
        }
    }
}
