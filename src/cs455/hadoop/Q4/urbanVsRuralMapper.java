/*
 * Created by Saurabh on 3/30/2017.
 */

package cs455.hadoop.Q4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class urbanVsRuralMapper extends Mapper<LongWritable, Text, Text, Text>
{

    public void map (LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        String line = value.toString();
        int partNo = Integer.parseInt(line.substring(24, 28));
        if (partNo == 2)
        {
            int lineSummary = Integer.parseInt(line.substring(10, 13));
            if(lineSummary == 100)
            {
                String state = line.substring(8, 10);

                String urbanInUrban = line.substring(1821, 1830);
                String urbanInNonUrban = line.substring(1830, 1839);
                long totalUrban = Long.parseLong(urbanInUrban) + Long.parseLong(urbanInNonUrban);
                String urban = Long.toString(totalUrban);

                String rural = line.substring(1839, 1848);

                context.write(new Text(state), new Text(urban + ":" +rural));
            }
        }

    }

}
