/*
 * Created by Saurabh on 3/31/2017.
 */

package cs455.hadoop.Q6;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class rentMedianMapper extends Mapper<LongWritable, Text, Text, Text>
{

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException
    {
        String line = value.toString();
        int partNo = Integer.parseInt(line.substring(24, 27));
        if (partNo == 2)
        {
            int lineSummary = Integer.parseInt(line.substring(10, 12));
            if(lineSummary == 100)
            {
                String state = line.substring(8, 9);

                String summaryString = toString(line);

                context.write(new Text(state), new Text(summaryString));

//                String vLT15K = line.substring(2928, 2936);
//                String v15k = line.substring(2937, 2945);
//                String v20k = line.substring(2946, 2954);
//                String v25k = line.substring(2955, 2963);
//                String v30k = line.substring(2964, 2972);
//                String v35k = line.substring(2973, 2981);
//                String v40k = line.substring(2982, 2990);
//                String v45k = line.substring(2991, 2999);
//                String v50k = line.substring(3000, 3008 );
//                String v60k = line.substring(3009, 3017);
//                String v75k = line.substring(3018, 3026);
//                String v100k = line.substring(3027, 3035);
//                String v125k = line.substring(3036, 3044);
//                String v150k = line.substring(3045, 3053);
//                String v175k = line.substring(3054, 3062);
//                String v200k = line.substring(3063, 3071);
//                String v250k = line.substring(3072, 3080);
//                String v300k = line.substring(3081, 3089);
//                String v400k = line.substring(3090, 3098);
//                String v500k = line.substring(3099, 3107);
            }
        }
    }

    private static String toString(String line)
    {
        String bigString = "";

        bigString = bigString + line.substring(3450, 3458) + ":";
        bigString = bigString + line.substring(3459, 3467) + ":";
        bigString = bigString + line.substring(3468, 3476) + ":";
        bigString = bigString + line.substring(3477, 3485) + ":";
        bigString = bigString + line.substring(3486, 3494) + ":";
        bigString = bigString + line.substring(3495, 3503) + ":";
        bigString = bigString + line.substring(3504, 3512) + ":";
        bigString = bigString + line.substring(3513, 3521) + ":";
        bigString = bigString + line.substring(3522, 3530) + ":";
        bigString = bigString + line.substring(3531, 3539) + ":";
        bigString = bigString + line.substring(3540, 3548) + ":";
        bigString = bigString + line.substring(3549, 3557) + ":";
        bigString = bigString + line.substring(3558, 3566) + ":";
        bigString = bigString + line.substring(3567, 3575) + ":";
        bigString = bigString + line.substring(3576, 3584) + ":";
        bigString = bigString + line.substring(3585, 3593) + ":";
        bigString = bigString + line.substring(3594, 3602);

        return bigString;
    }

}
