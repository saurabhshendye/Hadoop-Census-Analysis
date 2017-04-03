/*
 * Created by Saurabh on 4/2/2017.
 */

package cs455.hadoop.Q3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ageDistributionMapper extends Mapper<LongWritable, Text, Text, Text>
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

                String age18Male = upToAge18Male(line);
                String age19To29Male = from19To29Male(line);
                String age30To39Male = from30To39Male(line);

                String age18FeMale = upToAge18FeMale(line);
                String age19To29FeMale = from19To29FeMale(line);
                String age30To39FeMale = from30To39FeMale(line);

                String maleCount = line.substring(363, 371);
                String femaleCount = line.substring(372, 380);

                context.write(new Text(state), new Text(age18Male + "/" + age19To29Male + "/" +
                                                age30To39Male + "/" + age18FeMale + "/" + age19To29FeMale
                                                + "/" + age30To39FeMale + "/" + maleCount
                                                + "/" + femaleCount));
            }
        }
    }

    private static String from30To39FeMale(String line)
    {
        long total = Long.parseLong(line.substring(4305, 4313));
        total += Long.parseLong(line.substring(4314, 4322));

        return Long.toString(total);
    }

    private static String from19To29FeMale(String line)
    {
//        String bigString = "";

        long total = Long.parseLong(line.substring(4260, 4268));
        total += Long.parseLong(line.substring(4269, 4277));
        total += Long.parseLong(line.substring(4278, 4286));
        total += Long.parseLong(line.substring(4287, 4295));
        total += Long.parseLong(line.substring(4296, 4304));

        return Long.toString(total);
    }

    private static String upToAge18FeMale(String line)
    {
//        String bigString = "";

        long total = Long.parseLong(line.substring(4143, 4151));
        total += Long.parseLong(line.substring(4152, 4160));
        total += Long.parseLong(line.substring(4161, 4169));
        total += Long.parseLong(line.substring(4170, 4178));
        total += Long.parseLong(line.substring(4179, 4187));
        total += Long.parseLong(line.substring(4188, 4196));
        total += Long.parseLong(line.substring(4197, 4205));
        total += Long.parseLong(line.substring(4206, 4214));
        total += Long.parseLong(line.substring(4215, 4223));
        total += Long.parseLong(line.substring(4224, 4232));
        total += Long.parseLong(line.substring(4233, 4241));
        total += Long.parseLong(line.substring(4242, 4250));
        total += Long.parseLong(line.substring(4251, 4259));


        return Long.toString(total);
    }


    private static String from30To39Male(String line)
    {
//        String bigString = "";

        long total = Long.parseLong(line.substring(4026, 4034));
        total += Long.parseLong(line.substring(4035, 4043));


        return Long.toString(total);
    }

    private static String from19To29Male(String line)
    {
//        String bigString = "";

        long total = Long.parseLong(line.substring(3981, 3989));
        total += Long.parseLong(line.substring(3990, 3998));
        total += Long.parseLong(line.substring(3999, 4007));
        total += Long.parseLong(line.substring(4008, 4016));
        total += Long.parseLong(line.substring(4017, 4025));


        return Long.toString(total);
    }

    private static String upToAge18Male(String line)
    {
//        String bigString = "";

        long total = Long.parseLong(line.substring(3864, 3872));
        total += Long.parseLong(line.substring(3873, 3881));
        total += Long.parseLong(line.substring(3882, 3890));
        total += Long.parseLong(line.substring(3891, 3899));
        total += Long.parseLong(line.substring(3900, 3908));
        total += Long.parseLong(line.substring(3909, 3917));
        total += Long.parseLong(line.substring(3918, 3926));
        total += Long.parseLong(line.substring(3927, 3935));
        total += Long.parseLong(line.substring(3936, 3944));
        total += Long.parseLong(line.substring(3945, 3953));
        total += Long.parseLong(line.substring(3954, 3962));
        total += Long.parseLong(line.substring(3963, 3971));
        total += Long.parseLong(line.substring(3972, 3980));


        return Long.toString(total);
    }
}
