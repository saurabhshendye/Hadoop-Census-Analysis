package cs455.hadoop.part1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Saurabh on 4/11/2017.
 */
public class part1Map extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        int partNo = Integer.parseInt(line.substring(24, 28));

        if (partNo == 1) {
            int lineSummary = Integer.parseInt(line.substring(10, 13));
            if (lineSummary == 100) {
                String state = line.substring(8, 10);

                // Mapper for Question 2
                String Male = line.substring(363, 372);
                String Female = line.substring(372, 381);
                String unmarriedMale = line.substring(4422, 4431);
                String unmarriedFemale = line.substring(4467, 4476);

                String Q2 = Male + ":" + Female + ":" + unmarriedMale + ":" + unmarriedFemale;

//            context.write(new Text(state), new Text(Male + ":" + Female + ":" +
//                    unmarriedMale + ":" + unmarriedFemale));

                // Mapper for Question 3
                String age18Male = upToAge18Male(line);
                String age19To29Male = from19To29Male(line);
                String age30To39Male = from30To39Male(line);

                String age18FeMale = upToAge18FeMale(line);
                String age19To29FeMale = from19To29FeMale(line);
                String age30To39FeMale = from30To39FeMale(line);

                String maleCount = line.substring(363, 372);
                String femaleCount = line.substring(372, 381);

                String Q3 = age18Male + ":" + age19To29Male + ":" +
                        age30To39Male + ":" + age18FeMale + ":" + age19To29FeMale
                        + ":" + age30To39FeMale + ":" + maleCount
                        + ":" + femaleCount;

//                context.write(new Text(state), new Text(age18Male + "/" + age19To29Male + "/" +
//                        age30To39Male + "/" + age18FeMale + "/" + age19To29FeMale
//                        + "/" + age30To39FeMale + "/" + maleCount
//                        + "/" + femaleCount));

                context.write(new Text(state), new Text(Q2 + "/" + Q3));
            }
        }
    }


    private static String from30To39FeMale(String line)
    {
        long total = Long.parseLong(line.substring(4305, 4314));
        total += Long.parseLong(line.substring(4314, 4323));

        return Long.toString(total);
    }

    private static String from19To29FeMale(String line)
    {
//        String bigString = "";

        long total = Long.parseLong(line.substring(4260, 4269));
        total += Long.parseLong(line.substring(4269, 4278));
        total += Long.parseLong(line.substring(4278, 4287));
        total += Long.parseLong(line.substring(4287, 4296));
        total += Long.parseLong(line.substring(4296, 4305));

        return Long.toString(total);
    }

    private static String upToAge18FeMale(String line)
    {
//        String bigString = "";

        long total = Long.parseLong(line.substring(4143, 4152));
        total += Long.parseLong(line.substring(4152, 4161));
        total += Long.parseLong(line.substring(4161, 4170));
        total += Long.parseLong(line.substring(4170, 4179));
        total += Long.parseLong(line.substring(4179, 4188));
        total += Long.parseLong(line.substring(4188, 4197));
        total += Long.parseLong(line.substring(4197, 4206));
        total += Long.parseLong(line.substring(4206, 4215));
        total += Long.parseLong(line.substring(4215, 4224));
        total += Long.parseLong(line.substring(4224, 4233));
        total += Long.parseLong(line.substring(4233, 4242));
        total += Long.parseLong(line.substring(4242, 4251));
        total += Long.parseLong(line.substring(4251, 4260));


        return Long.toString(total);
    }


    private static String from30To39Male(String line)
    {
//        String bigString = "";

        long total = Long.parseLong(line.substring(4026, 4035));
        total += Long.parseLong(line.substring(4035, 4044));


        return Long.toString(total);
    }

    private static String from19To29Male(String line)
    {
//        String bigString = "";

        long total = Long.parseLong(line.substring(3981, 3990));
        total += Long.parseLong(line.substring(3990, 3999));
        total += Long.parseLong(line.substring(3999, 4008));
        total += Long.parseLong(line.substring(4008, 4017));
        total += Long.parseLong(line.substring(4017, 4026));


        return Long.toString(total);
    }

    private static String upToAge18Male(String line)
    {
//        String bigString = "";

        long total = Long.parseLong(line.substring(3864, 3873));
        total += Long.parseLong(line.substring(3873, 3882));
        total += Long.parseLong(line.substring(3882, 3891));
        total += Long.parseLong(line.substring(3891, 3900));
        total += Long.parseLong(line.substring(3900, 3909));
        total += Long.parseLong(line.substring(3909, 3918));
        total += Long.parseLong(line.substring(3918, 3927));
        total += Long.parseLong(line.substring(3927, 3936));
        total += Long.parseLong(line.substring(3936, 3945));
        total += Long.parseLong(line.substring(3945, 3954));
        total += Long.parseLong(line.substring(3954, 3963));
        total += Long.parseLong(line.substring(3963, 3972));
        total += Long.parseLong(line.substring(3972, 3981));


        return Long.toString(total);
    }
}
