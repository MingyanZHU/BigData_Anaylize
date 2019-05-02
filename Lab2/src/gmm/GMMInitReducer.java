package gmm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GMMInitReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        int k = configuration.getInt("gmm.num.mix", 8);
//        int featuresNumber = configuration.getInt("gmm.features.number", 69);
        // output order: pi, mu, sigma. delimited by tab
        double[][] sigma = new double[k][k];
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < k; j++) {
                if (i == j)
                    sigma[i][j] = 1;
                else
                    sigma[i][j] = 0;
            }
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < k; j++) {
                sb.append(sigma[i][j]);
                if (j != k - 1)
                    sb.append(",");
            }
            if (i != k - 1)
                sb.append(";");
        }
        int count = 0;
        for (Text text : values) {
            String mu = text.toString();
            mu = mu.substring(mu.indexOf(",") + 1);
            context.write(new IntWritable(count++), new Text(1.0 / k + "\t" + mu + "\t" + sb.toString()));
        }
    }
}
