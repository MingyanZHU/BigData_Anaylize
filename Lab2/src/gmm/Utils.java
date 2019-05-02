package gmm;

import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

public class Utils {
    public static double[][] getSigmaFromString(String s) {
        String[] row = s.split(";");
        double[][] ans = new double[row.length][row.length];
        for (int i = 0; i < row.length; i++) {
            String[] columns = row[i].split(",");
            for (int j = 0; j < columns.length; j++) {
                ans[i][j] = Double.parseDouble(columns[j]);
            }
        }
        return ans;
    }

    public static double[] getMuFromString(String s) {
        String[] row = s.split(",");
        double[] ans = new double[row.length];
        for (int i = 0; i < ans.length; i++)
            ans[i] = Double.parseDouble(row[i]);
        return ans;
    }

    public static double gaussian(double[] x, double[] mu, double[][] sigma) {
        int n = x.length;
        double ans = 1.0 / Math.pow(2 * Math.PI, n / 2);
        RealMatrix cov = MatrixUtils.createRealMatrix(sigma);
        ans *= 1.0 / Math.pow(new LUDecomposition(cov).getDeterminant(), n / 2);
        RealMatrix inverse = new LUDecomposition(cov).getSolver().getInverse();
        RealVector xVector = MatrixUtils.createRealVector(x);
        RealVector muVector = MatrixUtils.createRealVector(mu);
        RealVector x_sub_mu = xVector.subtract(muVector);
        ans *= Math.exp(-0.5 * inverse.preMultiply(x_sub_mu).dotProduct(x_sub_mu));
        return ans;
    }

    public static void main(String[] args) {
        double[][] x = {{1, 2, 3}};
        RealMatrix matrix = MatrixUtils.createRealMatrix(x);
        System.out.println(matrix.transpose().multiply(matrix));
//        System.out.println(matrix.getData());
//        System.out.println(matrix);
//        RealMatrix pInverse = new LUDecomposition(matrix).getSolver().getInverse();
//        System.out.println(pInverse);
//        System.out.println(matrix.multiply(pInverse));
//
//        RealVector vector = MatrixUtils.createRealVector(new double[]{1, 2, 3});
//        System.out.println(vector.toArray());
//        double[] x = new double[]{1, 0};
//        double[] mu = new double[]{1, 1};
//        double[][] sigma = new double[][]{{1, 0}, {0, 1}};
//        System.out.println(gaussian(x, mu, sigma));
    }
}
