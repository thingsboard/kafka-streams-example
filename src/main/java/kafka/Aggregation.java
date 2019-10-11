package kafka;

import lombok.Data;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Data
public class Aggregation {
    private int count;
    private double sum;
    private double avgPower;
    private double variance;
    private double deviance;
    private double squaresSum;
    private double normalZ;


    public Aggregation updateAvg(AggregationPerSolarPanel panel) {
        count++;
        sum += panel.getSumPower();
        avgPower = BigDecimal.valueOf(sum / count).setScale(1, RoundingMode.HALF_UP).doubleValue();
        return this;
    }

    public Aggregation updateDeviance(AggregationJoiner joiner) {
        avgPower = joiner.getAvgPower();
        if (sum < joiner.getSum()) {
            squaresSum = 0;
        }
        sum = joiner.getSum();

        count = joiner.getCount();

        squaresSum += Math.pow(joiner.getPanel().getSumPower() - avgPower, 2);

        variance = squaresSum / count;

        deviance = BigDecimal.valueOf(Math.sqrt(variance))
                .setScale(1, RoundingMode.HALF_UP).doubleValue();
        return this;
    }
}
