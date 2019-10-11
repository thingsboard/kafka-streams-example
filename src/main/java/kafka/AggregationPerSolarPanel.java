package kafka;

import lombok.Data;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Data
public class AggregationPerSolarPanel {

    private String panelName;
    private int count;
    private double sumPower;
    private double avgPower;

    private double squaresSum;
    private double variance;
    private double deviance;

    public AggregationPerSolarPanel updateFrom(AggregationPerSolarModule data) {
        this.panelName = data.getPanelName();

        count++;
        sumPower += data.getSumPower();
        avgPower = BigDecimal.valueOf(sumPower / count)
                .setScale(1, RoundingMode.HALF_UP).doubleValue();
        return this;
    }

    public AggregationPerSolarPanel updateFrom(JoinedPanel joinedPanel) {
        panelName = joinedPanel.getPanelName();
        sumPower = joinedPanel.getSumPower();
        avgPower = joinedPanel.getAvgPower();

        count = joinedPanel.getCount();

        squaresSum += Math.pow(joinedPanel.getAggregationPerSolarModule().getSumPower() - avgPower, 2);

        variance = squaresSum / count;

        deviance = BigDecimal.valueOf(Math.sqrt(variance))
                .setScale(1, RoundingMode.HALF_UP).doubleValue();
        return this;
    }

    public AggregationPerSolarPanel of(AggregationPerSolarPanel aggregationPerSolarPanel) {
        panelName = aggregationPerSolarPanel.getPanelName();
        count = aggregationPerSolarPanel.getCount();
        sumPower = aggregationPerSolarPanel.getSumPower();
        avgPower = aggregationPerSolarPanel.getAvgPower();
        return this;
    }
}
