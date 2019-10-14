package kafka;

import lombok.Data;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Data
public class SolarPanelAggregator {

    private String panelName;
    private int count;
    private double sumPower;
    private double avgPower;

    private double squaresSum;
    private double variance;
    private double deviance;

    public SolarPanelAggregator updateFrom(SolarModuleAggregator data) {
        this.panelName = data.getPanelName();

        count++;
        sumPower += data.getSumPower();
        avgPower = BigDecimal.valueOf(sumPower / count)
                .setScale(1, RoundingMode.HALF_UP).doubleValue();
        return this;
    }

    public SolarPanelAggregator updateFrom(SolarPanelAggregatorJoiner aggregationPanelJoiner) {
        panelName = aggregationPanelJoiner.getPanelName();
        sumPower = aggregationPanelJoiner.getSumPower();
        avgPower = aggregationPanelJoiner.getAvgPower();

        count = aggregationPanelJoiner.getCount();

        squaresSum += Math.pow(aggregationPanelJoiner.getSolarModuleAggregator().getSumPower() - avgPower, 2);

        variance = squaresSum / count;

        deviance = BigDecimal.valueOf(Math.sqrt(variance))
                .setScale(1, RoundingMode.HALF_UP).doubleValue();
        return this;
    }

    public SolarPanelAggregator of(SolarPanelAggregator solarPanelAggregator) {
        panelName = solarPanelAggregator.getPanelName();
        count = solarPanelAggregator.getCount();
        sumPower = solarPanelAggregator.getSumPower();
        avgPower = solarPanelAggregator.getAvgPower();
        return this;
    }
}
