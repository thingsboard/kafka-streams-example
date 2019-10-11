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

    public AggregationPerSolarPanel updateFrom(AggregationPerSolarModule data) {
        this.panelName = data.getPanelName();

        count++;
        sumPower += data.getSumPower();
        avgPower = BigDecimal.valueOf(sumPower / count)
                .setScale(1, RoundingMode.HALF_UP).doubleValue();
        return this;
    }
}
