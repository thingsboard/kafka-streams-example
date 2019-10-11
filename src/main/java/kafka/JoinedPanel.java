package kafka;

import lombok.Data;

@Data
public class JoinedPanel extends AggregationPerSolarPanel {

    private AggregationPerSolarModule aggregationPerSolarModule;

    public JoinedPanel(AggregationPerSolarPanel aggregationPerSolarPanel,
                       AggregationPerSolarModule aggregationPerSolarModule) {
        of(aggregationPerSolarPanel);
        this.aggregationPerSolarModule = aggregationPerSolarModule;
    }
}
