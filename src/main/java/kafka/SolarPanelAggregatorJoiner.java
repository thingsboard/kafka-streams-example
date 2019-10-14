package kafka;

import lombok.Data;

@Data
public class SolarPanelAggregatorJoiner extends SolarPanelAggregator {

    private SolarModuleAggregator solarModuleAggregator;

    public SolarPanelAggregatorJoiner(SolarPanelAggregator solarPanelAggregator,
                                      SolarModuleAggregator solarModuleAggregator) {
        of(solarPanelAggregator);
        this.solarModuleAggregator = solarModuleAggregator;
    }
}
