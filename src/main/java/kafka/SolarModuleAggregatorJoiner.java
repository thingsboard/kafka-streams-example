package kafka;

import lombok.Data;

@Data
public class SolarModuleAggregatorJoiner extends SolarModuleAggregator {
    private SolarPanelAggregator solarPanelAggregator;

    public SolarModuleAggregatorJoiner(SolarModuleAggregator solarModuleAggregator,
                                       SolarPanelAggregator solarPanelAggregator) {
        of(solarModuleAggregator);
        this.solarPanelAggregator = solarPanelAggregator;
    }
}
