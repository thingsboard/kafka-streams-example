package kafka;

import lombok.Data;

@Data
public class JoinedModule extends AggregationPerSolarModule {
    private AggregationPerSolarPanel aggregationPerSolarPanel;

    public JoinedModule(AggregationPerSolarModule aggregationPerSolarModule,
                        AggregationPerSolarPanel aggregationPerSolarPanel) {
        of(aggregationPerSolarModule);
        this.aggregationPerSolarPanel = aggregationPerSolarPanel;
    }
}
