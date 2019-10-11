package kafka;

import lombok.Data;

@Data
public class PanelJoiner extends AggregationPerSolarPanel {
    private Aggregation aggregation;

    public PanelJoiner updateFrom(AggregationPerSolarPanel panel, Aggregation aggregation) {
        this.aggregation = aggregation;

        setAvgPower(panel.getAvgPower());
        setCount(panel.getCount());
        setPanelName(panel.getPanelName());
        setSumPower(panel.getSumPower());
        return this;
    }

    public boolean check() {
        double currentZ = Math.abs(getSumPower() - aggregation.getAvgPower()) / aggregation.getDeviance();
        return currentZ <= aggregation.getNormalZ();
    }
}
