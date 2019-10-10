package kafka;

import lombok.Data;

@Data
public class AggregationJoiner extends Aggregation {

    private AggregationPerSolarPanel panel;

    public AggregationJoiner updateFrom(Aggregation aggregation, AggregationPerSolarPanel panel) {
        this.panel = panel;

        setCount(aggregation.getCount());
        setAvgPower(aggregation.getAvgPower());
        setSum(aggregation.getSum());
        return this;
    }
}
