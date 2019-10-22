/**
 * Copyright © 2016-2019 The Thingsboard Authors
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.kafka;

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
