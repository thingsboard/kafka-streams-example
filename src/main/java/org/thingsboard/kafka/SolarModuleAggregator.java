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
public class SolarModuleAggregator {

    private String moduleName;
    private String panelName;
    private int count;
    private double sumPower;
    private double avgPower;

    public SolarModuleAggregator updateFrom(SolarModuleData data) {
        moduleName = data.getName();
        panelName = data.getPanel();

        count++;
        sumPower += data.getPower();
        avgPower = BigDecimal.valueOf(sumPower / count)
                .setScale(1, RoundingMode.HALF_UP).doubleValue();
        return this;
    }

    public SolarModuleAggregator of(SolarModuleAggregator aggModule) {
        moduleName = aggModule.getModuleName();
        panelName = aggModule.getPanelName();
        count = aggModule.getCount();
        sumPower = aggModule.getSumPower();
        avgPower = aggModule.getAvgPower();
        return this;
    }
}
