package org.thingsboard.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SolarModuleKey {

    private String panelName;
    private String moduleName;
}
