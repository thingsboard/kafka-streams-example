package kafka;

import lombok.Data;

@Data
public class SolarModuleData {

    private double power;
    private String name;
    private String panel;
}