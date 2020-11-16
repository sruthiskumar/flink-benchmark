package util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigurationUtil {
    public static Properties loadProperties(String file) {
        Properties properties = new Properties();
       // String file = "config.properties";
        InputStream inputStream = ConfigurationUtil.class.getClassLoader().getResourceAsStream(file);
        if(inputStream != null){
            try {
                properties.load(inputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return properties;
    }
}
