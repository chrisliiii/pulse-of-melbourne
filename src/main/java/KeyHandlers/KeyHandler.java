package KeyHandlers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * The KeyHandler Parent Class reads all the key information from the relevant key file for API connectivity
 * using a java.util.Properties hashtable object
 */
public class KeyHandler {

    protected Properties keys;

    /**
     * The KeyHandler constructor reads the keys from a passed in file and stores them in a properties hashtable
     * @param keyFile The name of the key file containing all API keys
     */
    public KeyHandler(String keyFile) {

        try {
            File file = new File(keyFile);
            FileInputStream fileInput = new FileInputStream(file);
            this.keys = new Properties();
            this.keys.load(fileInput);
            fileInput.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
