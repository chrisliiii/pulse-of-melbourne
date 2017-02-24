package KeyHandlers;

/**
 * The FoursquareKeyHandler Child Class obtains the necessary Foursquare API keys from a file
 */
public class FoursquareKeyHandler extends KeyHandler{

    private String foursquareClientID;
    private String foursquareClientSecret;
    private String foursquareCallbackUrl;

    /**
     * The FoursquareKeyHandler constructor sets the relevant API keys
     * @param keyFile the name of the file containing all keys
     */
    public FoursquareKeyHandler(String keyFile) {
        super(keyFile);
        this.foursquareClientID = keys.getProperty("foursquare.clientid");
        this.foursquareClientSecret = keys.getProperty("foursquare.clientsecret");
        this.foursquareCallbackUrl = keys.getProperty("foursquare.callbackurl");
    }

    public String getFoursquareClientID() {
        return foursquareClientID;
    }

    public String getFoursquareClientSecret() {
        return foursquareClientSecret;
    }

    public String getFoursquareCallbackUrl() {
        return foursquareCallbackUrl;
    }

}
