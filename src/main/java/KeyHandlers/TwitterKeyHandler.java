package KeyHandlers;

import java.io.IOException;

/**
 * The TwitterKeyHandler Child Class obtains the necessary Twitter API keys from a file
 */
public class TwitterKeyHandler extends KeyHandler {

    private String twitterConsumerKey;
    private String twitterConsumerSecret;
    private String twitterAccessTokenKey;
    private String twitterAccessTokenSecret;

    /**
     * The TwitterKeyHandler constructor sets the relevant API keys
     * @param keyFile the name of the file containing all keys
     * @param city the city location of the twitter stream query. Multiple keys are available
     *             to avoid rate limits and choice depends on city name;
     */
    public TwitterKeyHandler(String keyFile, String city) {

        super(keyFile);

        int cityNum;
        if (city.equals("melbourne")) cityNum = 1;
        else cityNum = 2;
        this.twitterConsumerKey = keys.getProperty("twitter.consumerkey" + Integer.toString(cityNum));
        this.twitterConsumerSecret = keys.getProperty("twitter.consumersecret" + Integer.toString(cityNum));
        this.twitterAccessTokenKey = keys.getProperty("twitter.accesstokenkey" + Integer.toString(cityNum));
        this.twitterAccessTokenSecret = keys.getProperty("twitter.accesstokensecret" + Integer.toString(cityNum));

    }

    public String getTwitterConsumerKey() {
        return twitterConsumerKey;
    }

    public String getTwitterConsumerSecret() {
        return twitterConsumerSecret;
    }

    public String getTwitterAccessTokenKey() {
        return twitterAccessTokenKey;
    }

    public String getTwitterAccessTokenSecret() {
        return twitterAccessTokenSecret;
    }


}
