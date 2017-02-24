package KeyHandlers;

/**
 * The YoutubeKeyHandler Child Class obtains the necessary Youtube API keys from a file
 */
public class YoutubeKeyHandler extends KeyHandler{

    private String youtubeApiKey;

    /**
     * The YoutubeKeyHandler constructor sets the relevant API keys
     * @param keyFile the name of the file containing all keys
     * @param city the city location of the twitter stream query. Multiple keys are available
     *             to avoid rate limits and choice depends on city name;
     */
    public YoutubeKeyHandler(String keyFile, String city) {
        super(keyFile);

        int cityNum;
        if (city.equals("melbourne")) cityNum = 1;
        else cityNum = 2;

        this.youtubeApiKey = keys.getProperty("youtube.apikey" + Integer.toString(cityNum));
    }

    public String getYoutubeApiKeyApiKey() {
        return youtubeApiKey;
    }
}
