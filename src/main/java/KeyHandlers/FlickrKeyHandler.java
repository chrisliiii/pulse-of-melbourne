package KeyHandlers;

/**
 * The FlickrKeyHandler Child Class obtains the necessary Flickr API keys from a file
 */
public class FlickrKeyHandler extends KeyHandler {

    private String flickrApiKey;
    private String flickrSecret;

    /**
     * The FlickrKeyHandler constructor sets the relevant API keys
     * @param keyFile the name of the file containing all keys
     */
    public FlickrKeyHandler(String keyFile) {
        super(keyFile);
        this.flickrApiKey = keys.getProperty("flickr.apikey");
        this.flickrSecret = keys.getProperty("flickr.secret");
    }

    public String getFlickrApiKey() {
        return flickrApiKey;
    }

    public String getFlickrSecret() {
        return flickrSecret;
    }

}
