package KeyHandlers;

/**
 * The InstagramKeyHandler Child Class obtains the necessary Instagram API keys from a file
 */
public class InstagramKeyHandler extends KeyHandler{

    private String[] tokens = new String[3];
    private String[] secrets = new String[3];

    /**
     * The InstagramKeyHandler constructor sets the relevant API keys
     * @param keyFile the name of the file containing all keys
     */
    public InstagramKeyHandler(String keyFile) {
        super(keyFile);
        this.tokens[0] = keys.getProperty("instagram.token1");
        this.tokens[1] = keys.getProperty("instagram.token2");
        this.tokens[2] = keys.getProperty("instagram.token3");
        this.secrets[0] = keys.getProperty("instagram.secret1");
        this.secrets[1] = keys.getProperty("instagram.secret2");
        this.secrets[2] = keys.getProperty("instagram.secret3");
    }

    public String[] getTokens() {
        return tokens;
    }

    public String[] getSecrets() {
        return secrets;
    }
}
