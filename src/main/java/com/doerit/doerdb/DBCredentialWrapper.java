package com.doerit.doerdb;

/**
 * Wraps the Credentials for a DoerDatabase.
 */
public class DBCredentialWrapper {

    public final String hostURL;
    public final int hostPort;
    public final String dbName;
    public final String hostUsername;
    public final String hostPassword;

    /**
     * Constructor for CredentialsWrapper.
     * @param hostURL The Host IP of the MySQL server.
     * @param hostPort The Port used by MySQL server.
     * @param dbName Name of the database to connect.
     * @param hostUsername Username to connect to MySQL server.
     * @param hostPassword Password to connect to MySQL server.
     */
    public DBCredentialWrapper(String hostURL, int hostPort, String dbName, String hostUsername, String hostPassword) {
        this.hostURL = hostURL;
        this.hostPort = hostPort;
        this.dbName = dbName;
        this.hostUsername = hostUsername;
        this.hostPassword = hostPassword;
    }

}
