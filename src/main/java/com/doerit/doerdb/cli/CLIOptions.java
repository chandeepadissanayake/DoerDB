package com.doerit.doerdb.cli;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class CLIOptions {

    private static boolean BOOL_OPTION_HAS_FLAGS = true;
    private static boolean BOOL_OPTION_HAS_NO_FLAGS = false;

    public static String NAME_OPTION_CONVERT_DB = "convert";
    public static String NAME_OPTION_LOCAL_DB_HOST = "localDBHost";
    public static String NAME_OPTION_LOCAL_DB_PORT = "localDBPort";
    public static String NAME_OPTION_LOCAL_DB_DB_NAME = "localDBName";
    public static String NAME_OPTION_LOCAL_DB_USERNAME = "localDBUsername";
    public static String NAME_OPTION_LOCAL_DB_PASSWORD = "localDBPassword";
    public static String NAME_OPTION_REMOTE_DB_HOST = "remoteDBHost";
    public static String NAME_OPTION_REMOTE_DB_PORT = "remoteDBPort";
    public static String NAME_OPTION_REMOTE_DB_DB_NAME = "remoteDBName";
    public static String NAME_OPTION_REMOTE_DB_USERNAME = "remoteDBUsername";
    public static String NAME_OPTION_REMOTE_DB_PASSWORD = "remoteDBPassword";

    public static Option OPTION_CONVERT_DB = new Option(NAME_OPTION_CONVERT_DB, NAME_OPTION_CONVERT_DB, BOOL_OPTION_HAS_NO_FLAGS,"Convert a pair of databases into a DoerDB.");
    public static Option OPTION_LOCAL_DB_HOST = new Option(NAME_OPTION_LOCAL_DB_HOST, NAME_OPTION_LOCAL_DB_HOST, BOOL_OPTION_HAS_FLAGS,"Provides the host for the local database.");
    public static Option OPTION_LOCAL_DB_PORT = new Option(NAME_OPTION_LOCAL_DB_PORT, NAME_OPTION_LOCAL_DB_PORT, BOOL_OPTION_HAS_FLAGS,"Provides the port for the local database.");
    public static Option OPTION_LOCAL_DB_DB_NAME = new Option(NAME_OPTION_LOCAL_DB_DB_NAME, NAME_OPTION_LOCAL_DB_DB_NAME, BOOL_OPTION_HAS_FLAGS,"Provides the database name for the local database.");
    public static Option OPTION_LOCAL_DB_USERNAME = new Option(NAME_OPTION_LOCAL_DB_USERNAME, NAME_OPTION_LOCAL_DB_USERNAME, BOOL_OPTION_HAS_FLAGS,"Provides the username of the user for the local database.");
    public static Option OPTION_LOCAL_DB_PASSWORD = new Option(NAME_OPTION_LOCAL_DB_PASSWORD, NAME_OPTION_LOCAL_DB_PASSWORD, BOOL_OPTION_HAS_FLAGS,"Provides the password of the user for the local database.");
    public static Option OPTION_REMOTE_DB_HOST = new Option(NAME_OPTION_REMOTE_DB_HOST, NAME_OPTION_REMOTE_DB_HOST, BOOL_OPTION_HAS_FLAGS,"Provides the host for the remote database.");
    public static Option OPTION_REMOTE_DB_PORT = new Option(NAME_OPTION_REMOTE_DB_PORT, NAME_OPTION_REMOTE_DB_PORT, BOOL_OPTION_HAS_FLAGS,"Provides the port for the remote database.");
    public static Option OPTION_REMOTE_DB_DB_NAME = new Option(NAME_OPTION_REMOTE_DB_DB_NAME, NAME_OPTION_REMOTE_DB_DB_NAME, BOOL_OPTION_HAS_FLAGS,"Provides the database name for the remote database.");
    public static Option OPTION_REMOTE_DB_USERNAME = new Option(NAME_OPTION_REMOTE_DB_USERNAME, NAME_OPTION_REMOTE_DB_USERNAME, BOOL_OPTION_HAS_FLAGS,"Provides the username of the user for the remote database.");
    public static Option OPTION_REMOTE_DB_PASSWORD = new Option(NAME_OPTION_REMOTE_DB_PASSWORD, NAME_OPTION_REMOTE_DB_PASSWORD, BOOL_OPTION_HAS_FLAGS,"Provides the password of the user for the remote database.");
    private final Options cliOptions;

    public CLIOptions() {
        cliOptions = new Options();
        cliOptions.addOption(OPTION_CONVERT_DB);

        cliOptions.addOption(OPTION_LOCAL_DB_HOST);
        cliOptions.addOption(OPTION_LOCAL_DB_PORT);
        cliOptions.addOption(OPTION_LOCAL_DB_DB_NAME);
        cliOptions.addOption(OPTION_LOCAL_DB_USERNAME);
        cliOptions.addOption(OPTION_LOCAL_DB_PASSWORD);

        cliOptions.addOption(OPTION_REMOTE_DB_HOST);
        cliOptions.addOption(OPTION_REMOTE_DB_PORT);
        cliOptions.addOption(OPTION_REMOTE_DB_DB_NAME);
        cliOptions.addOption(OPTION_REMOTE_DB_USERNAME);
        cliOptions.addOption(OPTION_REMOTE_DB_PASSWORD);
    }

    public Options getCliOptions() {
        return cliOptions;
    }

}