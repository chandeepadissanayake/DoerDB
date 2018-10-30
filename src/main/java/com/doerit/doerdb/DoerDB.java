package com.doerit.doerdb;

import com.doerit.doerdb.cli.CLIProcessor;
import com.doerit.doerdb.db.DoerDatabase;
import com.doerit.doerdb.exceptions.InitializationFailureException;
import com.doerit.doerdb.exceptions.NotFoundException;
import com.doerit.doerdb.exceptions.QueryParseException;
import com.doerit.doerdb.db.types.DatabaseType;
import org.apache.commons.cli.ParseException;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * DoerDB is the base class which wraps two DoerDatabases: Local and Remote DoerDatabase.
 */
public class DoerDB {

    private final DoerDatabase doerLocalDB;
    private final DoerDatabase doerRemoteDB;

    /**
     * Constructs a DoerDB instance with CredentialWrappers.
     * @param localDBCredentials DBCredentialWrapper wrapping the credentials for Local Database.
     * @param remoteDBCredentials DBCredentialWrapper wrapping the credentials for Remote Database.
     * @throws SQLException If any exception occurs while connecting to databases.
     * @throws InitializationFailureException If database is invalid DoerDB.
     * @throws NotFoundException If Meta Table / Sync Table / Any trigger(s) are not found in any of the given databases.
     */
    public DoerDB(DBCredentialWrapper localDBCredentials, DBCredentialWrapper remoteDBCredentials) throws SQLException, InitializationFailureException, NotFoundException {
        this.doerLocalDB = new DoerDatabase(localDBCredentials.hostURL, localDBCredentials.hostPort, localDBCredentials.dbName, localDBCredentials.hostUsername, localDBCredentials.hostPassword, DatabaseType.LOCAL);
        this.doerRemoteDB = new DoerDatabase(remoteDBCredentials.hostURL, remoteDBCredentials.hostPort, remoteDBCredentials.dbName, remoteDBCredentials.hostUsername, remoteDBCredentials.hostPassword, DatabaseType.REMOTE);
    }

    /**
     * Used to obtain Local DoerDatabase.
     * @return DoerDatabase Local DoerDatabase.
     */
    public DoerDatabase getLocalDatabase() {
        return doerLocalDB;
    }

    /**
     * Used to obtain Remote DoerDatabase.
     * @return DoerDatabase Remote DoerDatabase.
     */
    public DoerDatabase getRemoteDatabase() {
        return doerRemoteDB;
    }

    /**
     * Executes a MySQL Query in Local Database.
     * @param sqlQuery The query to be executed on the databases.
     * @return java.sql.ResultSet returned after executing the query on the databases.
     * @throws SQLException If any exception occurs during the query execution.
     * @throws InitializationFailureException If DoerDatabase Implementation failed to initialize.
     */
    public ResultSet executeLocalQuery(String sqlQuery) throws SQLException, InitializationFailureException {
        return this.doerLocalDB.executeQuery(sqlQuery);
    }

    /**
     * Executes a MySQL Query in Remote Database.
     * @param sqlQuery The query to be executed on the databases.
     * @return java.sql.ResultSet returned after executing the query on the databases.
     * @throws SQLException If any exception occurs during the query execution.
     * @throws InitializationFailureException If DoerDatabase Implementation failed to initialize.
     */
    public ResultSet executeRemoteQuery(String sqlQuery) throws SQLException, InitializationFailureException {
        return this.doerRemoteDB.executeQuery(sqlQuery);
    }

    /**
     * Executes a MySQL Update type Query in Local Databases.
     * <b>Note: This is an attended query where it gets recorded in meta table.</b>
     * @param sqlQuery The update MySQL query to be executed on the databases.
     * @throws SQLException If any exception occurs during the query execution.
     * @throws InitializationFailureException If DoerDatabase Implementation failed to initialize.
     * @throws NotFoundException If a table name is not found in the MySQL query.
     * @throws QueryParseException If MySQL query is invalid.
     */
    public void executeLocalUpdate(String sqlQuery) throws QueryParseException, SQLException, NotFoundException, InitializationFailureException {
        this.doerLocalDB.executeUpdate(sqlQuery);
    }

    /**
     * Executes a MySQL Update type Query in Remote Databases.
     * <b>Note: This is an attended query where it gets recorded in meta table.</b>
     * @param sqlQuery The update MySQL query to be executed on the databases.
     * @throws SQLException If any exception occurs during the query execution.
     * @throws InitializationFailureException If DoerDatabase Implementation failed to initialize.
     * @throws NotFoundException If a table name is not found in the MySQL query.
     * @throws QueryParseException If MySQL query is invalid.
     */
    public void executeRemoteUpdate(String sqlQuery) throws QueryParseException, SQLException, NotFoundException, InitializationFailureException {
        this.doerRemoteDB.executeUpdate(sqlQuery);
    }

    /**
     * Following method handles CLI calls to the JAR.
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        try {
            CLIProcessor cliProcessor = new CLIProcessor(args);
            cliProcessor.processArgs();
        }

        catch (ParseException parseException) {
            parseException.printStackTrace();
        }
    }

}