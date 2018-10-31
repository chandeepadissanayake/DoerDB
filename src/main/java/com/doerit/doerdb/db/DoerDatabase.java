package com.doerit.doerdb.db;

import com.doerit.doerdb.db.jdbc.JDBCConstants;
import com.doerit.doerdb.db.metadata.DoerDBMetaTable;
import com.doerit.doerdb.db.metadata.DoerDBSyncDataTable;
import com.doerit.doerdb.db.metadata.DoerDBSyncStatusTable;
import com.doerit.doerdb.db.queries.executors.QueryExecutor;
import com.doerit.doerdb.exceptions.ExceptionCodes;
import com.doerit.doerdb.exceptions.InitializationFailureException;
import com.doerit.doerdb.exceptions.NotFoundException;
import com.doerit.doerdb.db.types.DatabaseType;
import com.doerit.doerdb.util.DatabaseValidator;

import java.sql.*;

public class DoerDatabase {

    private static final String EXCEPTION_MESSAGE_INITIALIZATION_FAILURE = "Database initialization failed due to either connection failure or database not being a valid DoerDB.";

    private final String hostURL;
    private final int hostPort;
    private final String dbName;
    private final String hostUsername;
    private final String hostPassword;

    private final Connection hostConnection;
    private final DoerDBMetaTable doerDBMetaTable;
    private final QueryExecutor queryExecutor;
    private DoerDBSyncDataTable doerDBSyncDataTable = null;
    private DoerDBSyncStatusTable doerDBSyncStatusTable = null;

    private boolean initSuccess;

    /**
     * Constructs DoerDatabase instance with the given parameters.
     * @param hostURL The Host(IP) address to the databases.
     * @param hostPort The port used by mysql servers.
     * @param dbName The name of the database to connect.
     * @param hostUsername Username which is used to establish the connection.
     * @param hostPassword Password associated with the given username.
     * @throws SQLException If JDBC Driver cannot establish a connection to the server.
     * @throws InitializationFailureException If DoerDatabase fails to be initialized.
     * @throws NotFoundException If Meta Table / Sync Table / Any trigger(s) are not found in the given database.
     */
    public DoerDatabase(String hostURL, int hostPort, String dbName, String hostUsername, String hostPassword, DatabaseType dbType) throws SQLException, InitializationFailureException, NotFoundException {
        this.hostURL = hostURL;
        this.hostPort = hostPort;
        this.dbName = dbName;
        this.hostUsername = hostUsername;
        this.hostPassword = hostPassword;

        String fqURL = JDBCConstants.PROTOCOL + "://" + hostURL + ":" + String.valueOf(hostPort) + "/" + dbName + "?" + JDBCConstants.CONNECTION_USER_ARG + "=" + hostUsername + "&" + JDBCConstants.CONNECTION_PASSWORD_ARG + "=" + hostPassword + "&" + JDBCConstants.CONNECTION_USE_SSL_ARG + "=false&allowMultiQueries=true";
        this.hostConnection = DriverManager.getConnection(fqURL);

        this.initSuccess = DatabaseValidator.isDatabaseValid(this.hostConnection, dbName, dbType);
        if (!this.initSuccess) {
            throw new InitializationFailureException(ExceptionCodes.INITIALIZATION_FAILURE, EXCEPTION_MESSAGE_INITIALIZATION_FAILURE);
        }

        this.doerDBMetaTable = new DoerDBMetaTable(this);
        this.queryExecutor = new QueryExecutor(this);
        if (dbType == DatabaseType.LOCAL) {
            this.doerDBSyncDataTable = new DoerDBSyncDataTable(this);
        }
        else if (dbType == DatabaseType.REMOTE) {
            this.doerDBSyncStatusTable = new DoerDBSyncStatusTable(this);
        }
    }

    /*
     * Executes a MySQL query on the databases.
     * @param sqlQuery The query to be executed on the databases.
     * @return java.sql.ResultSet returned after executing the query on the databases.
     * @throws SQLException If any exception occurs during the query execution.
     * @throws InitializationFailureException If DoerDatabase Implementation failed to initialize.
     */
    public ResultSet executeQuery(String sqlQuery) throws SQLException, InitializationFailureException {
        if (this.initSuccess) {
            Statement sqlStatement = this.hostConnection.createStatement();
            return sqlStatement.executeQuery(sqlQuery);
        }
        else {
            throw new InitializationFailureException(ExceptionCodes.INITIALIZATION_FAILURE, EXCEPTION_MESSAGE_INITIALIZATION_FAILURE);
        }
    }

    /**
     * Executes a raw SQL ADD/UPDATE/DELETE query on the databases.
     * No addition of the Query to Meta Table.
     * @param sqlQuery The ADD/UPDATE/DELETE query to be executed on the databases.
     * @return int The number of columns affected.
     * @throws SQLException If any exception occurs during the ADD/UPDATE/DELETE query execution.
     * @throws InitializationFailureException If DoerDatabase Implementation failed to initialize.
     */
    public int executeUpdate(String sqlQuery) throws SQLException, InitializationFailureException {
        if (this.initSuccess) {
            Statement sqlStatement = this.hostConnection.createStatement();
            return sqlStatement.executeUpdate(sqlQuery);
        }
        else {
            throw new InitializationFailureException(ExceptionCodes.INITIALIZATION_FAILURE, EXCEPTION_MESSAGE_INITIALIZATION_FAILURE);
        }
    }

    /**
     * Used to obtain the name of the database.
     * @return String The Name of the Database.
     */
    public String getDatabaseName() {
        return dbName;
    }

    /**
     * Used to obtain the Meta Table associated with the database.
     * @return DoerDBMetaTable for the container database.
     */
    public DoerDBMetaTable getMetaTable() {
        return this.doerDBMetaTable;
    }

    /**
     * Used to obtain Sync Data Table associated with the database.
     * This would return null if the instance of DoerDatabase is a type of DatabaseType.REMOTE(The remote database).
     * @return DoerDBSyncDataTable The Instance of DoerDBSyncDataTable associated with the DoerDatabase.
     */
    public DoerDBSyncDataTable getSyncDataTable() {
        return doerDBSyncDataTable;
    }

    /**
     * Used to obtain Sync Status Table associated with the database.
     * This would return null if the instance of DoerDatabase is a type of DatabaseType.LOCAL(The local database).
     * @return DoerDBSyncStatusTable The Instance of DoerDBSyncStatusTable associated with the DoerDatabase.
     */
    public DoerDBSyncStatusTable getSyncStatusTable() {
        return doerDBSyncStatusTable;
    }

    /**
     * Used to obtain the query executor for this DoerDatabase.
     * @return QueryExecutor The query executor for the DoerDatabase.
     */
    public QueryExecutor getQueryExecutor() {
        return queryExecutor;
    }
}
