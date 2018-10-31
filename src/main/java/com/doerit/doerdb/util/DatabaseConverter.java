package com.doerit.doerdb.util;

import com.doerit.doerdb.DBCredentialWrapper;
import com.doerit.doerdb.DoerDB;
import com.doerit.doerdb.db.MySQL;
import com.doerit.doerdb.db.jdbc.JDBCConstants;
import com.doerit.doerdb.db.metadata.DoerDBMetaTable;
import com.doerit.doerdb.db.metadata.DoerDBSyncDataTable;
import com.doerit.doerdb.db.metadata.DoerDBSyncStatusTable;
import com.doerit.doerdb.db.queries.InsertQuery;
import com.doerit.doerdb.db.queries.UpdateQuery;
import com.doerit.doerdb.exceptions.ExceptionCodes;
import com.doerit.doerdb.exceptions.InitializationFailureException;
import com.doerit.doerdb.exceptions.InvalidException;
import com.doerit.doerdb.exceptions.NotFoundException;
import com.doerit.doerdb.synchronizer.DoerDBSynchronizer;

import java.sql.*;

/**
 * DatabaseConverter is used to convert the existing databases to DoerDBs.
 */
public class DatabaseConverter {

    private static final String QUERY_META_TABLE_CREATE = "CREATE TABLE IF NOT EXISTS `" + DoerDBMetaTable.TABLE_NAME + "` (" +
            "`" + DoerDBMetaTable.TABLE_COL_ID + "` int(11) NOT NULL AUTO_INCREMENT," +
            "`" + DoerDBMetaTable.TABLE_COL_TABLE_NAME + "` text COLLATE utf32_bin NOT NULL," +
            "`" + DoerDBMetaTable.TABLE_COL_QUERY_TYPE + "` text COLLATE utf32_bin NOT NULL," +
            "`" + DoerDBMetaTable.TABLE_COL_NEW_RECORD + "` json NOT NULL," +
            "`" + DoerDBMetaTable.TABLE_COL_OLD_RECORD + "` json DEFAULT NULL," +
            "`" + DoerDBMetaTable.TABLE_COL_QUERY_TIMESTAMP + "` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
            "PRIMARY KEY (`" + DoerDBMetaTable.TABLE_COL_ID + "`)" +
            ") ENGINE=MyISAM AUTO_INCREMENT=4 DEFAULT CHARSET=utf32 COLLATE=utf32_bin";

    private static final String QUERY_SYNC_DATA_TABLE_CREATE = "CREATE TABLE IF NOT EXISTS `" + DoerDBSyncDataTable.TABLE_NAME + "` (" +
            " `" + DoerDBSyncDataTable.TABLE_COL_ID + "` int(11) NOT NULL AUTO_INCREMENT," +
            " `" + DoerDBSyncDataTable.TABLE_COL_LOCAL_LAST_ID + "` int(11) NOT NULL," +
            " `" + DoerDBSyncDataTable.TABLE_COL_REMOTE_LAST_ID + "` int(11) NOT NULL," +
            " PRIMARY KEY (`" + DoerDBSyncDataTable.TABLE_COL_ID + "`)" +
            ") ENGINE=MyISAM DEFAULT CHARSET=utf32 COLLATE=utf32_bin";

    private static final String QUERY_SYNC_STATUS_TABLE_CREATE = "CREATE TABLE IF NOT EXISTS `" + DoerDBSyncStatusTable.TABLE_NAME + "` (" +
            " `" + DoerDBSyncStatusTable.TABLE_COL_SYNC_STATUS + "` tinyint(1) NOT NULL DEFAULT '0'" +
            ") ENGINE=MyISAM DEFAULT CHARSET=utf32 COLLATE=utf32_bin";

    private static final String QUERY_SYNC_STATUS_TABLE_INSERT_STATUS = MySQL.SQL_INSERT_PREFIX + MySQL.SQL_SPACE +
            MySQL.SQL_INTERNAL_QUOTES + DoerDBSyncStatusTable.TABLE_NAME + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SPACE +
            MySQL.SQL_BRACKET_ROUND_OPEN +
            MySQL.SQL_INTERNAL_QUOTES + DoerDBSyncStatusTable.TABLE_COL_SYNC_STATUS + MySQL.SQL_INTERNAL_QUOTES +
            MySQL.SQL_BRACKET_ROUND_CLOSE + MySQL.SQL_SPACE +
            MySQL.SQL_INSERT_VALUES + MySQL.SQL_SPACE +
            MySQL.SQL_BRACKET_ROUND_OPEN +
            "0" +
            MySQL.SQL_BRACKET_ROUND_CLOSE;

    private static final String TRIGGER_JSON_PLACEHOLDER = "[REC_TYPE]";

    private final DBCredentialWrapper localDBCredentials;
    private final DBCredentialWrapper remoteDBCredentials;

    private final Connection localConnection;
    private final Connection remoteConnection;

    /**
     * Constructor for DatabaseConverter.
     * @param localDBCredentials DBCredentialWrapper wrapped with local database credentials.
     * @param remoteDBCredentials DBCredentialWrapper wrapped with remote database credentials.
     * @throws SQLException If any error occurs while connecting to the databases.
     */
    public DatabaseConverter(DBCredentialWrapper localDBCredentials, DBCredentialWrapper remoteDBCredentials) throws SQLException {
        this.localDBCredentials = localDBCredentials;
        this.remoteDBCredentials = remoteDBCredentials;

        if (localDBCredentials != null) {
            String localFQURL = JDBCConstants.PROTOCOL + "://" + localDBCredentials.hostURL + ":" + String.valueOf(localDBCredentials.hostPort) + "/" + localDBCredentials.dbName + "?" + JDBCConstants.CONNECTION_USER_ARG + "=" + localDBCredentials.hostUsername + "&" + JDBCConstants.CONNECTION_PASSWORD_ARG + "=" + localDBCredentials.hostPassword + "&" + JDBCConstants.CONNECTION_USE_SSL_ARG + "=false&allowMultiQueries=true";
            this.localConnection = DriverManager.getConnection(localFQURL);
        }
        else {
            this.localConnection = null;
        }

        if (remoteDBCredentials != null) {
            String remoteFQURL = JDBCConstants.PROTOCOL + "://" + remoteDBCredentials.hostURL + ":" + String.valueOf(remoteDBCredentials.hostPort) + "/" + remoteDBCredentials.dbName + "?" + JDBCConstants.CONNECTION_USER_ARG + "=" + remoteDBCredentials.hostUsername + "&" + JDBCConstants.CONNECTION_PASSWORD_ARG + "=" + remoteDBCredentials.hostPassword + "&" + JDBCConstants.CONNECTION_USE_SSL_ARG + "=false&allowMultiQueries=true";
            this.remoteConnection = DriverManager.getConnection(remoteFQURL);
        }
        else {
            this.remoteConnection = null;
        }
    }

    /**
     * Checks for the existence of a meta table in a database specified by a MySQL JDBC connection.
     * @param connection MySQL JDBC Connection in which the existence of meta table should be checked.
     * @return boolean true if meta table exists, false otherwise.
     * @throws SQLException If any error occurs while querying the database.
     */
    private static boolean isMetaTableExisting(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultMetaTable = statement.executeQuery(MySQL.SQL_SHOW_CLAUSE + MySQL.SQL_SPACE + MySQL.SQL_CONTENT_TABLES + MySQL.SQL_SPACE +
                MySQL.SQL_LIKE_OPERATOR + MySQL.SQL_SPACE + MySQL.SQL_EXTERNAL_QUOTES + DoerDBMetaTable.TABLE_NAME + MySQL.SQL_EXTERNAL_QUOTES);

        return resultMetaTable.next();
    }

    /**
     * Checks for the existence of a sync data table in a database specified by a MySQL JDBC connection.
     * @param connection MySQL JDBC Connection in which the existence of sync data table should be checked.
     * @return boolean true if sync data table exists, false otherwise.
     * @throws SQLException If any error occurs while querying the database.
     */
    private static boolean isSyncDataTableExisting(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSyncDataTable = statement.executeQuery(MySQL.SQL_SHOW_CLAUSE + MySQL.SQL_SPACE + MySQL.SQL_CONTENT_TABLES + MySQL.SQL_SPACE +
                MySQL.SQL_LIKE_OPERATOR + MySQL.SQL_SPACE + MySQL.SQL_EXTERNAL_QUOTES + DoerDBSyncDataTable.TABLE_NAME + MySQL.SQL_EXTERNAL_QUOTES);

        return resultSyncDataTable.next();
    }

    /**
     * Checks for the existence of a sync status table in a database specified by a MySQL JDBC connection.
     * @param connection MySQL JDBC Connection in which the existence of sync status table should be checked.
     * @return boolean true if sync status table exists, false otherwise.
     * @throws SQLException If any error occurs while querying the database.
     */
    private static boolean isSyncStatusTableExisting(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSyncStatusTable = statement.executeQuery(MySQL.SQL_SHOW_CLAUSE + MySQL.SQL_SPACE + MySQL.SQL_CONTENT_TABLES + MySQL.SQL_SPACE +
                MySQL.SQL_LIKE_OPERATOR + MySQL.SQL_SPACE + MySQL.SQL_EXTERNAL_QUOTES + DoerDBSyncStatusTable.TABLE_NAME + MySQL.SQL_EXTERNAL_QUOTES);

        return resultSyncStatusTable.next();
    }

    /**
     * Generates triggers for the given database.
     * @param connection Connection MySQL Connection to the database in which the triggers should be generated.
     * @param dbName String The name of the database specified by the connection.
     * @throws SQLException If any error occurs while querying the database.
     */
    public void generateTriggers(Connection connection, String dbName) throws SQLException {
        String queryTableSet = MySQL.SQL_SELECT_CLAUSE + MySQL.SQL_SPACE + MySQL.SQL_CONTENT_TABLE_NAME + MySQL.SQL_SPACE +
                MySQL.SQL_FROM_CLAUSE + MySQL.SQL_SPACE + MySQL.SQL_CONTENT_INFORMATION_SCHEMA_TABLES + MySQL.SQL_SPACE +
                MySQL.SQL_WHERE_CLAUSE + MySQL.SQL_SPACE + MySQL.SQL_CONTENT_TABLE_SCHEMA + MySQL.SQL_EQUATOR + MySQL.SQL_EXTERNAL_QUOTES + dbName + MySQL.SQL_EXTERNAL_QUOTES;
        ResultSet resultTableSet = connection.createStatement().executeQuery(queryTableSet);
        while (resultTableSet.next()) {
            String tableName = resultTableSet.getString(MySQL.SQL_CONTENT_TABLE_NAME);
            if (DatabaseValidator.isUnmonitoredTableName(tableName)) {
                continue;
            }

            String tableTriggerNameInsert = DatabaseValidator.getTriggerName(tableName, DatabaseValidator.TRIGGER_INSERT);
            String tableTriggerNameUpdate = DatabaseValidator.getTriggerName(tableName, DatabaseValidator.TRIGGER_UPDATE);

            String queryDropTriggerInsert = MySQL.SQL_DROP_CLAUSE + MySQL.SQL_SPACE + MySQL.SQL_CONTENT_TRIGGER + MySQL.SQL_SPACE + MySQL.SQL_IF_CONDITION + MySQL.SQL_SPACE + MySQL.SQL_EXISTS_OPERATOR + MySQL.SQL_SPACE + tableTriggerNameInsert;
            String queryDropTriggerUpdate = MySQL.SQL_DROP_CLAUSE + MySQL.SQL_SPACE + MySQL.SQL_CONTENT_TRIGGER + MySQL.SQL_SPACE + MySQL.SQL_IF_CONDITION + MySQL.SQL_SPACE + MySQL.SQL_EXISTS_OPERATOR + MySQL.SQL_SPACE + tableTriggerNameUpdate;
            connection.createStatement().executeUpdate(queryDropTriggerInsert);
            connection.createStatement().executeUpdate(queryDropTriggerUpdate);

            String queryTableColumns = MySQL.SQL_SELECT_CLAUSE + MySQL.SQL_SPACE + MySQL.SQL_CONTENT_COLUMN_NAME + MySQL.SQL_SPACE +
                    MySQL.SQL_FROM_CLAUSE + MySQL.SQL_INTERNAL_QUOTES + "INFORMATION_SCHEMA" + MySQL.SQL_INTERNAL_QUOTES + "." + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_CONTENT_COLUMNS + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SPACE +
                    MySQL.SQL_WHERE_CLAUSE + MySQL.SQL_SPACE + MySQL.SQL_INTERNAL_QUOTES + "TABLE_SCHEMA" + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_EQUATOR + MySQL.SQL_EXTERNAL_QUOTES + dbName + MySQL.SQL_EXTERNAL_QUOTES + MySQL.SQL_SPACE +
                    MySQL.SQL_AND_OPERATOR + MySQL.SQL_SPACE + MySQL.SQL_INTERNAL_QUOTES + "TABLE_NAME" + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_EQUATOR + MySQL.SQL_EXTERNAL_QUOTES + tableName + MySQL.SQL_EXTERNAL_QUOTES;
            ResultSet resultTableColumns = connection.createStatement().executeQuery(queryTableColumns);
            String serialColumnsWithValues = "CONCAT('{";
            while (resultTableColumns.next()) {
                String columnName = resultTableColumns.getString(MySQL.SQL_CONTENT_COLUMN_NAME);
                String columnValue = DatabaseConverter.TRIGGER_JSON_PLACEHOLDER + "." + columnName;
                serialColumnsWithValues += "\"" + columnName + "\": \"', " + columnValue + ", '\", ";
            }
            serialColumnsWithValues = serialColumnsWithValues.substring(0, serialColumnsWithValues.length() - 4) + "'\"}')";

            String queryCreateTriggerInsert = String.format(
                            MySQL.SQL_CREATE_CLAUSE + MySQL.SQL_SPACE + MySQL.SQL_CONTENT_TRIGGER + MySQL.SQL_SPACE + tableTriggerNameInsert + MySQL.SQL_SPACE +
                            MySQL.SQL_AFTER_CLAUSE + MySQL.SQL_SPACE + MySQL.SQL_INSERT_CLAUSE + MySQL.SQL_SPACE + MySQL.SQL_ON_OPERATOR + MySQL.SQL_SPACE + MySQL.SQL_INTERNAL_QUOTES + tableName + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SPACE +
                            "\t" + MySQL.SQL_FOR_OPERATOR + " EACH ROW " +
                            "\t" + MySQL.SQL_BEGIN_CLAUSE + " " +
                            "\t\t" + MySQL.SQL_INSERT_PREFIX + MySQL.SQL_SPACE + MySQL.SQL_INTERNAL_QUOTES + DoerDBMetaTable.TABLE_NAME + MySQL.SQL_INTERNAL_QUOTES +
                            MySQL.SQL_BRACKET_ROUND_OPEN +
                            MySQL.SQL_INTERNAL_QUOTES + DoerDBMetaTable.TABLE_COL_TABLE_NAME + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SEPARATOR +
                            MySQL.SQL_INTERNAL_QUOTES + DoerDBMetaTable.TABLE_COL_QUERY_TYPE + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SEPARATOR +
                            MySQL.SQL_INTERNAL_QUOTES + DoerDBMetaTable.TABLE_COL_NEW_RECORD + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SEPARATOR +
                            MySQL.SQL_INTERNAL_QUOTES + DoerDBMetaTable.TABLE_COL_QUERY_TIMESTAMP + MySQL.SQL_INTERNAL_QUOTES +
                            MySQL.SQL_BRACKET_ROUND_CLOSE + MySQL.SQL_SPACE +
                            MySQL.SQL_INSERT_VALUES + MySQL.SQL_SPACE +
                            MySQL.SQL_BRACKET_ROUND_OPEN +
                            MySQL.SQL_EXTERNAL_QUOTES + tableName + MySQL.SQL_EXTERNAL_QUOTES + MySQL.SQL_SEPARATOR +
                            MySQL.SQL_EXTERNAL_QUOTES + InsertQuery.QUERY_TYPE + MySQL.SQL_EXTERNAL_QUOTES + MySQL.SQL_SEPARATOR +
                            "%s" + MySQL.SQL_SEPARATOR +
                            MySQL.SQL_IF_CONDITION + MySQL.SQL_BRACKET_ROUND_OPEN + DoerDBSynchronizer.MYSQL_TAG_QUERY_TIMESTAMP + MySQL.SQL_SPACE + MySQL.SQL_IS_OPERATOR + MySQL.SQL_SPACE + MySQL.SQL_VALUE_NULL + MySQL.SQL_SEPARATOR + "CURRENT_TIMESTAMP" + MySQL.SQL_SEPARATOR + DoerDBSynchronizer.MYSQL_TAG_QUERY_TIMESTAMP + MySQL.SQL_BRACKET_ROUND_CLOSE +
                            MySQL.SQL_BRACKET_ROUND_CLOSE + ";" +
                            "\t" + MySQL.SQL_END_CLAUSE + ";",
                    serialColumnsWithValues.replace(DatabaseConverter.TRIGGER_JSON_PLACEHOLDER, MySQL.SQL_NEW_OPERATOR));

            String queryCreateTriggerUpdate = String.format(
                            MySQL.SQL_CREATE_CLAUSE + MySQL.SQL_SPACE + MySQL.SQL_CONTENT_TRIGGER + MySQL.SQL_SPACE + tableTriggerNameUpdate + MySQL.SQL_SPACE +
                            MySQL.SQL_AFTER_CLAUSE + MySQL.SQL_SPACE + MySQL.SQL_UPDATE_PREFIX + MySQL.SQL_SPACE + MySQL.SQL_ON_OPERATOR + MySQL.SQL_SPACE + MySQL.SQL_INTERNAL_QUOTES + tableName + MySQL.SQL_INTERNAL_QUOTES + " " +
                            "\t" + MySQL.SQL_FOR_OPERATOR + " EACH ROW " +
                            "\t" + MySQL.SQL_BEGIN_CLAUSE + " " +
                            "\t\t" + MySQL.SQL_INSERT_PREFIX + MySQL.SQL_SPACE + MySQL.SQL_INTERNAL_QUOTES + DoerDBMetaTable.TABLE_NAME + MySQL.SQL_INTERNAL_QUOTES +
                            MySQL.SQL_BRACKET_ROUND_OPEN +
                            MySQL.SQL_INTERNAL_QUOTES + DoerDBMetaTable.TABLE_COL_TABLE_NAME + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SEPARATOR +
                            MySQL.SQL_INTERNAL_QUOTES + DoerDBMetaTable.TABLE_COL_QUERY_TYPE + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SEPARATOR +
                            MySQL.SQL_INTERNAL_QUOTES + DoerDBMetaTable.TABLE_COL_NEW_RECORD + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SEPARATOR +
                            MySQL.SQL_INTERNAL_QUOTES + DoerDBMetaTable.TABLE_COL_OLD_RECORD + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SEPARATOR +
                            MySQL.SQL_INTERNAL_QUOTES + DoerDBMetaTable.TABLE_COL_QUERY_TIMESTAMP + MySQL.SQL_INTERNAL_QUOTES +
                            MySQL.SQL_BRACKET_ROUND_CLOSE + MySQL.SQL_SPACE +
                            MySQL.SQL_INSERT_VALUES + MySQL.SQL_SPACE +
                            MySQL.SQL_BRACKET_ROUND_OPEN +
                            MySQL.SQL_EXTERNAL_QUOTES + tableName + MySQL.SQL_EXTERNAL_QUOTES + MySQL.SQL_SEPARATOR +
                            MySQL.SQL_EXTERNAL_QUOTES + UpdateQuery.QUERY_TYPE + MySQL.SQL_EXTERNAL_QUOTES + MySQL.SQL_SEPARATOR +
                            "%s" + MySQL.SQL_SEPARATOR +
                            "%s" + MySQL.SQL_SEPARATOR +
                            MySQL.SQL_IF_CONDITION + MySQL.SQL_BRACKET_ROUND_OPEN + DoerDBSynchronizer.MYSQL_TAG_QUERY_TIMESTAMP + MySQL.SQL_SPACE + MySQL.SQL_IS_OPERATOR + MySQL.SQL_SPACE + MySQL.SQL_VALUE_NULL + MySQL.SQL_SEPARATOR + "CURRENT_TIMESTAMP" + MySQL.SQL_SEPARATOR + DoerDBSynchronizer.MYSQL_TAG_QUERY_TIMESTAMP + MySQL.SQL_BRACKET_ROUND_CLOSE +
                            MySQL.SQL_BRACKET_ROUND_CLOSE + "; " +
                            "\t" + MySQL.SQL_END_CLAUSE + ";",
                    serialColumnsWithValues.replace(DatabaseConverter.TRIGGER_JSON_PLACEHOLDER, MySQL.SQL_NEW_OPERATOR),
                    serialColumnsWithValues.replace(DatabaseConverter.TRIGGER_JSON_PLACEHOLDER, MySQL.SQL_OLD_OPERATOR));

            connection.createStatement().executeUpdate(queryCreateTriggerInsert);
            connection.createStatement().executeUpdate(queryCreateTriggerUpdate);
        }
        resultTableSet.close();
    }

    /**
     * Converts the given pair of local and remote databases into DoerDatabases.
     * Creates a meta table in both databases.
     * Generates triggers required for recording queries on meta tables.
     * @throws SQLException If any error occurs while querying the database.
     * @throws InvalidException If meta table is currently found in any of the databases.
     */
    public void convertToDoerDB() throws SQLException, InvalidException {
        boolean localOnly = this.localConnection != null && this.remoteConnection == null;
        boolean remoteOnly = this.remoteConnection != null && this.localConnection == null;

        boolean localMetaTableExists = this.localConnection != null && DatabaseConverter.isMetaTableExisting(this.localConnection);
        boolean localSyncDataTableExists = this.localConnection != null && DatabaseConverter.isSyncDataTableExisting(this.localConnection);
        boolean remoteMetaTableExists = this.remoteConnection != null && DatabaseConverter.isMetaTableExisting(this.remoteConnection);
        boolean remoteSyncStatusTableExists = this.remoteConnection != null && DatabaseConverter.isSyncStatusTableExisting(this.remoteConnection);
        if (localMetaTableExists) {
            throw new InvalidException(ExceptionCodes.ALREADY_FOUND, "A MetaTable already exists in the Local Database. Meta Table Name: " + DoerDBMetaTable.TABLE_NAME);
        }
        else if (localSyncDataTableExists) {
            throw new InvalidException(ExceptionCodes.ALREADY_FOUND, "A SyncDataTable already exists in the Local Database. Sync Data Table Name: " + DoerDBSyncDataTable.TABLE_NAME);
        }
        else if (remoteMetaTableExists) {
            throw new InvalidException(ExceptionCodes.ALREADY_FOUND, "A MetaTable already exists in the Remote Database. Meta Table Name: " + DoerDBMetaTable.TABLE_NAME);
        }
        else if (remoteSyncStatusTableExists) {
            throw new InvalidException(ExceptionCodes.ALREADY_FOUND, "A SyncStatusTable already exists in the Remote Database. Sync Status Table Name: " + DoerDBSyncStatusTable.TABLE_NAME);
        }
        else {
            boolean shouldRunLocal = true;
            boolean shouldRunRemote = true;

            if (!localOnly && remoteOnly) {
                shouldRunLocal = false;
            }

            if (!remoteOnly && localOnly) {
                shouldRunRemote = false;
            }

            if (!remoteOnly && !localOnly) {
                throw new InvalidException(ExceptionCodes.INVALID_OPERATION, "There should be a database connection to convert. Please provide the credentials.");
            }

            if (shouldRunLocal) {
                this.localConnection.createStatement().executeUpdate(DatabaseConverter.QUERY_META_TABLE_CREATE);
                this.localConnection.createStatement().executeUpdate(DatabaseConverter.QUERY_SYNC_DATA_TABLE_CREATE);
                this.generateTriggers(this.localConnection, this.localDBCredentials.dbName);
            }

            if (shouldRunRemote) {
                this.remoteConnection.createStatement().executeUpdate(DatabaseConverter.QUERY_META_TABLE_CREATE);
                this.remoteConnection.createStatement().executeUpdate(DatabaseConverter.QUERY_SYNC_STATUS_TABLE_CREATE);
                this.remoteConnection.createStatement().executeUpdate(DatabaseConverter.QUERY_SYNC_STATUS_TABLE_INSERT_STATUS);
                this.generateTriggers(this.remoteConnection, this.remoteDBCredentials.dbName);
            }
        }
    }

    /**
     * Returns a DoerDB instance for the pair of databases provided initially.
     * @return DoerDB identified by the pair of databases.
     * @throws SQLException If any exception occurs while connecting to databases.
     * @throws InitializationFailureException If database is invalid DoerDB.
     * @throws NotFoundException If Meta Table / Sync Table / Any trigger(s) are not found in any of the given databases.
     */
    public DoerDB getDoerDB() throws SQLException, InitializationFailureException, NotFoundException {
        return new DoerDB(this.localDBCredentials, this.remoteDBCredentials);
    }

}
