package com.doerit.doerdb.util;

import com.doerit.doerdb.db.MySQL;
import com.doerit.doerdb.db.metadata.DoerDBMetaTable;
import com.doerit.doerdb.db.metadata.DoerDBSyncDataTable;
import com.doerit.doerdb.db.metadata.DoerDBSyncStatusTable;
import com.doerit.doerdb.db.templates.MySQLQueryTemplates;
import com.doerit.doerdb.db.types.DatabaseType;
import com.doerit.doerdb.exceptions.ExceptionCodes;
import com.doerit.doerdb.exceptions.NotFoundException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DatabaseValidator {

    public static final String TRIGGER_PREFIX = "trigger";
    public static final String TRIGGER_INSERT = "insert";
    public static final String TRIGGER_UPDATE = "update";

    public static boolean isUnmonitoredTableName(String tableName) {
        return tableName.equals(DoerDBMetaTable.TABLE_NAME) ||
                tableName.equals(DoerDBSyncDataTable.TABLE_NAME) ||
                tableName.equals(DoerDBSyncStatusTable.TABLE_NAME);
    }

    public static String getTriggerName(String tableName, String triggerType) {
        return TRIGGER_PREFIX + "_" + triggerType + "_" + tableName;
    }

    private static boolean triggerExists(Connection connection, String databaseName, String triggerName) throws SQLException {
        String queryTriggerInsertExists = MySQLQueryTemplates.QUERY_TRIGGER_EXISTS
                .replace(MySQLQueryTemplates.PLACEHOLDER_DATABASE_NAME, databaseName)
                .replace(MySQLQueryTemplates.PLACEHOLDER_TRIGGER_NAME, triggerName);
        ResultSet resultsTriggerExists = connection.createStatement().executeQuery(queryTriggerInsertExists);
        return resultsTriggerExists.next();
    }

    public static boolean isDatabaseValid(Connection connection, String databaseName, DatabaseType databaseType) throws SQLException, NotFoundException {
        String queryMetaTable = MySQLQueryTemplates.QUERY_TABLE_EXISTS
                .replace(MySQLQueryTemplates.PLACEHOLDER_DATABASE_NAME, databaseName)
                .replace(MySQLQueryTemplates.PLACEHOLDER_TABLE_NAME, DoerDBMetaTable.TABLE_NAME);
        ResultSet resultsMetaTable = connection.createStatement().executeQuery(queryMetaTable);
        if (!resultsMetaTable.next()) {
            throw new NotFoundException(ExceptionCodes.NOT_FOUND, "Meta table in the database: " + databaseName + " is not found.");
        }

        if (databaseType == DatabaseType.LOCAL) {
            String querySyncDataTable = MySQLQueryTemplates.QUERY_TABLE_EXISTS
                    .replace(MySQLQueryTemplates.PLACEHOLDER_DATABASE_NAME, databaseName)
                    .replace(MySQLQueryTemplates.PLACEHOLDER_TABLE_NAME, DoerDBSyncDataTable.TABLE_NAME);
            ResultSet resultsSyncDataTable = connection.createStatement().executeQuery(querySyncDataTable);
            if (!resultsSyncDataTable.next()) {
                throw new NotFoundException(ExceptionCodes.NOT_FOUND, "Sync data table in the database: " + databaseName + " is not found.");
            }
        }
        else if (databaseType == DatabaseType.REMOTE) {
            String querySyncStatusTable = MySQLQueryTemplates.QUERY_TABLE_EXISTS
                    .replace(MySQLQueryTemplates.PLACEHOLDER_DATABASE_NAME, databaseName)
                    .replace(MySQLQueryTemplates.PLACEHOLDER_TABLE_NAME, DoerDBSyncStatusTable.TABLE_NAME);
            ResultSet resultsSyncStatusTable = connection.createStatement().executeQuery(querySyncStatusTable);
            if (!resultsSyncStatusTable.next()) {
                throw new NotFoundException(ExceptionCodes.NOT_FOUND, "Sync status table in the database: " + databaseName + " is not found.");
            }
        }

        String queryAllTables = MySQLQueryTemplates.QUERY_ALL_TABLES
                .replace(MySQLQueryTemplates.PLACEHOLDER_DATABASE_NAME, databaseName);
        ResultSet resultsAllTables = connection.createStatement().executeQuery(queryAllTables);
        while (resultsAllTables.next()) {
            String tableName = resultsAllTables.getString(MySQL.SQL_CONTENT_TABLE_NAME);

            if (!DatabaseValidator.isUnmonitoredTableName(tableName)) {
                String triggerNameInsert = DatabaseValidator.getTriggerName(tableName, DatabaseValidator.TRIGGER_INSERT);
                boolean existsTriggerInsert = DatabaseValidator.triggerExists(connection, databaseName, triggerNameInsert);

                String triggerNameUpdate = DatabaseValidator.getTriggerName(tableName, DatabaseValidator.TRIGGER_UPDATE);
                boolean existsTriggerUpdate = DatabaseValidator.triggerExists(connection, databaseName, triggerNameUpdate);

                if (!existsTriggerInsert) {
                    throw new NotFoundException(ExceptionCodes.NOT_FOUND, "Trigger: " + triggerNameInsert + " not found. (Database: " + databaseName + ")");
                }
                else if (!existsTriggerUpdate) {
                    throw new NotFoundException(ExceptionCodes.NOT_FOUND, "Trigger: " + triggerNameUpdate + " not found. (Database: " + databaseName + ")");
                }
            }
        }

        return true;
    }
}
