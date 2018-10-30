package com.doerit.doerdb.synchronizer.mappers;

import com.doerit.doerdb.DoerDB;
import com.doerit.doerdb.db.MySQL;
import com.doerit.doerdb.db.templates.MySQLQueryTemplates;
import com.doerit.doerdb.exceptions.InitializationFailureException;
import com.doerit.doerdb.util.DatabaseValidator;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DatabaseMapper {

    private final String localDatabaseName;
    private final String remoteDatabaseName;
    private List<TableMapper> tableMappers;

    /**
     * Constructor.
     * @param doerDB DoerDB The DoerDB instance for which the Mapper should be created.
     * @throws SQLException If any exception is thrown during the execution of MySQL queries internally.
     * @throws InitializationFailureException If any exception is thrown during the initialization of DoerDatabases.
     */
    public DatabaseMapper(DoerDB doerDB) throws SQLException, InitializationFailureException {
        this.localDatabaseName = doerDB.getLocalDatabase().getDatabaseName();
        this.remoteDatabaseName = doerDB.getRemoteDatabase().getDatabaseName();

        this.initTableMapper(doerDB);
    }

    /**
     * Initializes the TableMapper for the DatabaseMapper.
     * @param doerDB DoerDB The DoerDB instance related to the DatabaseMapper.
     * @throws SQLException If any exception is thrown during the execution of MySQL queries internally.
     * @throws InitializationFailureException If any exception is thrown during the initialization of DoerDatabases.
     */
    private void initTableMapper(DoerDB doerDB) throws SQLException, InitializationFailureException {
        String queryLocalAllTables = MySQLQueryTemplates.QUERY_ALL_TABLES
                .replace(MySQLQueryTemplates.PLACEHOLDER_DATABASE_NAME, doerDB.getLocalDatabase().getDatabaseName());
        String queryRemoteAllTables = MySQLQueryTemplates.QUERY_ALL_TABLES
                .replace(MySQLQueryTemplates.PLACEHOLDER_DATABASE_NAME, doerDB.getRemoteDatabase().getDatabaseName());

        ResultSet resultLocalAllTables = doerDB.executeLocalQuery(queryLocalAllTables);
        ResultSet resultRemoteAllTables = doerDB.executeRemoteQuery(queryRemoteAllTables);

        List<String> localTableNames = new ArrayList<>();
        List<String> remoteTableNames = new ArrayList<>();

        while (resultLocalAllTables.next()) {
            String localTableName = resultLocalAllTables.getString(MySQL.SQL_CONTENT_TABLE_NAME);
            if (!DatabaseValidator.isUnmonitoredTableName(localTableName)) {
                localTableNames.add(localTableName);
            }
        }

        while (resultRemoteAllTables.next()) {
            String remoteTableName = resultRemoteAllTables.getString(MySQL.SQL_CONTENT_TABLE_NAME);
            if (!DatabaseValidator.isUnmonitoredTableName(remoteTableName)) {
                remoteTableNames.add(remoteTableName);
            }
        }

        this.tableMappers = new ArrayList<>();
        for (String localTableName : localTableNames) {
            String matchingRemoteTableName = null;
            for (String remoteTableName : remoteTableNames) {
                if (localTableName.equals(remoteTableName)) {
                    matchingRemoteTableName = remoteTableName;
                    break;
                }
            }

            this.tableMappers.add(new TableMapper(doerDB, localTableName, matchingRemoteTableName));
        }
    }

    /**
     * Used to obtain the local database name used in mappings.
     * @return String Local Database Name.
     */
    public String getLocalDatabaseName() {
        return localDatabaseName;
    }

    /**
     * Used to obtain the remote database name used in mappings.
     * @return String Remote Database Name.
     */
    public String getRemoteDatabaseName() {
        return remoteDatabaseName;
    }

    /**
     * Returns the list of Table Mappers for the DoerDB.
     * @return List of TableMapper instances.
     */
    public List<TableMapper> getTableMappers() {
        return tableMappers;
    }

    /**
     * Sets the list of Table Mappers for the DoerDB instance.
     * @param tableMappers List of TableMapper instances.
     */
    public void setTableMappers(List<TableMapper> tableMappers) {
        this.tableMappers = tableMappers;
    }

    /**
     * Used to obtain the Table Mapper related to the provided table name in the local database.
     * @param localTableName String The name of the local table.
     * @return TableMapper Instance.
     */
    public TableMapper getTableMapperByLocalTable(String localTableName) {
        for (TableMapper tableMapper : this.tableMappers) {
            if (tableMapper.getLocalTableName().equals(localTableName)) {
                return tableMapper;
            }
        }

        return null;
    }

    /**
     * Used to obtain the Table Mapper related to the provided table name in the remote database.
     * @param remoteTableName String The name of the remote table.
     * @return TableMapper Instance.
     */
    public TableMapper getTableMapperByRemoteTable(String remoteTableName) {
        for (TableMapper tableMapper : this.tableMappers) {
            if (tableMapper.getRemoteTableName().equals(remoteTableName)) {
                return tableMapper;
            }
        }

        return null;
    }

    /**
     * Sets a TableMapper for DatabaseMapper.
     * Searches for either of local/remote table name in the given TableMapper in the list of TableMappers in the current DatabaseMapper and sets it.
     * @param tableMapper TableMapper The TableMapper instance to be set.
     * @return boolean true if successful, false otherwise(when either of the table names cannot be found in the database)
     */
    public boolean setTableMapper(TableMapper tableMapper) {
        for (int i = 0; i < this.tableMappers.size(); i++) {
            TableMapper iterTableMapper = this.tableMappers.get(i);

            if (iterTableMapper.getLocalTableName().equals(tableMapper.getLocalTableName()) || iterTableMapper.getRemoteTableName().equals(tableMapper.getRemoteTableName())) {
                this.tableMappers.set(i, tableMapper);
                return true;
            }
        }

        return false;
    }

}
