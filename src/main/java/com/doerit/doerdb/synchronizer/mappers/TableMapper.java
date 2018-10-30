package com.doerit.doerdb.synchronizer.mappers;

import com.doerit.doerdb.DoerDB;
import com.doerit.doerdb.db.MySQL;
import com.doerit.doerdb.db.templates.MySQLQueryTemplates;
import com.doerit.doerdb.exceptions.InitializationFailureException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TableMapper {

    private String localTableName;
    private String remoteTableName;
    private List<ColumnMapper> columnMappers;

    /**
     * Constructor.
     * @param doerDB DoerDB The DoerDB instance for which the Mapper should be created.
     * @param localTableName String The name of the local table to be used in mapping.
     * @param remoteTableName String The name of the remote table to be used in mapping.
     * @throws SQLException If any exception is thrown during the execution of MySQL queries internally.
     * @throws InitializationFailureException If any exception is thrown during the initialization of DoerDatabases.
     */
    public TableMapper(DoerDB doerDB, String localTableName, String remoteTableName) throws SQLException, InitializationFailureException {
        this.localTableName = localTableName;
        this.remoteTableName = remoteTableName;

        this.initColumnMapper(doerDB);
    }

    /**
     * Initializes the ColumnMapper for the TableMapper.
     * @param doerDB DoerDB The DoerDB instance related to the TableMapper.
     * @throws SQLException If any exception is thrown during the execution of MySQL queries internally.
     * @throws InitializationFailureException If any exception is thrown during the initialization of DoerDatabases.
     */
    private void initColumnMapper(DoerDB doerDB) throws SQLException, InitializationFailureException {
        String queryLocalAllColumns = MySQLQueryTemplates.QUERY_ALL_COLUMNS
                .replace(MySQLQueryTemplates.PLACEHOLDER_TABLE_NAME, this.localTableName);
        String queryRemoteAllColumns = MySQLQueryTemplates.QUERY_ALL_COLUMNS
                .replace(MySQLQueryTemplates.PLACEHOLDER_TABLE_NAME, this.remoteTableName);

        ResultSet resultLocalAllColumns = doerDB.executeLocalQuery(queryLocalAllColumns);
        ResultSet resultRemoteAllColumns = doerDB.executeRemoteQuery(queryRemoteAllColumns);

        List<String> localColumnNames = new ArrayList<>();
        List<String> remoteColumnNames = new ArrayList<>();

        while (resultLocalAllColumns.next()) {
            localColumnNames.add(resultLocalAllColumns.getString(MySQL.SQL_CONTENT_FIELD));
        }

        while (resultRemoteAllColumns.next()) {
            remoteColumnNames.add(resultRemoteAllColumns.getString(MySQL.SQL_CONTENT_FIELD));
        }

        this.columnMappers = new ArrayList<>();
        for (String localColumnName : localColumnNames) {
            String matchingRemoteColumnName = null;
            for (String remoteColumnName : remoteColumnNames) {
                if (localColumnName.equals(remoteColumnName)) {
                    matchingRemoteColumnName = remoteColumnName;
                    break;
                }
            }

            this.columnMappers.add(new ColumnMapper(localColumnName, matchingRemoteColumnName));
        }
    }

    /**
     * Used to obtain the local table name used in mappings.
     * @return String Local Table Name.
     */
    public String getLocalTableName() {
        return localTableName;
    }

    /**
     * Used to obtain the remote table name used in mappings.
     * @return String Remote Table Name.
     */
    public String getRemoteTableName() {
        return remoteTableName;
    }

    /**
     * Returns the list of Column Mappers for the TableMapper's DoerDB.
     * @return List of TableMapper instances.
     */
    public List<ColumnMapper> getColumnMappers() {
        return columnMappers;
    }

    /**
     * Used to obtain the Column Mapper related to the provided table name in the local database.
     * @param localColumnName String The name of the local column.
     * @return ColumnMapper Instance.
     */
    public ColumnMapper getColumnMapperByLocalColumn(String localColumnName) {
        for (ColumnMapper columnMapper : this.columnMappers) {
            if (columnMapper.getLocalColumnName().equals(localColumnName)) {
                return columnMapper;
            }
        }

        return null;
    }

    /**
     * Used to obtain the Column Mapper related to the provided table name in the remote database.
     * @param remoteColumnName String The name of the remote column.
     * @return ColumnMapper Instance.
     */
    public ColumnMapper getColumnMapperByRemoteColumn(String remoteColumnName) {
        for (ColumnMapper columnMapper : this.columnMappers) {
            if (columnMapper.getRemoteColumnName().equals(remoteColumnName)) {
                return columnMapper;
            }
        }

        return null;
    }

    /**
     * Sets the ColumnMapper for TableMapper.
     * Searches for either of local/remote column name in the given ColumnMapper in the list of ColumnMappers in the current TableMapper and sets it.
     * @param columnMapper ColumnMapper The ColumnMapper instance to be set.
     * @return boolean true if successful, false otherwise(when either of the column names cannot be found in the table)
     */
    public boolean setColumnMapper(ColumnMapper columnMapper) {
        for (int i = 0; i < this.columnMappers.size(); i++) {
            ColumnMapper iterColumnMapper = this.columnMappers.get(i);

            if (iterColumnMapper.getLocalColumnName().equals(columnMapper.getLocalColumnName()) || iterColumnMapper.getRemoteColumnName().equals(columnMapper.getRemoteColumnName())) {
                this.columnMappers.set(i, columnMapper);
                return true;
            }
        }

        return false;
    }

}
