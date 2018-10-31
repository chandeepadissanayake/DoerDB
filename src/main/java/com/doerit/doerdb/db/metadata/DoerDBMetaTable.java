package com.doerit.doerdb.db.metadata;

import com.doerit.doerdb.db.DoerDatabase;
import com.doerit.doerdb.db.MySQL;
import com.doerit.doerdb.exceptions.InitializationFailureException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class DoerDBMetaTable {

    public static final String TABLE_NAME = "tbl_version_table";
    public static final String TABLE_COL_ID = "id";
    public static final String TABLE_COL_TABLE_NAME = "table_name";
    public static final String TABLE_COL_QUERY_TYPE = "query_type";
    public static final String TABLE_COL_NEW_RECORD = "new_record";
    public static final String TABLE_COL_OLD_RECORD = "old_record";
    public static final String TABLE_COL_QUERY_TIMESTAMP = "query_timestamp";

    /* Following list contains all the column names in the order as they exist in the real table. */
    public static final List<String> TABLE_COLS = new ArrayList<String>() {{
        add(TABLE_COL_ID);
        add(TABLE_COL_QUERY_TYPE);
        add(TABLE_COL_TABLE_NAME);
        add(TABLE_COL_NEW_RECORD);
        add(TABLE_COL_OLD_RECORD);
        add(TABLE_COL_QUERY_TIMESTAMP);
    }};

    private final DoerDatabase doerDatabase;

    /**
     * Constructor for DoerDBMetaTable
     * @param doerDatabase The DoerDatabase instance to which the DoerDBMetaTable belongs to.
     */
    public DoerDBMetaTable(DoerDatabase doerDatabase) {
        this.doerDatabase = doerDatabase;
    }

    /**
     * Used to obtain a set of records by the provided query.
     * @param query String The query to be executed on Meta Table.
     * @return List of HashMaps containing the data of the record queried if found, else null.
     * @throws SQLException                   If unexpected error occurs while querying the database.
     * @throws InitializationFailureException If DoerDB failed to initialize.
     */
    private List<Map<String, Object>> getRecordsInfoByQuery(String query) throws SQLException, InitializationFailureException {
        ResultSet resultsQueryInfo = this.doerDatabase.executeQuery(query);

        List<Map<String, Object>> recordsInfo = new ArrayList<>();
        while (resultsQueryInfo.next()) {
            Map<String, Object> mapQueryInfo = new HashMap<>();
            for (String tableColumn : TABLE_COLS) {
                mapQueryInfo.put(tableColumn, resultsQueryInfo.getObject(tableColumn));
            }

            recordsInfo.add(mapQueryInfo);
        }

        return recordsInfo;
    }

    /**
     * Used to filter and obtain a record by its ID on the Meta Table.
     * @param queryID The ID of the record to be filtered out.
     * @return If found, HashMap containing all the fields in DoerDBMetaTable.TABLE_COLS else null
     * @throws SQLException                   If unexpected error occurs while querying the database.
     * @throws InitializationFailureException If DoerDB failed to initialize.
     */
    public Map<String, Object> getRecordInfoByID(int queryID) throws SQLException, InitializationFailureException {
        String query = MySQL.SQL_SELECT_CLAUSE + MySQL.SQL_SPACE + "*" + MySQL.SQL_SPACE + MySQL.SQL_FROM_CLAUSE + MySQL.SQL_SPACE +
                MySQL.SQL_INTERNAL_QUOTES + TABLE_NAME + MySQL.SQL_INTERNAL_QUOTES +
                MySQL.SQL_SPACE + MySQL.SQL_WHERE_CLAUSE + MySQL.SQL_SPACE +
                MySQL.SQL_INTERNAL_QUOTES + DoerDBMetaTable.TABLE_COL_ID + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_EQUATOR +
                MySQL.SQL_EXTERNAL_QUOTES + String.valueOf(queryID) + MySQL.SQL_EXTERNAL_QUOTES;

        return this.getRecordsInfoByQuery(query).get(0);
    }

    /**
     * Updates the record with the given ID using the HashMap of column names mapping to the values.
     * @param queryID ID of the record in the Meta Table.
     * @param columnValues HashMap containing column names as keys mapping to the column names as the values.
     * @throws SQLException                   If unexpected error occurs while querying the database.
     * @throws InitializationFailureException If DoerDB failed to initialize.
     */
    public void updateRecordInfoByID(int queryID, Map<String, String> columnValues) throws SQLException, InitializationFailureException {
        String query = MySQL.SQL_UPDATE_PREFIX + MySQL.SQL_SPACE + MySQL.SQL_INTERNAL_QUOTES + DoerDBMetaTable.TABLE_NAME + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SPACE +
                MySQL.SQL_SET_OPERATOR + MySQL.SQL_SPACE;

        for (Map.Entry<String, String> columnValue : columnValues.entrySet()) {
            query += MySQL.SQL_INTERNAL_QUOTES + columnValue.getKey() + MySQL.SQL_INTERNAL_QUOTES +
                    MySQL.SQL_EQUATOR +
                    MySQL.SQL_EXTERNAL_QUOTES + columnValue.getValue() + MySQL.SQL_EXTERNAL_QUOTES +
                    MySQL.SQL_SEPARATOR;
        }
        query = query.substring(0, query.length() - 1) + MySQL.SQL_SPACE;

        query += MySQL.SQL_WHERE_CLAUSE + MySQL.SQL_SPACE +
                MySQL.SQL_INTERNAL_QUOTES + DoerDBMetaTable.TABLE_COL_ID + MySQL.SQL_INTERNAL_QUOTES +
                MySQL.SQL_EQUATOR +
                MySQL.SQL_EXTERNAL_QUOTES + String.valueOf(queryID) + MySQL.SQL_EXTERNAL_QUOTES;

        this.doerDatabase.executeUpdate(query);
    }

    /**
     * Used to obtain a List of HashMaps containing records' data by comparing the timestamp of the queries with the provided ID according to the comparator.
     * @param comparator String The Comparator to be used in the MySQL query to compare the query ID with the given ID.
     * @param thresholdID The threshold ID to be used for comparison(filtering) the queries.
     * @return List of HashMaps of records' data.
     * @throws SQLException                   If unexpected error occurs while querying the database.
     * @throws InitializationFailureException If DoerDB failed to initialize.
     */
    private List<Map<String, Object>> getRecordsByID(String comparator, int thresholdID) throws SQLException, InitializationFailureException {
        String query = MySQL.SQL_SELECT_CLAUSE + MySQL.SQL_SPACE + "*" + MySQL.SQL_SPACE + MySQL.SQL_FROM_CLAUSE + MySQL.SQL_SPACE +
                MySQL.SQL_INTERNAL_QUOTES + TABLE_NAME + MySQL.SQL_INTERNAL_QUOTES;
        if (thresholdID != -1) {
            query += MySQL.SQL_SPACE + MySQL.SQL_WHERE_CLAUSE + MySQL.SQL_SPACE +
                    MySQL.SQL_INTERNAL_QUOTES + DoerDBMetaTable.TABLE_COL_ID + MySQL.SQL_INTERNAL_QUOTES + comparator +
                    MySQL.SQL_EXTERNAL_QUOTES + String.valueOf(thresholdID) + MySQL.SQL_EXTERNAL_QUOTES;
        }

        return this.getRecordsInfoByQuery(query);
    }

    /**
     * Used to obtain a List of HashMaps containing records' data by comparing the timestamp of the queries with the provided timestamp according to the comparator.
     * @param comparator String The Comparator to be used in the MySQL query to compare the query timestamp with the given timestamp.
     * @param thresholdTimestamp The threshold timestamp to be used for comparison(filtering) the queries.
     * @return List of HashMaps of records' data.
     * @throws SQLException                   If unexpected error occurs while querying the database.
     * @throws InitializationFailureException If DoerDB failed to initialize.
     */
    private List<Map<String, Object>> getRecordsByTimestamp(String comparator, Date thresholdTimestamp) throws SQLException, InitializationFailureException {
        String formattedThresholdTimestamp = thresholdTimestamp != null ? MySQL.getFormattedTimestampSQL(thresholdTimestamp) : null;

        String query = MySQL.SQL_SELECT_CLAUSE + MySQL.SQL_SPACE + "*" + MySQL.SQL_SPACE + MySQL.SQL_FROM_CLAUSE + MySQL.SQL_SPACE +
                MySQL.SQL_INTERNAL_QUOTES + TABLE_NAME + MySQL.SQL_INTERNAL_QUOTES;
        if (thresholdTimestamp != null) {
            query += MySQL.SQL_SPACE + MySQL.SQL_WHERE_CLAUSE + MySQL.SQL_SPACE +
                    MySQL.SQL_INTERNAL_QUOTES + DoerDBMetaTable.TABLE_COL_QUERY_TIMESTAMP + MySQL.SQL_INTERNAL_QUOTES + comparator +
                    MySQL.SQL_EXTERNAL_QUOTES + String.valueOf(formattedThresholdTimestamp) + MySQL.SQL_EXTERNAL_QUOTES;
        }

        return this.getRecordsInfoByQuery(query);
    }

    /**
     * Used to obtain the ID of the latest query recorded in the Meta Table.
     * @return int ID of the latest query.
     * @throws SQLException                   If unexpected error occurs while querying the database.
     * @throws InitializationFailureException If DoerDB failed to initialize.
     */
    public int getLastQueryID() throws SQLException, InitializationFailureException {
        String query = MySQL.SQL_SELECT_CLAUSE + MySQL.SQL_SPACE + MySQL.SQL_INTERNAL_QUOTES + DoerDBMetaTable.TABLE_COL_ID + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SPACE +
                MySQL.SQL_FROM_CLAUSE + MySQL.SQL_SPACE + MySQL.SQL_INTERNAL_QUOTES + DoerDBMetaTable.TABLE_NAME + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SPACE +
                MySQL.SQL_ORDER_CLAUSE + MySQL.SQL_SPACE + MySQL.SQL_BY_OPERATOR + MySQL.SQL_SPACE +
                MySQL.SQL_INTERNAL_QUOTES + DoerDBMetaTable.TABLE_COL_ID + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SPACE +
                MySQL.SQL_SORT_DESC + MySQL.SQL_SPACE +
                MySQL.SQL_LIMIT_OPERATOR + MySQL.SQL_SPACE + "1";

        ResultSet resultLastQuery = this.doerDatabase.executeQuery(query);
        if (resultLastQuery.next()) {
            return resultLastQuery.getInt(DoerDBMetaTable.TABLE_COL_ID);
        }
        else {
            return -1;
        }
    }

    /**
     * Used to obtain the timestamp of the latest query recorded in the Meta Table.
     * @return Date Timestamp of the latest query.
     * @throws SQLException                   If unexpected error occurs while querying the database.
     * @throws InitializationFailureException If DoerDB failed to initialize.
     */
    public Date getLastQueryTimestamp() throws SQLException, InitializationFailureException {
        String query = MySQL.SQL_SELECT_CLAUSE + MySQL.SQL_SPACE + MySQL.SQL_INTERNAL_QUOTES + DoerDBMetaTable.TABLE_COL_QUERY_TIMESTAMP + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SPACE +
                MySQL.SQL_FROM_CLAUSE + MySQL.SQL_SPACE + MySQL.SQL_INTERNAL_QUOTES + DoerDBMetaTable.TABLE_NAME + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SPACE +
                MySQL.SQL_ORDER_CLAUSE + MySQL.SQL_SPACE + MySQL.SQL_BY_OPERATOR + MySQL.SQL_SPACE +
                MySQL.SQL_INTERNAL_QUOTES + DoerDBMetaTable.TABLE_COL_QUERY_TIMESTAMP + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SPACE +
                MySQL.SQL_SORT_DESC + MySQL.SQL_SPACE +
                MySQL.SQL_LIMIT_OPERATOR + MySQL.SQL_SPACE + "1";

        ResultSet resultLastQuery = this.doerDatabase.executeQuery(query);
        if (resultLastQuery.next()) {
            return MySQL.getFormattedTimestampDateSQL(resultLastQuery.getTimestamp(DoerDBMetaTable.TABLE_COL_QUERY_TIMESTAMP));
        }
        else {
            return null;
        }
    }

    /**
     * Used to obtain a List of HashMaps containing data(records) of the queries executed <b>before</b> a given ID(lesser ID).
     * @param thresholdID The threshold ID to be used for comparison(filtering) the queries.
     * @return List of HashMaps of records' data.
     * @throws SQLException                   If unexpected error occurs while querying the database.
     * @throws InitializationFailureException If DoerDB failed to initialize.
     */
    public List<Map<String, Object>> getQueryRecordsInfoBeforeID(int thresholdID) throws SQLException, InitializationFailureException {
        return this.getRecordsByID("<", thresholdID);
    }

    /**
     * Used to obtain a List of HashMaps containing data(records) of the queries executed <b>before</b> a given timestamp.
     * @param thresholdTimestamp The threshold timestamp to be used for comparison(filtering) the queries.
     * @return List of HashMaps of records' data.
     * @throws SQLException                   If unexpected error occurs while querying the database.
     * @throws InitializationFailureException If DoerDB failed to initialize.
     */
    public List<Map<String, Object>> getQueryRecordsInfoBeforeTimestamp(Date thresholdTimestamp) throws SQLException, InitializationFailureException {
        return this.getRecordsByTimestamp("<", thresholdTimestamp);
    }

    /**
     * Used to obtain a List of HashMaps containing data(records) of the queries executed <b>after</b> a given ID(higher ID).
     * @param thresholdID The threshold ID to be used for comparison(filtering) the queries.
     * @return List of HashMaps of records' data.
     * @throws SQLException                   If unexpected error occurs while querying the database.
     * @throws InitializationFailureException If DoerDB failed to initialize.
     */
    public List<Map<String, Object>> getQueryRecordsInfoAfterID(int thresholdID) throws SQLException, InitializationFailureException {
        return this.getRecordsByID(">", thresholdID);
    }

    /**
     * Used to obtain a List of HashMaps containing data(records) of the queries executed <b>after</b> a given timestamp.
     * @param thresholdTimestamp The threshold timestamp to be used for comparison(filtering) the queries.
     * @return List of HashMaps of records' data.
     * @throws SQLException                   If unexpected error occurs while querying the database.
     * @throws InitializationFailureException If DoerDB failed to initialize.
     */
    public List<Map<String, Object>> getQueryRecordsInfoAfterTimestamp(Date thresholdTimestamp) throws SQLException, InitializationFailureException {
        return this.getRecordsByTimestamp(">", thresholdTimestamp);
    }

}
