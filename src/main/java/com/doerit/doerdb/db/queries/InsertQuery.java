package com.doerit.doerdb.db.queries;

import com.doerit.doerdb.db.MySQL;
import com.doerit.doerdb.synchronizer.DoerDBSynchronizer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Date;

public class InsertQuery extends BasicQuery {

    public static final String QUERY_TYPE = "INSERT";

    private static final String PLACEHOLDER_QUERY_COLUMN_NAMES = "[COLUMN_NAMES]";
    private static final String PLACEHOLDER_QUERY_VALUES = "[COLUMN_VALUES]";

    /**
     * Constructor for Query class representing MySQL Update queries.
     * @param queryID int The ID of the query in the Meta Table.
     * @param tableName String The name of the table affected by the Query.
     * @param newRecord JSONObject The new record whose keys are the column names of the table and values are the new values.
     * @param queryTimestamp Date The timestamp of the time at which the query was generated.
     */
    public InsertQuery(int queryID, String tableName, JSONObject newRecord, Date queryTimestamp) {
        setQueryID(queryID);
        setQueryType(InsertQuery.QUERY_TYPE);
        setTableName(tableName);
        setNewRecord(newRecord);
        setQueryTimestamp(queryTimestamp);
    }

    /**
     * Used to obtain Insert MySQL query from the provided data.
     * @param queryTimestamp The timestamp at which the query was executed on the databases.
     * @return String Insert MySQL query.
     */
    @Override
    public String getMySQLQuery(Date queryTimestamp) {
        JSONObject newRecord = this.getNewRecord();

        String columnNames = "", columnValues = "";
        JSONArray jsonColumnNames = newRecord.names();
        for (int i = 0; i < jsonColumnNames.length(); i++) {
            String columnName = jsonColumnNames.getString(i);
            String columnValue = newRecord.getString(columnName);

            columnNames += MySQL.SQL_INTERNAL_QUOTES + columnName + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SEPARATOR;
            columnValues += MySQL.SQL_EXTERNAL_QUOTES + columnValue + MySQL.SQL_EXTERNAL_QUOTES + MySQL.SQL_SEPARATOR;
        }
        columnNames = columnNames.substring(0, columnNames.length() - 1);
        columnValues = columnValues.substring(0, columnValues.length() - 1);

        String queryMySQL = (MySQL.SQL_SET_OPERATOR + MySQL.SQL_SPACE + DoerDBSynchronizer.MYSQL_TAG_QUERY_TIMESTAMP + MySQL.SQL_EQUATOR + MySQL.SQL_EXTERNAL_QUOTES + MySQL.getFormattedTimestampSQL(queryTimestamp) + MySQL.SQL_EXTERNAL_QUOTES + ";" + MySQL.SQL_SPACE +
                MySQL.SQL_INSERT_PREFIX + MySQL.SQL_SPACE + MySQL.SQL_INTERNAL_QUOTES + this.getTableName() + MySQL.SQL_INTERNAL_QUOTES +
                MySQL.SQL_BRACKET_ROUND_OPEN + InsertQuery.PLACEHOLDER_QUERY_COLUMN_NAMES + MySQL.SQL_BRACKET_ROUND_CLOSE + MySQL.SQL_SPACE +
                MySQL.SQL_INSERT_VALUES + MySQL.SQL_SPACE +
                MySQL.SQL_BRACKET_ROUND_OPEN + InsertQuery.PLACEHOLDER_QUERY_VALUES + MySQL.SQL_BRACKET_ROUND_CLOSE)
                .replace(InsertQuery.PLACEHOLDER_QUERY_COLUMN_NAMES, columnNames)
                .replace(InsertQuery.PLACEHOLDER_QUERY_VALUES, columnValues);

        return queryMySQL;
    }

}
