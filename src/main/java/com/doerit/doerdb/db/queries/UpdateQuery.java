package com.doerit.doerdb.db.queries;

import com.doerit.doerdb.db.MySQL;
import com.doerit.doerdb.synchronizer.DoerDBSynchronizer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Date;

public class UpdateQuery extends BasicQuery {

    public static final String QUERY_TYPE = "UPDATE";

    /**
     * Constructor for Query class representing MySQL Update queries.
     * @param queryID int The ID of the query in the Meta Table.
     * @param tableName String The name of the table affected by the Query.
     * @param newRecord JSONObject The new record whose keys are the column names of the table and values are the new values.
     * @param oldRecord JSONObject The old record whose keys are the column names of the table and values are the old values.
     * @param queryTimestamp Date The timestamp of the time at which the query was generated.
     */
    public UpdateQuery(int queryID, String tableName, JSONObject newRecord, JSONObject oldRecord, Date queryTimestamp) {
        setQueryID(queryID);
        setQueryType(UpdateQuery.QUERY_TYPE);
        setTableName(tableName);
        setNewRecord(newRecord);
        setOldRecord(oldRecord);
        setQueryTimestamp(queryTimestamp);
    }

    /**
     * Used to obtain MySQL Update query from the provided data.
     * @param queryTimestamp The timestamp at which the query was executed on the databases.
     * @return String MySQL Update query.
     */
    @Override
    public String getMySQLQuery(Date queryTimestamp) {
        JSONObject newRecord = this.getNewRecord();
        JSONObject oldRecord = this.getOldRecord();

        String subQuerySet = "";
        JSONArray setQueryColumnNames = newRecord.names();
        for (int i = 0; i < setQueryColumnNames.length(); i++) {
            String setQueryColumnName = setQueryColumnNames.getString(i);
            String setQueryColumnValue = newRecord.getString(setQueryColumnName);

            subQuerySet += MySQL.SQL_INTERNAL_QUOTES + setQueryColumnName + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_EQUATOR +
                    MySQL.SQL_EXTERNAL_QUOTES + setQueryColumnValue + MySQL.SQL_EXTERNAL_QUOTES + MySQL.SQL_SEPARATOR;
        }
        subQuerySet = subQuerySet.substring(0, subQuerySet.length() - 1);

        String subQueryWhere = "";
        String subQueryWhereSeparator = MySQL.SQL_SPACE + MySQL.SQL_AND_OPERATOR + MySQL.SQL_SPACE;
        JSONArray whereQueryColumnNames = oldRecord.names();
        for (int i = 0; i < whereQueryColumnNames.length(); i++) {
            String whereQueryColumnName = whereQueryColumnNames.getString(i);
            String whereQueryColumnValue = oldRecord.getString(whereQueryColumnName);

            subQueryWhere += MySQL.SQL_INTERNAL_QUOTES + whereQueryColumnName + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_EQUATOR +
                    MySQL.SQL_EXTERNAL_QUOTES + whereQueryColumnValue + MySQL.SQL_EXTERNAL_QUOTES + subQueryWhereSeparator;
        }
        subQueryWhere = subQueryWhere.substring(0, subQueryWhere.length() - subQueryWhereSeparator.length());

        String queryMySQL = MySQL.SQL_SET_OPERATOR + MySQL.SQL_SPACE + DoerDBSynchronizer.MYSQL_TAG_QUERY_TIMESTAMP + MySQL.SQL_EQUATOR + MySQL.SQL_EXTERNAL_QUOTES + MySQL.getFormattedTimestampSQL(queryTimestamp) + MySQL.SQL_EXTERNAL_QUOTES + ";" + MySQL.SQL_SPACE +
                MySQL.SQL_UPDATE_PREFIX + MySQL.SQL_SPACE + MySQL.SQL_INTERNAL_QUOTES + this.getTableName() + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SPACE +
                MySQL.SQL_SET_OPERATOR + MySQL.SQL_SPACE + subQuerySet + MySQL.SQL_SPACE +
                MySQL.SQL_WHERE_CLAUSE + MySQL.SQL_SPACE + subQueryWhere;

        return queryMySQL;
    }

}
