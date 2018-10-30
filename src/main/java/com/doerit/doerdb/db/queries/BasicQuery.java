package com.doerit.doerdb.db.queries;

import org.json.JSONObject;

import java.util.Date;

/**
 * BasicQuery provides the interface for each query in the MetaTable
 */
public abstract class BasicQuery {

    protected static final String EXCEPTION_MESSAGE_COLUMN_KEY_NOT_FOUND = "The column name is not found in the query.";
    public static final String QUERY_NULL = "@NULL";
    public static String QUERY_TYPE = null;

    private int queryID = -1;
    private String implQueryType = null;
    private String tableName = null;
    private JSONObject newRecord = null;
    private JSONObject oldRecord = null;
    private Date queryTimestamp = null;

    public void setQueryID(int queryID) {
        this.queryID = queryID;
    }

    public int getQueryID() {
        return queryID;
    }

    /**
     * Sets the Query Type for the implementation.
     * @param queryType String Type of the Query. Consult queryType constant string of the respective implementation.
     */
    public void setQueryType(String queryType) {
        this.implQueryType = queryType;
    }

    /**
     * Returns the Query type of the implementation.
     * @return String Type of the Query(from the Implementation).
     */
    public String getQueryType() {
        return implQueryType;
    }

    /**
     * Returns the new record associated with the table row after executing the query.
     * @return JSONObject The New Record
     */
    public JSONObject getNewRecord() {
        return newRecord;
    }

    /**
     * Sets the new record of the query.
     * @param newRecord JSONObject The New Record to be set.
     */
    public void setNewRecord(JSONObject newRecord) {
        this.newRecord = newRecord;
    }

    /**
     * Returns the old record associated with the table row before executing the query.
     * @return JSONObject The Old Record
     */
    public JSONObject getOldRecord() {
        return oldRecord;
    }

    /**
     * Sets the old record of the query.
     * @param oldRecord JSONObject The Old Record to be set.
     */
    public void setOldRecord(JSONObject oldRecord) {
        this.oldRecord = oldRecord;
    }

    /**
     * Sets the Timestamp at which the query should be executed.
     * @param queryTimestamp Date The timestamp on which the query should be executed.
     */
    public void setQueryTimestamp(Date queryTimestamp) {
        this.queryTimestamp = queryTimestamp;
    }

    /**
     * Used to obtain the Timestamp at which the query should be executed.
     * @return Date Timestamp at which the query should be executed.
     */
    public Date getQueryTimestamp() {
        return queryTimestamp;
    }

    /**
     * Sets the table on which the query is executed.
     * @param tableName String The name of the table on which MySQL query has been executed.
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Used to obtain the table on which the query is executed.
     * @return String The name of the table on which the query is executed.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Used to obtain MySQL query from the provided data.
     * @param queryTimestamp The timestamp at which the query was executed on the databases.
     * @return String MySQL query.
     */
    public abstract String getMySQLQuery(Date queryTimestamp);

    /**
     * Determines whether the current Basic Query's Old Record(the record entries before executing the query) is similar to that of another BasicQuery instance.
     * This will be valid only in the context of two UpdateQueries whereas all other comparisons would return false.
     * @param otherQuery BasicQuery The other BasicQuery to be compared to.
     * @return boolean true if the Old Records are similar, false otherwise.
     */
    public boolean compareOldRecordTo(BasicQuery otherQuery) {
        if (this.getQueryType().equals(UpdateQuery.QUERY_TYPE) && otherQuery.getQueryType().equals(UpdateQuery.QUERY_TYPE)) {
            UpdateQuery thisUpdateQuery = (UpdateQuery)this;
            UpdateQuery otherUpdateQuery = (UpdateQuery)otherQuery;

            JSONObject thisOldRecord = thisUpdateQuery.getOldRecord();
            JSONObject otherOldRecord = otherUpdateQuery.getOldRecord();

            return thisOldRecord.similar(otherOldRecord);
        }
        else {
            return false;
        }
    }

}
