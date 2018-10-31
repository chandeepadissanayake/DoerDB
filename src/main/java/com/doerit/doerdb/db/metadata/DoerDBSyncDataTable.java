package com.doerit.doerdb.db.metadata;

import com.doerit.doerdb.db.DoerDatabase;
import com.doerit.doerdb.db.MySQL;
import com.doerit.doerdb.exceptions.InitializationFailureException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DoerDBSyncDataTable {

    public static final String TABLE_NAME = "tbl_sync_data";
    public static final String TABLE_COL_ID = "id";
    public static final String TABLE_COL_LOCAL_LAST_ID = "local_last_id";
    public static final String TABLE_COL_REMOTE_LAST_ID = "remote_last_id";

    /* Following list contains all the column names in the order as they exist in the real table. */
    public static final List<String> TABLE_COLS = new ArrayList<String>() {{
        add(TABLE_COL_ID);
        add(TABLE_COL_LOCAL_LAST_ID);
        add(TABLE_COL_REMOTE_LAST_ID);
    }};

    private final DoerDatabase doerDatabase;

    /**
     * Constructor for DoerDBSyncDataTable
     * @param doerDatabase The DoerDatabase instance to which the DoerDBSyncDataTable belongs to.
     */
    public DoerDBSyncDataTable(DoerDatabase doerDatabase) {
        this.doerDatabase = doerDatabase;
    }

    /**
     * Sets the Last Synchronized IDs of the local and remote Meta Tables.
     * @param localID int The last synchronized ID of the local Meta Table.
     * @param remoteID int The last synchronized ID of the remote Meta Table.
     * @throws SQLException If any exception is thrown during the execution of MySQL query.
     * @throws InitializationFailureException If any exception is thrown during the initialization of DoerDatabase.
     */
    public void setLastSyncIDs(int localID, int remoteID) throws SQLException, InitializationFailureException {
        String query = MySQL.SQL_INSERT_PREFIX + MySQL.SQL_SPACE + MySQL.SQL_INTERNAL_QUOTES + DoerDBSyncDataTable.TABLE_NAME + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_BRACKET_ROUND_OPEN +
                MySQL.SQL_INTERNAL_QUOTES + DoerDBSyncDataTable.TABLE_COL_LOCAL_LAST_ID + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SEPARATOR +
                MySQL.SQL_INTERNAL_QUOTES + DoerDBSyncDataTable.TABLE_COL_REMOTE_LAST_ID + MySQL.SQL_INTERNAL_QUOTES +
                MySQL.SQL_BRACKET_ROUND_CLOSE + MySQL.SQL_SPACE +
                MySQL.SQL_INSERT_VALUES + MySQL.SQL_SPACE + MySQL.SQL_BRACKET_ROUND_OPEN +
                String.valueOf(localID) + MySQL.SQL_SEPARATOR +
                String.valueOf(remoteID) +
                MySQL.SQL_BRACKET_ROUND_CLOSE;

        this.doerDatabase.executeUpdate(query);
    }

    /**
     * Used to obtain the last entered record of the Meta Tables.
     * @return ResultSet MySQL ResultSet representing the last record of the DoerDBSyncDataTable.
     * @throws SQLException If any exception is thrown during the execution of MySQL query.
     * @throws InitializationFailureException If any exception is thrown during the initialization of DoerDatabase.
     */
    private ResultSet getLastRecord() throws SQLException, InitializationFailureException {
        String query = MySQL.SQL_SELECT_CLAUSE + MySQL.SQL_SPACE + "*" + MySQL.SQL_SPACE + MySQL.SQL_FROM_CLAUSE + MySQL.SQL_SPACE +
                MySQL.SQL_INTERNAL_QUOTES + DoerDBSyncDataTable.TABLE_NAME + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SPACE +
                MySQL.SQL_ORDER_CLAUSE + MySQL.SQL_SPACE + MySQL.SQL_BY_OPERATOR + MySQL.SQL_SPACE +
                MySQL.SQL_INTERNAL_QUOTES + DoerDBSyncDataTable.TABLE_COL_ID + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SPACE +
                MySQL.SQL_SORT_DESC + MySQL.SQL_SPACE +
                MySQL.SQL_LIMIT_OPERATOR + MySQL.SQL_SPACE + "1";

        return this.doerDatabase.executeQuery(query);
    }

    /**
     * Returns the value of the last record's column(specifically ID)
     * @param columnName The name of the column whose value is to be obtained.
     * @return int The value(ID) related to the column.
     * @throws SQLException If any exception is thrown during the execution of MySQL query.
     * @throws InitializationFailureException If any exception is thrown during the initialization of DoerDatabase.
     */
    private int getLastID(String columnName) throws SQLException, InitializationFailureException {
        ResultSet resultSyncLast = this.getLastRecord();
        if (resultSyncLast.next()) {
            return resultSyncLast.getInt(columnName);
        }
        else {
            return -1;
        }
    }

    /**
     * Used to obtain the last synchronized ID of the local Meta Table(Local Database's Meta Table ID).
     * @return int Last Synchronized Local Meta Table ID
     * @throws SQLException If any exception is thrown during the execution of MySQL query.
     * @throws InitializationFailureException If any exception is thrown during the initialization of DoerDatabase.
     */
    public int getLastLocalID() throws SQLException, InitializationFailureException {
        return this.getLastID(DoerDBSyncDataTable.TABLE_COL_LOCAL_LAST_ID);
    }

    /**
     * Used to obtain the last synchronized ID of the remote Meta Table(Remote Database's Meta Table ID).
     * @return int Last Synchronized Remote Meta Table ID
     * @throws SQLException If any exception is thrown during the execution of MySQL query.
     * @throws InitializationFailureException If any exception is thrown during the initialization of DoerDatabase.
     */
    public int getLastRemoteID() throws SQLException, InitializationFailureException {
        return this.getLastID(DoerDBSyncDataTable.TABLE_COL_REMOTE_LAST_ID);
    }

}
