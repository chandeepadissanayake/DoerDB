package com.doerit.doerdb.db.metadata;

import com.doerit.doerdb.db.DoerDatabase;
import com.doerit.doerdb.db.MySQL;
import com.doerit.doerdb.exceptions.InitializationFailureException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DoerDBSyncStatusTable {

    public static final String TABLE_NAME = "tbl_sync_status";
    public static final String TABLE_COL_SYNC_STATUS = "sync_status";

    /* Following list contains all the column names in the order as they exist in the real table. */
    public static final List<String> TABLE_COLS = new ArrayList<String>() {{
        add(TABLE_COL_SYNC_STATUS);
    }};

    private final DoerDatabase doerDatabase;

    /**
     * Constructor for DoerDBSyncStatusTable
     * @param doerDatabase The DoerDatabase instance to which the DoerDBSyncStatusTable belongs to.
     */
    public DoerDBSyncStatusTable(DoerDatabase doerDatabase) {
        this.doerDatabase = doerDatabase;
    }

    /**
     * Used to obtain the current synchronization status of the related DoerDatabase instance.
     * @return boolean true if related DoerDatabase is under a synchronization process, false otherwise.
     * @throws SQLException If any exception is thrown during the execution of MySQL query.
     * @throws InitializationFailureException If any exception is thrown during the initialization of DoerDatabase.
     */
    public boolean getSyncStatus() throws SQLException, InitializationFailureException {
        String query = MySQL.SQL_SELECT_CLAUSE + MySQL.SQL_SPACE + "*" + MySQL.SQL_SPACE + MySQL.SQL_FROM_CLAUSE + MySQL.SQL_SPACE +
                MySQL.SQL_INTERNAL_QUOTES + DoerDBSyncStatusTable.TABLE_NAME + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SPACE +
                MySQL.SQL_WHERE_CLAUSE + MySQL.SQL_SPACE +
                "1";

        ResultSet resultSyncStatus = this.doerDatabase.executeQuery(query);
        return resultSyncStatus.next() && resultSyncStatus.getBoolean(DoerDBSyncStatusTable.TABLE_COL_SYNC_STATUS);
    }

    /**
     * Sets the current synchronization status of the related DoerDatabase instance.
     * @param state boolean true if related DoerDatabase is under a synchronization process, false otherwise.
     * @throws SQLException If any exception is thrown during the execution of MySQL query.
     * @throws InitializationFailureException If any exception is thrown during the initialization of DoerDatabase.
     */
    public void setSyncStatus(boolean state) throws SQLException, InitializationFailureException {
        String query = MySQL.SQL_UPDATE_PREFIX + MySQL.SQL_SPACE +
                MySQL.SQL_INTERNAL_QUOTES + DoerDBSyncStatusTable.TABLE_NAME + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SPACE +
                MySQL.SQL_SET_OPERATOR + MySQL.SQL_SPACE +
                MySQL.SQL_INTERNAL_QUOTES + DoerDBSyncStatusTable.TABLE_COL_SYNC_STATUS + MySQL.SQL_INTERNAL_QUOTES +
                MySQL.SQL_EQUATOR +
                (state ? "1" : "0") + MySQL.SQL_SPACE +
                MySQL.SQL_WHERE_CLAUSE + MySQL.SQL_SPACE +
                "1";

        this.doerDatabase.executeUpdate(query);
    }

}
