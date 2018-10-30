package com.doerit.doerdb.db.metadata;

import com.doerit.doerdb.db.DoerDatabase;
import com.doerit.doerdb.db.MySQL;
import com.doerit.doerdb.exceptions.InitializationFailureException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DoerDBSyncTable {

    public static final String TABLE_NAME = "tbl_sync_data";
    public static final String TABLE_COL_ID = "id";
    public static final String TABLE_COL_SYNCED_AT = "synced_at";

    /* Following list contains all the column names in the order as they exist in the real table. */
    public static final List<String> TABLE_COLS = new ArrayList<String>() {{
        add(TABLE_COL_ID);
        add(TABLE_COL_SYNCED_AT);
    }};

    private final DoerDatabase doerDatabase;

    public DoerDBSyncTable(DoerDatabase doerDatabase) {
        this.doerDatabase = doerDatabase;
    }

    public void setSyncTime(Date syncTimestamp) throws SQLException, InitializationFailureException {
        String formattedSyncTimestamp = MySQL.getFormattedTimestampSQL(syncTimestamp);

        String query = MySQL.SQL_INSERT_PREFIX + MySQL.SQL_SPACE + MySQL.SQL_INTERNAL_QUOTES + DoerDBSyncTable.TABLE_NAME + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_BRACKET_ROUND_OPEN +
                MySQL.SQL_INTERNAL_QUOTES + DoerDBSyncTable.TABLE_COL_SYNCED_AT + MySQL.SQL_INTERNAL_QUOTES +
                MySQL.SQL_BRACKET_ROUND_CLOSE + MySQL.SQL_SPACE +
                MySQL.SQL_INSERT_VALUES + MySQL.SQL_SPACE + MySQL.SQL_BRACKET_ROUND_OPEN +
                MySQL.SQL_EXTERNAL_QUOTES + formattedSyncTimestamp + MySQL.SQL_EXTERNAL_QUOTES +
                MySQL.SQL_BRACKET_ROUND_CLOSE;

        this.doerDatabase.executeUpdate(query);
    }

    public Date getLastSyncTime() throws SQLException, InitializationFailureException {
        String query = MySQL.SQL_SELECT_CLAUSE + MySQL.SQL_SPACE + "*" + MySQL.SQL_SPACE + MySQL.SQL_FROM_CLAUSE + MySQL.SQL_SPACE +
                MySQL.SQL_INTERNAL_QUOTES + DoerDBSyncTable.TABLE_NAME + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SPACE +
                MySQL.SQL_ORDER_CLAUSE + MySQL.SQL_SPACE + MySQL.SQL_BY_OPERATOR + MySQL.SQL_SPACE +
                MySQL.SQL_INTERNAL_QUOTES + DoerDBSyncTable.TABLE_COL_SYNCED_AT + MySQL.SQL_INTERNAL_QUOTES + MySQL.SQL_SPACE +
                MySQL.SQL_SORT_DESC + MySQL.SQL_SPACE +
                MySQL.SQL_LIMIT_OPERATOR + MySQL.SQL_SPACE + "1";

        ResultSet resultSyncLast = this.doerDatabase.executeQuery(query);
        if (resultSyncLast.next()) {
            return MySQL.getFormattedTimestampDateSQL(resultSyncLast.getTimestamp(DoerDBSyncTable.TABLE_COL_SYNCED_AT));
        }
        else {
            return null;
        }
    }

}
