package com.doerit.doerdb.db.queries.executors;

import com.doerit.doerdb.db.queries.BasicQuery;
import com.doerit.doerdb.db.queries.UpdateQuery;
import com.doerit.doerdb.exceptions.InitializationFailureException;
import com.doerit.doerdb.synchronizer.DoerDBChange;
import com.doerit.doerdb.synchronizer.DoerDBSynchronizer;
import com.doerit.doerdb.synchronizer.mappers.ColumnMapper;
import com.doerit.doerdb.synchronizer.mappers.DatabaseMapper;
import com.doerit.doerdb.synchronizer.mappers.TableMapper;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.SQLException;

public class DoerDBChangeExecutor {

    private final DoerDBSynchronizer doerDBSynchronizer;

    /**
     * Constructor for DoerDBChangeExecutor
     * @param doerDBSynchronizer DoerDBSynchronizer Instance
     */
    public DoerDBChangeExecutor(DoerDBSynchronizer doerDBSynchronizer) {
        this.doerDBSynchronizer = doerDBSynchronizer;
    }

    /**
     * Executes a DoerDBChange on the relavant database.
     * @param doerDBChange DoerDBChange Instance that is needed to be executed.
     * @throws SQLException If any exception is thrown during the execution of MySQL query.
     * @throws InitializationFailureException If any exception is thrown during the initialization of DoerDatabase.
     */
    public void executeDoerDBChange(DoerDBChange doerDBChange) throws SQLException, InitializationFailureException {
        BasicQuery changeQuery = doerDBChange.getQuery();

        JSONObject oldNewRecord = changeQuery.getNewRecord();
        JSONArray oldNewRecordColumns = oldNewRecord.names();
        JSONObject oldOldRecord = changeQuery.getOldRecord();

        DoerDBChange.SyncDirection changeDirection = doerDBChange.getSyncDirection();
        DatabaseMapper databaseMapper = this.doerDBSynchronizer.getDoerDBMapper();

        String oppositeTableName;
        JSONObject newNewRecord = new JSONObject();
        JSONObject newOldRecord = changeQuery.getQueryType().equals(UpdateQuery.QUERY_TYPE) ? new JSONObject() : null;
        QueryExecutor queryExecutor;

        if (changeDirection == DoerDBChange.SyncDirection.LOCAL_TO_REMOTE) {
            TableMapper tableMapper = databaseMapper.getTableMapperByLocalTable(changeQuery.getTableName());
            oppositeTableName = tableMapper.getRemoteTableName();

            for (int i = 0; i < oldNewRecordColumns.length(); i++) {
                String oldRecordColumn = oldNewRecordColumns.getString(i);
                ColumnMapper columnMapper = tableMapper.getColumnMapperByLocalColumn(oldRecordColumn);

                String newRecordColumn = columnMapper.getRemoteColumnName();
                String newNewRecordValue = oldNewRecord.getString(oldRecordColumn);
                // Prevent the column from synchronizing by eliminating it from the columns list.
                if (newRecordColumn != null) {
                    newNewRecord.put(newRecordColumn, newNewRecordValue);

                    if (changeQuery.getQueryType().equals(UpdateQuery.QUERY_TYPE)) {
                        String newOldRecordValue = oldOldRecord.getString(oldRecordColumn);
                        newOldRecord.put(newRecordColumn, newOldRecordValue);
                    }
                }
            }

            queryExecutor = this.doerDBSynchronizer.getDoerDB().getRemoteDatabase().getQueryExecutor();
        }
        else {
            TableMapper tableMapper = databaseMapper.getTableMapperByRemoteTable(changeQuery.getTableName());
            oppositeTableName = tableMapper.getLocalTableName();

            for (int i = 0; i < oldNewRecordColumns.length(); i++) {
                String oldRecordColumn = oldNewRecordColumns.getString(i);
                ColumnMapper columnMapper = tableMapper.getColumnMapperByRemoteColumn(oldRecordColumn);

                String newRecordColumn = columnMapper.getLocalColumnName();
                String newNewRecordValue = oldNewRecord.getString(oldRecordColumn);
                // Prevent the column from synchronizing by eliminating it from the columns list.
                if (newRecordColumn != null) {
                    newNewRecord.put(newRecordColumn, newNewRecordValue);

                    if (changeQuery.getQueryType().equals(UpdateQuery.QUERY_TYPE)) {
                        String newOldRecordValue = oldOldRecord.getString(oldRecordColumn);
                        newOldRecord.put(newRecordColumn, newOldRecordValue);
                    }
                }
            }

            queryExecutor = this.doerDBSynchronizer.getDoerDB().getLocalDatabase().getQueryExecutor();
        }

        if (newNewRecord.length() > 0) {
            changeQuery.setTableName(oppositeTableName);
            changeQuery.setNewRecord(newNewRecord);
            changeQuery.setOldRecord(newOldRecord);

            queryExecutor.executeQuery(changeQuery);
        }
    }
}
