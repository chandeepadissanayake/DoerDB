package com.doerit.doerdb.synchronizer;

import com.doerit.doerdb.db.DoerDatabase;
import com.doerit.doerdb.db.metadata.DoerDBMetaTable;
import com.doerit.doerdb.db.queries.BasicQuery;
import com.doerit.doerdb.exceptions.InitializationFailureException;
import com.doerit.doerdb.synchronizer.mappers.TableMapper;
import org.json.JSONObject;

import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DoerDBChange implements Comparable<DoerDBChange> {

    /**
     * Enum indicating the direction of a DoerDBChange.
     */
    public enum SyncDirection {
        LOCAL_TO_REMOTE,
        REMOTE_TO_LOCAL
    }

    private final DoerDatabase doerDatabase;
    private final SyncDirection syncDirection;
    private final BasicQuery changeQuery;

    /**
     * Constructor for DoerDBChange.
     * @param doerDatabase DoerDatabase The DoerDatabase which the change is associated with.
     * @param syncDirection SyncDirection The Direction of Synchronization.
     * @param changeQuery BasicQuery The query associated with the change.
     */
    public DoerDBChange(DoerDatabase doerDatabase, SyncDirection syncDirection, BasicQuery changeQuery) {
        this.doerDatabase = doerDatabase;
        this.syncDirection = syncDirection;
        this.changeQuery = changeQuery;
    }

    /**
     * Used to obtain the SyncDirection associated with the change.
     * @return SyncDirection The direction of the change.
     */
    public SyncDirection getSyncDirection() {
        return syncDirection;
    }

    /**
     * Used to obtain the BasicQuery associated with the change.
     * @return BasicQuery The query associated with the change.
     */
    public BasicQuery getQuery() {
        return changeQuery;
    }

    /**
     * Redirected method to obtain the Query ID(in the Meta Table) of the BasicQuery associated with the change.
     * @return int The Query ID in the Meta Table for the BasicQuery associated.
     */
    public int getQueryID() {
        return this.changeQuery.getQueryID();
    }

    /**
     * Redirected method to obtain the query timestamp(in the Meta Table) of the BasicQuery associated with the change.
     * @return Date The timestamp of which the query associated with the change was executed.
     */
    public Date getChangeTimestamp() {
        return this.changeQuery.getQueryTimestamp();
    }

    /**
     * Overridden method to compare two DoerDBChanges.
     * Compares by using the timestamps of the BasicQueries.
     * @param doerDBChange The other DoerDBChange instance to compare with.
     * @return int The position after comparison.
     */
    @Override
    public int compareTo(DoerDBChange doerDBChange) {
        return this.getChangeTimestamp().compareTo(doerDBChange.getChangeTimestamp());
    }

    /**
     * Updates the Old Record associated with the Basic Query implementation associated with the change.
     * @param newOldRecord The new Old Record to be set.
     * @throws SQLException If any exception is thrown during the execution of MySQL query.
     * @throws InitializationFailureException If any exception is thrown during the initialization of DoerDatabase.
     */
    public void updateOldRecord(JSONObject newOldRecord) throws SQLException, InitializationFailureException {
        DoerDBMetaTable databaseMetaTable = this.doerDatabase.getMetaTable();
        Map<String, String> columnValues = new HashMap<>();
        columnValues.put(DoerDBMetaTable.TABLE_COL_OLD_RECORD, newOldRecord.toString());
        databaseMetaTable.updateRecordInfoByID(this.getQueryID(), columnValues);

        this.changeQuery.setOldRecord(newOldRecord);
    }

}
