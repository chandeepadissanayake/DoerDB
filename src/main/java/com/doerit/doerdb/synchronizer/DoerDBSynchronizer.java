package com.doerit.doerdb.synchronizer;

import com.doerit.doerdb.DoerDB;
import com.doerit.doerdb.db.DoerDatabase;
import com.doerit.doerdb.db.metadata.DoerDBMetaTable;
import com.doerit.doerdb.db.metadata.DoerDBSyncDataTable;
import com.doerit.doerdb.db.metadata.DoerDBSyncStatusTable;
import com.doerit.doerdb.db.queries.BasicQuery;
import com.doerit.doerdb.db.queries.builders.QueryBuilder;
import com.doerit.doerdb.db.queries.executors.DoerDBChangeExecutor;
import com.doerit.doerdb.exceptions.ExceptionCodes;
import com.doerit.doerdb.exceptions.InitializationFailureException;
import com.doerit.doerdb.exceptions.SynchronizeException;
import com.doerit.doerdb.synchronizer.mappers.DatabaseMapper;

import java.sql.SQLException;
import java.util.*;

public class DoerDBSynchronizer {

    public static final String MYSQL_TAG_QUERY_TIMESTAMP = "@QUERY_TIMESTAMP";

    private final DoerDB doerDB;
    private final DatabaseMapper doerDBMapper;
    private final DoerDBChangeExecutor doerDBChangeExecutor;

    /**
     * Basic Constructor for DoerDBSynchronizer
     * @param doerDB DoerDB Instance created previously.
     * @param doerDBMapper DatabaseMapper Mapper for Database Tables.
     */
    public DoerDBSynchronizer(DoerDB doerDB, DatabaseMapper doerDBMapper) {
        this.doerDB = doerDB;
        this.doerDBMapper = doerDBMapper;

        this.doerDBChangeExecutor = new DoerDBChangeExecutor(this);
    }

    /**
     * Constructor for DoerDBSynchronizer.
     * @param doerDB DoerDB instance.
     * @throws SQLException If any exception is thrown during the execution of MySQL queries internally.
     * @throws InitializationFailureException If any exception is thrown during the initialization of DoerDatabases.
     */
    public DoerDBSynchronizer(DoerDB doerDB) throws SQLException, InitializationFailureException {
        this(doerDB, new DatabaseMapper(doerDB));
    }

    /**
     * Used to obtain a mapper for DoerDB's Tables and their columns.
     * @return DatabaseMapper Mapper for the DoerDB instance.
     */
    public DatabaseMapper getDoerDBMapper() {
        return doerDBMapper;
    }

    /**
     * Used to obtain the DoerDB instance associated.
     * @return DoerDB An instance of DoerDB.
     */
    public DoerDB getDoerDB() {
        return doerDB;
    }

    /**
     * Method to parse the changes in Meta Table as Queries after the given threshold ID.
     * @param doerDBMetaTable DoerDBMetaTable The Meta Table where queries are to be parsed.
     * @param thresholdID Date The threshold timestamp to be used to obtain the executed queries.
     * @return List of BasicQuery implementations for each change in the meta table.
     * @throws SQLException If any exception is thrown during the execution of MySQL query.
     * @throws InitializationFailureException If any exception is thrown during the initialization of DoerDatabase.
     */
    private List<BasicQuery> getChangesAsQueries(DoerDBMetaTable doerDBMetaTable, int thresholdID) throws SQLException, InitializationFailureException {
        List<Map<String, Object>> resultsChanges = doerDBMetaTable.getQueryRecordsInfoAfterID(thresholdID);
        List<BasicQuery> changes = new ArrayList<>();
        for (Map<String, Object> resultsChange : resultsChanges) {
            QueryBuilder changeQueryBuilder = new QueryBuilder(resultsChange);
            BasicQuery changeQuery = changeQueryBuilder.getQuery();
            if (changeQuery != null) {
                changes.add(changeQuery);
            }
        }

        return changes;
    }

    /**
     * Used to obtain the changes recorded in the local Meta Table after the given ID.
     * @param thresholdID Date The threshold timestamp to be used to obtain the executed queries.
     * @return List of BasicQuery implementations for each change in the meta table.
     * @throws SQLException If any exception is thrown during the execution of MySQL query.
     * @throws InitializationFailureException If any exception is thrown during the initialization of DoerDatabase.
     */
    private List<BasicQuery> getLocalChangesAsQueries(int thresholdID) throws SQLException, InitializationFailureException {
        DoerDatabase doerLocalDB = doerDB.getLocalDatabase();
        DoerDBMetaTable doerDBMetaTable = doerLocalDB.getMetaTable();
        return this.getChangesAsQueries(doerDBMetaTable, thresholdID);
    }

    /**
     * Used to obtain the changes recorded in the remote Meta Table after the given ID.
     * @param thresholdID Date The threshold timestamp to be used to obtain the executed queries.
     * @return List of BasicQuery implementations for each change in the meta table.
     * @throws SQLException If any exception is thrown during the execution of MySQL query.
     * @throws InitializationFailureException If any exception is thrown during the initialization of DoerDatabase.
     */
    private List<BasicQuery> getRemoteChangesAsQueries(int thresholdID) throws SQLException, InitializationFailureException {
        DoerDatabase doerRemoteDB = doerDB.getRemoteDatabase();
        DoerDBMetaTable doerDBMetaTable = doerRemoteDB.getMetaTable();
        return this.getChangesAsQueries(doerDBMetaTable, thresholdID);
    }

    /**
     * Used to obtain a set of changes as DoerDBChange instances characterized by the direction of Synchronizing.
     * @param syncDirection DoerDBChange.SyncDirection The direction of synchronizing for the List of BasicQueries.
     * @param queries List of BasicQuery implementations.
     * @return List of DoerDBChange instances.
     */
    private List<DoerDBChange> getChangesByQueries(DoerDBChange.SyncDirection syncDirection, List<BasicQuery> queries) {
        List<DoerDBChange> changes = new ArrayList<>();
        for (BasicQuery query : queries) {
            DoerDatabase databaseForChange = syncDirection == DoerDBChange.SyncDirection.LOCAL_TO_REMOTE ? this.doerDB.getLocalDatabase() : this.doerDB.getRemoteDatabase();
            changes.add(new DoerDBChange(databaseForChange, syncDirection, query));
        }

        return changes;
    }

    /**
     * Used to obtain the set of changes done in local database as a List of DoerDBChange instances.
     * @param thresholdID int The threshold ID to be used to obtain the executed queries.
     * @return List of DoerDBChange instances representing all the changes done to the local database in the given ID interval(w.r.t the thresholdID and Now).
     * @throws SQLException If any exception is thrown during the execution of MySQL query.
     * @throws InitializationFailureException If any exception is thrown during the initialization of DoerDatabase.
     */
    private List<DoerDBChange> getLocalChanges(int thresholdID) throws SQLException, InitializationFailureException {
        List<BasicQuery> localChanges = this.getLocalChangesAsQueries(thresholdID);
        return this.getChangesByQueries(DoerDBChange.SyncDirection.LOCAL_TO_REMOTE, localChanges);
    }

    /**
     * Used to obtain the set of changes done in remote database as a List of DoerDBChange instances.
     * @param thresholdID int The threshold ID to be used to obtain the executed queries.
     * @return List of DoerDBChange instances representing all the changes done to the remote database in the given ID interval(w.r.t the thresholdID and Now).
     * @throws SQLException If any exception is thrown during the execution of MySQL query.
     * @throws InitializationFailureException If any exception is thrown during the initialization of DoerDatabase.
     */
    private List<DoerDBChange> getRemoteChanges(int thresholdID) throws SQLException, InitializationFailureException {
        List<BasicQuery> remoteChanges = this.getRemoteChangesAsQueries(thresholdID);
        return this.getChangesByQueries(DoerDBChange.SyncDirection.REMOTE_TO_LOCAL, remoteChanges);
    }

    /**
     * Synchronizes changes between the local database and remote database after the given ID.
     * @param thresholdLocalID int The threshold ID to be used to obtain the executed queries. Uses all the changes done after this ID.
     * @throws SQLException If any exception is thrown during the execution of MySQL query.
     * @throws InitializationFailureException If any exception is thrown during the initialization of DoerDatabase.
     * @throws SynchronizeException If the remote database is under another synchronization process at the moment.
     */
    private void synchronizeChangesFrom(int thresholdLocalID, int thresholdRemoteID, boolean forceSyncLockRemove) throws SQLException, InitializationFailureException, SynchronizeException {
        DoerDBSyncStatusTable remoteSyncStatusTable = this.doerDB.getRemoteDatabase().getSyncStatusTable();
        boolean remoteSyncStatus = remoteSyncStatusTable.getSyncStatus();
        if (remoteSyncStatus && !forceSyncLockRemove) {
            throw new SynchronizeException(ExceptionCodes.STATUS_SYNCING, "The remote database is currently synchronizing. Please try again later.");
        }
        remoteSyncStatusTable.setSyncStatus(true);

        List<DoerDBChange> changes = this.getLocalChanges(thresholdLocalID);
        changes.addAll(this.getRemoteChanges(thresholdRemoteID));

        Collections.sort(changes);

        for (int i = 0; i < changes.size(); i++) {
            DoerDBChange currentChange = changes.get(i);
            BasicQuery changeQuery = currentChange.getQuery();

            boolean similarFound = false;
            for (int k = i + 1; k < changes.size(); k++) {
                DoerDBChange checkerChange = changes.get(k);
                BasicQuery checkerQuery = checkerChange.getQuery();

                if (changeQuery.compareOldRecordTo(checkerQuery)) {
                    checkerChange.updateOldRecord(changeQuery.getNewRecord());
                    similarFound = true;
                }
            }

            if (!similarFound) {
                this.doerDBChangeExecutor.executeDoerDBChange(currentChange);
            }
        }

        /* Sets the timestamp of last query if there were changes. */
        if (changes.size() > 0) {
            DoerDBSyncDataTable localSyncTable = this.doerDB.getLocalDatabase().getSyncDataTable();

            int lastLocalID = this.doerDB.getLocalDatabase().getMetaTable().getLastQueryID();
            int lastRemoteID = this.doerDB.getRemoteDatabase().getMetaTable().getLastQueryID();

            localSyncTable.setLastSyncIDs(lastLocalID, lastRemoteID);
        }

        remoteSyncStatusTable.setSyncStatus(false);
    }

    /**
     * Synchronizes changes between the local database and remote database, for the changes after the last synchronization process.
     * @param forceSyncLockRemove boolean true to force synchronization even if another device is synchronizing, false otherwise.
     * @throws SQLException If any exception is thrown during the execution of MySQL query.
     * @throws InitializationFailureException If any exception is thrown during the initialization of DoerDatabase.
     * @throws SynchronizeException If the remote database is under another synchronization process at the moment(If and only if forceSyncLockRemove=false).
     */
    public void synchronizeChanges(boolean forceSyncLockRemove) throws SQLException, InitializationFailureException, SynchronizeException {
        DoerDBSyncDataTable localSyncTable = this.doerDB.getLocalDatabase().getSyncDataTable();
        int lastLocalID = localSyncTable.getLastLocalID();
        int lastRemoteID = localSyncTable.getLastRemoteID();
        this.synchronizeChangesFrom(lastLocalID, lastRemoteID, forceSyncLockRemove);
    }

    /**
     * Synchronizes changes between the local database and remote database, for the changes after the last synchronization process.
     * <b>Note: Would not proceed if the server is in another synchronizing process.</b>
     * @throws SQLException If any exception is thrown during the execution of MySQL query.
     * @throws InitializationFailureException If any exception is thrown during the initialization of DoerDatabase.
     * @throws SynchronizeException If the remote database is under another synchronization process at the moment(If and only if forceSyncLockRemove=false).
     */
    public void synchronizeChanges() throws SQLException, InitializationFailureException, SynchronizeException {
        this.synchronizeChanges(false);
    }

}
