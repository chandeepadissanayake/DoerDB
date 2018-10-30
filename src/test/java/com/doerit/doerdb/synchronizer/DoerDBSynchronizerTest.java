package com.doerit.doerdb.synchronizer;

import com.doerit.doerdb.DBCredentialWrapper;
import com.doerit.doerdb.DoerDB;
import com.doerit.doerdb.exceptions.InitializationFailureException;
import com.doerit.doerdb.exceptions.NotFoundException;
import com.doerit.doerdb.synchronizer.mappers.DatabaseMapper;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.*;

public class DoerDBSynchronizerTest {

    private DoerDB doerDB;

    @Before
    public void setUp() throws Exception {
        DBCredentialWrapper credentialsLocal = new DBCredentialWrapper("localhost", 3306, "db_doerdb_local", "root", "");
        DBCredentialWrapper credentialsRemote = new DBCredentialWrapper("localhost", 3306, "db_doerdb_remote", "root", "");

        try {
            this.doerDB = new DoerDB(credentialsLocal, credentialsRemote);
        }

        catch (NotFoundException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void constructorShouldWork() {
        try {
            DoerDBSynchronizer doerDBSynchronizer = new DoerDBSynchronizer(this.doerDB);
        } catch (SQLException | InitializationFailureException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void synchronizeChangesShouldWork() {
        try {
            DoerDBSynchronizer doerDBSynchronizer = new DoerDBSynchronizer(this.doerDB);
            doerDBSynchronizer.synchronizeChanges();
        }

        catch (SQLException | InitializationFailureException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void synchronizeChangesWithMappersShouldWork() {
        try {
            DoerDBSynchronizer doerDBSynchronizer = new DoerDBSynchronizer(this.doerDB);

            DatabaseMapper databaseMapper = doerDBSynchronizer.getDoerDBMapper();
            databaseMapper.getTableMapperByLocalTable("tbl_test").getColumnMapperByLocalColumn("full_name").setRemoteColumnName("total_name");

            doerDBSynchronizer.synchronizeChanges();
        }

        catch (SQLException | InitializationFailureException e) {
            e.printStackTrace();
        }
    }

}