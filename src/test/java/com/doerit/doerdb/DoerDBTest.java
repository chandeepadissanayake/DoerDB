package com.doerit.doerdb;

import com.doerit.doerdb.exceptions.InitializationFailureException;
import com.doerit.doerdb.exceptions.NotFoundException;
import org.junit.Test;

import java.sql.SQLException;

public class DoerDBTest {

    @Test
    public void createDatabaseShouldSucceed() throws SQLException, InitializationFailureException {
        DBCredentialWrapper credentialsLocal = new DBCredentialWrapper("localhost", 3306, "db_doerdb_local", "root", "");
        DBCredentialWrapper credentialsRemote = new DBCredentialWrapper("localhost", 3306, "db_doerdb_remote", "root", "");

        try {
            DoerDB doerDB = new DoerDB(credentialsLocal, credentialsRemote);
        }

        catch (NotFoundException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

}