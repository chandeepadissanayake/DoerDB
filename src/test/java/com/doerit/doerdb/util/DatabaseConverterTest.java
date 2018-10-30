package com.doerit.doerdb.util;

import com.doerit.doerdb.DBCredentialWrapper;
import org.junit.Test;

import static org.junit.Assert.*;

public class DatabaseConverterTest {

    private DBCredentialWrapper credentialsLocal = new DBCredentialWrapper("localhost", 3306, "db_doerdb_local", "root", "");
    private DBCredentialWrapper credentialsRemote = new DBCredentialWrapper("localhost", 3306, "db_doerdb_remote", "root", "");

    @Test
    public void convertToDoerDB() {
        try {
            DatabaseConverter dbConverter = new DatabaseConverter(credentialsLocal, credentialsRemote);
            dbConverter.convertToDoerDB();
        }

        catch (Exception e) {
            e.printStackTrace();
        }
    }

}