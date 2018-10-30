package com.doerit.doerdb.db.queries.executors;

import com.doerit.doerdb.db.DoerDatabase;
import com.doerit.doerdb.db.queries.BasicQuery;
import com.doerit.doerdb.exceptions.InitializationFailureException;
import com.doerit.doerdb.synchronizer.mappers.TableMapper;

import java.sql.SQLException;

public class QueryExecutor {

    private final DoerDatabase doerDatabase;

    /**
     * Constructor for QueryExecutor.
     * @param doerDatabase DoerDatabase The instance of DoerDatabase associated with the QueryExecutor.
     */
    public QueryExecutor(DoerDatabase doerDatabase) {
        this.doerDatabase = doerDatabase;
    }

    /**
     * Executes a query on the DoerDatabase associated.
     * @param query BasicQuery The query to be executed.
     * @throws SQLException If any exception is thrown during the execution of MySQL query.
     * @throws InitializationFailureException If any exception is thrown during the initialization of DoerDatabase.
     */
    public void executeQuery(BasicQuery query) throws SQLException, InitializationFailureException {
        String mysqlQuery = query.getMySQLQuery(query.getQueryTimestamp());
        this.doerDatabase.executeUpdate(mysqlQuery);
    }

}
