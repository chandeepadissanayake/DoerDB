package com.doerit.doerdb.db.queries.builders;

import com.doerit.doerdb.db.metadata.DoerDBMetaTable;
import com.doerit.doerdb.db.queries.BasicQuery;
import com.doerit.doerdb.db.queries.InsertQuery;
import com.doerit.doerdb.db.queries.UpdateQuery;
import jdk.nashorn.internal.parser.JSONParser;
import org.json.JSONObject;

import java.util.Date;
import java.util.Map;

public class QueryBuilder {

    private final Map<String, Object> queryRecordInfo;

    public QueryBuilder(Map<String, Object> queryRecordInfo) {
        this.queryRecordInfo = queryRecordInfo;
    }

    public BasicQuery getQuery() {
        int queryID = Integer.parseInt(this.queryRecordInfo.get(DoerDBMetaTable.TABLE_COL_ID).toString());
        String tableName = this.queryRecordInfo.get(DoerDBMetaTable.TABLE_COL_TABLE_NAME).toString();
        String queryType = this.queryRecordInfo.get(DoerDBMetaTable.TABLE_COL_QUERY_TYPE).toString();
        JSONObject newRecord = new JSONObject(this.queryRecordInfo.get(DoerDBMetaTable.TABLE_COL_NEW_RECORD).toString());
        Date queryTimestamp = (Date) this.queryRecordInfo.get(DoerDBMetaTable.TABLE_COL_QUERY_TIMESTAMP);

        switch (queryType) {
            case InsertQuery.QUERY_TYPE:
                return new InsertQuery(queryID, tableName, newRecord, queryTimestamp);
            case UpdateQuery.QUERY_TYPE:
                JSONObject oldRecord = new JSONObject(this.queryRecordInfo.get(DoerDBMetaTable.TABLE_COL_OLD_RECORD).toString());
                return new UpdateQuery(queryID, tableName, newRecord, oldRecord, queryTimestamp);
            default:
                return null;
        }
    }
}
