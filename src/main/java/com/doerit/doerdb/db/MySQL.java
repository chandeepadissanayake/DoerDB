package com.doerit.doerdb.db;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MySQL {

    public static final String SQL_INSERT_PREFIX = MySQL.SQL_INSERT_CLAUSE + " INTO";
    public static final String SQL_INSERT_VALUES = "VALUES";
    public static final String SQL_UPDATE_PREFIX = "UPDATE";
    public static final String SQL_SHOW_CLAUSE = "SHOW";
    public static final String SQL_CREATE_CLAUSE = "CREATE";
    public static final String SQL_DROP_CLAUSE = "DROP";
    public static final String SQL_INSERT_CLAUSE = "INSERT";
    public static final String SQL_BEGIN_CLAUSE = "BEGIN";
    public static final String SQL_END_CLAUSE = "END";
    public static final String SQL_SELECT_CLAUSE = "SELECT";
    public static final String SQL_FROM_CLAUSE = "FROM";
    public static final String SQL_WHERE_CLAUSE = "WHERE";
    public static final String SQL_AFTER_CLAUSE = "AFTER";
    public static final String SQL_ORDER_CLAUSE = "ORDER";
    public static final String SQL_THEN_CLAUSE = "THEN";
    public static final String SQL_IF_CONDITION = "IF";
    public static final String SQL_END_IF_CLAUSE = "END IF";
    public static final String SQL_SET_OPERATOR = "SET";
    public static final String SQL_IS_OPERATOR = "IS";
    public static final String SQL_EXISTS_OPERATOR = "EXISTS";
    public static final String SQL_AND_OPERATOR = "AND";
    public static final String SQL_OR_OPERATOR = "OR";
    public static final String SQL_ON_OPERATOR = "ON";
    public static final String SQL_NOT_OPERATOR = "NOT";
    public static final String SQL_CONCAT_OPERATOR = "CONCAT";
    public static final String SQL_LIKE_OPERATOR = "LIKE";
    public static final String SQL_FOR_OPERATOR = "FOR";
    public static final String SQL_BY_OPERATOR = "BY";
    public static final String SQL_NEW_OPERATOR = "NEW";
    public static final String SQL_OLD_OPERATOR = "OLD";
    public static final String SQL_LIMIT_OPERATOR = "LIMIT";
    public static final String SQL_SORT_ASC = "ASC";
    public static final String SQL_SORT_DESC = "DESC";
    public static final String SQL_VALUE_NULL = "NULL";
    public static final String SQL_CONTENT_TRIGGER = "TRIGGER";
    public static final String SQL_CONTENT_TABLES = "TABLES";
    public static final String SQL_CONTENT_COLUMNS = "COLUMNS";
    public static final String SQL_CONTENT_TABLE_NAME = "table_name";
    public static final String SQL_CONTENT_COLUMN_NAME = "column_name";
    public static final String SQL_CONTENT_TABLE_SCHEMA = "table_schema";
    public static final String SQL_CONTENT_FIELD = "Field";
    public static final String SQL_CONTENT_INFORMATION_SCHEMA_TABLES = "information_schema.tables";

    public static final String SQL_INTERNAL_QUOTES = "`";
    public static final String SQL_EXTERNAL_QUOTES = "'";
    public static final String SQL_EQUATOR = "=";
    public static final String SQL_SEPARATOR = ",";
    public static final String SQL_SPACE = " ";
    public static final String SQL_BRACKET_ROUND_OPEN = "(";
    public static final String SQL_BRACKET_ROUND_CLOSE = ")";

    public static final String SQL_DEFAULT_TIMESTAMP_FORMAT = "yyyy-M-d H:m:s";

    /**
     * Used to obtain current date and time as a Timestamp.
     * @return Timestamp for the current date and time.
     */
    public static Timestamp getCurrentTimestampSQL() {
        return new Timestamp(System.currentTimeMillis());
    }

    /**
     * Used to format the current timestamp into MySQL Timestamp format.
     * @see <a href="https://dev.mysql.com/doc/refman/5.5/en/datetime.html">MySQL 5.5 Documentation</a>
     * @return String Formatted timestamp as in MySQL.
     */
    public static String getFormattedTimestampSQL() {
        DateFormat dateFormatTimestamp = new SimpleDateFormat(MySQL.SQL_DEFAULT_TIMESTAMP_FORMAT);
        return dateFormatTimestamp.format(MySQL.getCurrentTimestampSQL());
    }

    /**
     * Used to format the given timestamp into MySQL Timestamp format.
     * @see <a href="https://dev.mysql.com/doc/refman/5.5/en/datetime.html">MySQL 5.5 Documentation</a>
     * @param timestamp Date The Date and Time(Timestamp) to be formatted.
     * @return String Formatted timestamp as in MySQL.
     */
    public static String getFormattedTimestampSQL(Date timestamp) {
        DateFormat dateFormatTimestamp = new SimpleDateFormat(MySQL.SQL_DEFAULT_TIMESTAMP_FORMAT);
        return dateFormatTimestamp.format(timestamp);
    }

    /**
     * Used to format the given timestamp into MySQL Timestamp format.
     * @param timestamp Date The Date and Time(Timestamp) to be formatted.
     * @return Date Formatted timestamp as in MySQL.
     */
    public static Date getFormattedTimestampDateSQL(Date timestamp) {
        DateFormat dateFormatTimestamp = new SimpleDateFormat(MySQL.SQL_DEFAULT_TIMESTAMP_FORMAT);

        try {
            return dateFormatTimestamp.parse(MySQL.getFormattedTimestampSQL(timestamp));
        }

        catch (ParseException e) {
            return null;
        }
    }

}
