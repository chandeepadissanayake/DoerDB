package com.doerit.doerdb.synchronizer.mappers;

public class ColumnMapper {

    private String localColumnName;
    private String remoteColumnName;

    /**
     * Constructor.
     * @param localColumnName String The name of the local column name to be used in mapping.
     * @param remoteColumnName String The name of the remote column name to be used in mapping.
     */
    public ColumnMapper(String localColumnName, String remoteColumnName) {
        this.localColumnName = localColumnName;
        this.remoteColumnName = remoteColumnName;
    }

    /**
     * Used to obtain the local column name.
     * @return String The Local Column Name.
     */
    public String getLocalColumnName() {
        return localColumnName;
    }

    /**
     * Used to obtain the remote column name.
     * @return String The Remote Column Name.
     */
    public String getRemoteColumnName() {
        return remoteColumnName;
    }

    /**
     * Sets the Local Column Name
     * @param localColumnName String The Name of the Column.
     */
    public void setLocalColumnName(String localColumnName) {
        this.localColumnName = localColumnName;
    }

    /**
     * Sets the Remote Column Name
     * @param remoteColumnName String The Name of the Column.
     */
    public void setRemoteColumnName(String remoteColumnName) {
        this.remoteColumnName = remoteColumnName;
    }

}
