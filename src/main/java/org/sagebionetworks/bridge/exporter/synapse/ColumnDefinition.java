package org.sagebionetworks.bridge.exporter.synapse;

import org.sagebionetworks.repo.model.table.ColumnType;

public class ColumnDefinition  {
    private String name;
    private Long maximumSize;
    private TransferMethod transferMethod;
    private String ddbName;
    private boolean sanitize;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getMaximumSize() {
        return maximumSize;
    }

    public void setMaximumSize(Long maximumSize) {
        this.maximumSize = maximumSize;
    }

    public TransferMethod getTransferMethod() {
        return transferMethod;
    }

    public void setTransferMethod(TransferMethod transferMethod) {
        this.transferMethod = transferMethod;
    }

    public String getDdbName() {
        return ddbName;
    }

    public void setDdbName(String ddbName) {
        this.ddbName = ddbName;
    }

    public boolean getSanitize() {
        return sanitize;
    }

    public void setSanitize(boolean sanitize) {
        this.sanitize = sanitize;
    }

}
