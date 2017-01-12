package org.sagebionetworks.bridge.exporter.synapse;

import org.sagebionetworks.repo.model.table.ColumnType;

public class ColumnDefinition  {
    private String name;
    private ColumnType columnType;
    private Long maximumSize;
    private TransferMethod transferMethod;
    private String ddbName;
    private Boolean sanitize;

    public ColumnDefinition() {

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public void setColumnType(ColumnType columnType) {
        this.columnType = columnType;
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

    public Boolean getSanitize() {
        return sanitize;
    }

    public void setSanitize(Boolean sanitize) {
        this.sanitize = sanitize;
    }

    public int hashCode() {
        boolean prime = true;
        byte result = 1;
        int result1 = 31 * result + (this.name == null?0:this.name.hashCode());
        result1 = 31 * result1 + (this.columnType == null?0:this.columnType.hashCode());
        result1 = 31 * result1 + (this.maximumSize == null?0:this.maximumSize.hashCode());
        result1 = 31 * result1 + (this.transferMethod == null?0:this.transferMethod.hashCode());
        result1 = 31 * result1 + (this.ddbName == null?0:this.ddbName.hashCode());
        result1 = 31 * result1 + (this.sanitize == null?0:this.sanitize.hashCode());
        return result1;
    }

    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        } else if(obj == null) {
            return false;
        } else if(this.getClass() != obj.getClass()) {
            return false;
        } else {
            ColumnDefinition other = (ColumnDefinition)obj;
            if(this.name == null) {
                if(other.name != null) {
                    return false;
                }
            } else if(!this.name.equals(other.name)) {
                return false;
            }

            if(this.columnType == null) {
                if(other.columnType != null) {
                    return false;
                }
            } else if(!this.columnType.equals(other.columnType)) {
                return false;
            }

            if(this.maximumSize == null) {
                if(other.maximumSize != null) {
                    return false;
                }
            } else if(!this.maximumSize.equals(other.maximumSize)) {
                return false;
            }

            if(this.transferMethod == null) {
                if(other.transferMethod != null) {
                    return false;
                }
            } else if(!this.transferMethod.equals(other.transferMethod)) {
                return false;
            }

            if(this.ddbName == null) {
                if(other.ddbName != null) {
                    return false;
                }
            } else if(!this.ddbName.equals(other.ddbName)) {
                return false;
            }

            if(this.sanitize == null) {
                if(other.sanitize != null) {
                    return false;
                }
            } else if(!this.sanitize.equals(other.sanitize)) {
                return false;
            }

            return true;
        }
    }

    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("");
        result.append("org.sagebionetworks.bridge.exporter.synapse.ColumnDefinition");
        result.append(" [");
        result.append("name=");
        result.append(this.name);
        result.append(" ");
        result.append("columnType=");
        result.append(this.columnType);
        result.append(" ");
        result.append("maximumSize=");
        result.append(this.maximumSize);
        result.append(" ");
        result.append("transferMethod=");
        result.append(this.transferMethod);
        result.append(" ");
        result.append("ddbName=");
        result.append(this.ddbName);
        result.append(" ");
        result.append("sanitize=");
        result.append(this.sanitize);
        result.append(" ");
        result.append("]");
        return result.toString();
    }
}
