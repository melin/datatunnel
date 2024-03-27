package com.superior.datatunnel.api.model;

import java.io.Serializable;
import java.util.List;

public class DistCpOption implements Serializable {

    private String[] srcPaths;

    private String destPath;

    private boolean update = false;

    private boolean overwrite = false;

    private boolean delete = false;

    private boolean ignoreErrors = false;

    private boolean dryRun = false;

    private int maxFilesPerTask = 1000;

    private Long maxBytesPerTask = 1073741824L;

    private List<String> filters;

    private int numListstatusThreads = 10;

    private boolean consistentPathBehaviour = false;

    private List<String> filterNot;

    public boolean updateOverwritePathBehaviour() {
        return !consistentPathBehaviour && (update || overwrite);
    }

    public String[] getSrcPaths() {
        return srcPaths;
    }

    public void setSrcPaths(String[] srcPaths) {
        this.srcPaths = srcPaths;
    }

    public String getDestPath() {
        return destPath;
    }

    public void setDestPath(String destPath) {
        this.destPath = destPath;
    }

    public int getNumListstatusThreads() {
        return numListstatusThreads;
    }

    public void setNumListstatusThreads(int numListstatusThreads) {
        this.numListstatusThreads = numListstatusThreads;
    }

    public boolean isUpdate() {
        return update;
    }

    public void setUpdate(boolean update) {
        this.update = update;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public void setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    public boolean isConsistentPathBehaviour() {
        return consistentPathBehaviour;
    }

    public void setConsistentPathBehaviour(boolean consistentPathBehaviour) {
        this.consistentPathBehaviour = consistentPathBehaviour;
    }

    public List<String> getFilterNot() {
        return filterNot;
    }

    public void setFilterNot(List<String> filterNot) {
        this.filterNot = filterNot;
    }

    public boolean isDelete() {
        return delete;
    }

    public void setDelete(boolean delete) {
        this.delete = delete;
    }

    public boolean isIgnoreErrors() {
        return ignoreErrors;
    }

    public void setIgnoreErrors(boolean ignoreErrors) {
        this.ignoreErrors = ignoreErrors;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public int getMaxFilesPerTask() {
        return maxFilesPerTask;
    }

    public void setMaxFilesPerTask(int maxFilesPerTask) {
        this.maxFilesPerTask = maxFilesPerTask;
    }

    public Long getMaxBytesPerTask() {
        return maxBytesPerTask;
    }

    public void setMaxBytesPerTask(Long maxBytesPerTask) {
        this.maxBytesPerTask = maxBytesPerTask;
    }

    public List<String> getFilters() {
        return filters;
    }

    public void setFilters(List<String> filters) {
        this.filters = filters;
    }
}
