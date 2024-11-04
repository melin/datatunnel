package com.superior.datatunnel.api.model;

import java.io.Serializable;

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

    private int numListstatusThreads = 10;

    private boolean consistentPathBehaviour = false;

    private String[] includes;

    private String[] excludes;

    private boolean excludeHiddenFile = true;

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

    public String[] getIncludes() {
        return includes;
    }

    public void setIncludes(String[] includes) {
        this.includes = includes;
    }

    public String[] getExcludes() {
        return excludes;
    }

    public void setExcludes(String[] excludes) {
        this.excludes = excludes;
    }

    public boolean isExcludeHiddenFile() {
        return excludeHiddenFile;
    }

    public void setExcludeHiddenFile(boolean excludeHiddenFile) {
        this.excludeHiddenFile = excludeHiddenFile;
    }
}
