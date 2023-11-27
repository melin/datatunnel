package com.aliyun.odps.cupid.table.v1.tunnel.impl;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.StsAccount;
import com.aliyun.odps.cupid.table.v1.util.Options;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.utils.StringUtils;
import com.superior.datatunnel.common.util.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class Util {
    private static final Logger LOG = LoggerFactory.getLogger(Util.class);

    public static final String WRITER_STREAM_ENABLE = "odps.cupid.writer.stream.enable";

    public static final String WRITER_COMPRESS_ENABLE = "odps.cupid.writer.compress.enable";

    public static final String WRITER_BUFFER_ENABLE = "odps.cupid.writer.buffer.enable";

    public static final String WRITER_BUFFER_SHARES = "odps.cupid.writer.buffer.shares";

    public static final String WRITER_BUFFER_SIZE = "odps.cupid.writer.buffer.size";

    public static final int DEFAULT_WRITER_BUFFER_SIZE = 67108864;

    public static PartitionSpec toOdpsPartitionSpec(Map<String, String> partitionSpec) {
        if (partitionSpec == null || partitionSpec.isEmpty()) {
            return new PartitionSpec();
        }
        PartitionSpec odpsPartitionSpec = new PartitionSpec();
        for (String key : partitionSpec.keySet()) {
            String value = partitionSpec.get(key);
            odpsPartitionSpec.set(key.trim(), CommonUtils.cleanQuote(value));
        }
        return odpsPartitionSpec;
    }

    public static Map<String, String> fromOdpsPartitionSpec(PartitionSpec odpsPartitionSpec) {
        Map<String, String> partitionSpec = new HashMap<>();
        for (String key : odpsPartitionSpec.keys()) {
            partitionSpec.put(key, odpsPartitionSpec.get(key));
        }

        return partitionSpec;
    }

    public static TableTunnel getTableTunnel(Options options) {
        TableTunnel tunnel = new TableTunnel(getOdps(options));
        if (!StringUtils.isNullOrEmpty(options.getOdpsConf().getTunnelEndpoint())) {
            tunnel.setEndpoint(options.getOdpsConf().getTunnelEndpoint());
        }
        return tunnel;
    }

    public static Odps getOdps(Options options) {
        Odps odps = new Odps(getDefaultAccount(options));
        odps.setEndpoint(options.getOdpsConf().getEndpoint());
        odps.setDefaultProject(options.getOdpsConf().getProject());
        return odps;
    }

    private static Account getDefaultAccount(Options options) {
        if (options.contains("odps.access.security.token")
                && StringUtils.isNotBlank(options.get("odps.access.security.token"))) {
            return new StsAccount(options.getOdpsConf().getAccessId(),
                    options.getOdpsConf().getAccessKey(),
                    options.get("odps.access.security.token"));
        } else {
            return new AliyunAccount(options.getOdpsConf().getAccessId(),
                    options.getOdpsConf().getAccessKey());
        }
    }

    public static void createPartition(
            String project,
            String table,
            PartitionSpec partitionSpec,
            Odps odps) throws IOException {
        int retry = 0;
        long sleep = 2000;
        while (true) {
            try {
                odps.tables().get(project, table).createPartition(partitionSpec, true);
                break;
            } catch (OdpsException e) {
                retry++;
                if (retry > 5) {
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(sleep + ThreadLocalRandom.current().nextLong(3000));
                } catch (InterruptedException ex) {

                }
                sleep = sleep * 2;
            }
        }
    }

    public static TableTunnel.DownloadSession createDownloadSession(String project,
                                                                    String table,
                                                                    PartitionSpec partitionSpec,
                                                                    TableTunnel tunnel) throws IOException {
        int retry = 0;
        long sleep = 2000;
        TableTunnel.DownloadSession downloadSession;
        while (true) {
            try {
                if (partitionSpec == null || partitionSpec.isEmpty()) {
                    downloadSession = tunnel.createDownloadSession(project,
                            table);
                } else {
                    downloadSession = tunnel.createDownloadSession(project,
                            table,
                            partitionSpec);
                }
                break;
            } catch (TunnelException e) {
                retry++;
                if (retry > 5) {
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(sleep + ThreadLocalRandom.current().nextLong(3000));
                } catch (InterruptedException ex) {
                    LOG.error(ex.getMessage(), ex);
                }
                sleep = sleep * 2;
            }
        }
        return downloadSession;
    }

    public static TableTunnel.UploadSession createUploadSession(String project,
                                                                String table,
                                                                PartitionSpec partitionSpec,
                                                                boolean isOverwrite,
                                                                TableTunnel tunnel) throws IOException {
        int retry = 0;
        long sleep = 2000;
        TableTunnel.UploadSession uploadSession;
        while (true) {
            try {
                if (partitionSpec == null || partitionSpec.isEmpty()) {
                    uploadSession = tunnel.createUploadSession(project,
                            table,
                            isOverwrite);
                } else {
                    uploadSession = tunnel.createUploadSession(project,
                            table,
                            partitionSpec,
                            isOverwrite);
                }
                break;
            } catch (TunnelException e) {
                retry++;
                if (retry > 5) {
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(sleep + ThreadLocalRandom.current().nextLong(3000));
                } catch (InterruptedException ex) {
                    LOG.error(ex.getMessage(), ex);
                }
                sleep = sleep * 2;
            }
        }
        return uploadSession;
    }

    public static TableTunnel.StreamUploadSession createStreamUploadSession(String project,
                                                                            String table,
                                                                            PartitionSpec partitionSpec,
                                                                            boolean createParitition,
                                                                            TableTunnel tunnel) throws IOException {
        int retry = 0;
        long sleep = 2000;
        TableTunnel.StreamUploadSession uploadSession;
        while (true) {
            try {
                if (partitionSpec == null || partitionSpec.isEmpty()) {
                    uploadSession = tunnel.buildStreamUploadSession(project, table).build();
                } else {
                    uploadSession = tunnel.buildStreamUploadSession(project, table)
                            .setPartitionSpec(partitionSpec)
                            .setCreatePartition(createParitition).build();
                }
                break;
            } catch (TunnelException e) {
                retry++;
                if (retry > 5) {
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(sleep + ThreadLocalRandom.current().nextLong(3000));
                } catch (InterruptedException ex) {
                    LOG.error(ex.getMessage(), ex);
                }
                sleep = sleep * 2;
            }
        }
        return uploadSession;
    }

}
