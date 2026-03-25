// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.tools;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;

import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HAUtilClient;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.NameNodeProxiesClient.ProxyAndInfo;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator.OpenFilesType;
import org.apache.hadoop.hdfs.protocol.ReplicatedBlockStats;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeLocalInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeVolumeInfo;
import org.apache.hadoop.hdfs.protocol.ECBlockGroupStats;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.UpgradeAction;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator;
import org.apache.hadoop.hdfs.protocol.ReconfigurationProtocol;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RefreshCallQueueProtocol;
import org.apache.hadoop.ipc.RefreshResponse;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.protocolPB.GenericRefreshProtocolClientSideTranslatorPB;
import org.apache.hadoop.ipc.protocolPB.GenericRefreshProtocolPB;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.util.Preconditions;

/**
 * This class provides some DFS administrative access shell commands.
 * HDFS dfsadmin 命令行工具实现，提供各类HDFS集群管理功能，只能由超级用户执行
 */
@InterfaceAudience.Private
public class DFSAdmin extends FsShell {

  static {
    HdfsConfiguration.init();
  }
  
  private static final Logger LOG = LoggerFactory.getLogger(DFSAdmin.class);

  /**
   * 所有DFSAdmin子命令的抽象基类，提供通用的命令执行框架
   */
  abstract private static class DFSAdminCommand extends Command {
    protected DistributedFileSystem dfs;
    /** 构造方法 */
    public DFSAdminCommand(Configuration conf) {
      super(conf);
    }

    @Override
    public void run(PathData pathData) throws IOException {
      FileSystem fs = pathData.fs;
      this.dfs = AdminHelper.checkAndGetDFS(fs, getConf());
      run(pathData.path);
    }
  }
  
  /**
   * 清除目录配额命令实现，对应 -clrQuota 子命令
   */
  private static class ClearQuotaCommand extends DFSAdminCommand {
    private static final String NAME = "clrQuota";
    private static final String USAGE = "-"+NAME+" <dirname>...<dirname>";
    private static final String DESCRIPTION = USAGE + ": " +
    "Clear the quota for each directory <dirName>.\n" +
    "\t\tFor each directory, attempt to clear the quota. An error will be reported if\n" +
    "\t\t1. the directory does not exist or is a file, or\n" +
    "\t\t2. user is not an administrator.\n" +
    "\t\tIt does not fault if the directory has no quota.";
    
    /** 构造方法 */
    ClearQuotaCommand(String[] args, int pos, Configuration conf) {
      super(conf);
      CommandFormat c = new CommandFormat(1, Integer.MAX_VALUE);
      List<String> parameters = c.parse(args, pos);
      this.args = parameters.toArray(new String[parameters.size()]);
    }
    
    /**
     * 判断输入命令是否为 clrQuota 命令
     * @param cmd 以'-'开头的命令字符串
     * @return 是否匹配
     */
    public static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    public void run(Path path) throws IOException {
      dfs.setQuota(path, HdfsConstants.QUOTA_RESET, HdfsConstants.QUOTA_DONT_SET);
    }
  }
  
  /**
   * 设置目录名称配额命令实现，对应 -setQuota 子命令
   * 名称配额限制目录树下的总文件/目录数量
   */
  private static class SetQuotaCommand extends DFSAdminCommand {
    private static final String NAME = "setQuota";
    private static final String USAGE =
      "-"+NAME+" <quota> <dirname>...<dirname>";
    private static final String DESCRIPTION =
        "-setQuota <quota> <dirname>...<dirname>: " +
        "Set the quota <quota> for each directory <dirName>.\n" +
        "\t\tThe directory quota is a long integer that puts a hard limit\n" +
        "\t\ton the number of names in the directory tree\n" +
        "\t\tFor each directory, attempt to set the quota. An error will be reported if\n" +
        "\t\t1. quota is not a positive integer, or\n" +
        "\t\t2. User is not an administrator, or\n" +
        "\t\t3. The directory does not exist or is a file.\n" +
        "\t\tNote: A quota of 1 would force the directory to remain empty.\n";

    private final long quota; // 需要设置的名称配额值

    /** 构造方法 */
    SetQuotaCommand(String[] args, int pos, Configuration conf) {
      super(conf);
      CommandFormat c = new CommandFormat(2, Integer.MAX_VALUE);
      List<String> parameters = c.parse(args, pos);
      this.quota =
          StringUtils.TraditionalBinaryPrefix.string2long(parameters.remove(0));
      this.args = parameters.toArray(new String[parameters.size()]);
    }
    
    /**
     * 判断输入命令是否为 setQuota 命令
     * @param cmd 以'-'开头的命令字符串
     * @return 是否匹配
     */
    public static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    public void run(Path path) throws IOException {
      dfs.setQuota(path, quota, HdfsConstants.QUOTA_DONT_SET);
    }
  }
  
  /**
   * 清除目录空间配额命令实现，对应 -clrSpaceQuota 子命令
   * 支持按存储类型清除空间配额
   */
  private static class ClearSpaceQuotaCommand extends DFSAdminCommand {
    private static final String NAME = "clrSpaceQuota";
    private static final String USAGE = "-"+NAME+" [-storageType <storagetype>] <dirname>...<dirname>";
    private static final String DESCRIPTION = USAGE + ": " +
        "Clear the space quota for each directory <dirName>.\n" +
        "\t\tFor each directory, attempt to clear the quota. " +
        "An error will be reported if\n" +
        "\t\t1. the directory does not exist or is a file, or\n" +
        "\t\t2. user is not an administrator.\n" +
        "\t\tIt does not fault if the directory has no quota.\n" +
        "\t\tThe storage type specific quota is cleared when -storageType " +
        "option is specified.\n" +
        "\t\tAvailable storageTypes are \n" +
        "\t\t- DISK\n" +
        "\t\t- SSD\n" +
        "\t\t- ARCHIVE\n" +
        "\t\t- PROVIDED\n" +
        "\t\t- NVDIMM";


    private StorageType type;

    /** 构造方法 */
    ClearSpaceQuotaCommand(String[] args, int pos, Configuration conf) {
      super(conf);
      CommandFormat c = new CommandFormat(1, Integer.MAX_VALUE);
      c.addOptionWithValue("storageType");
      List<String> parameters = c.parse(args, pos);
      String storageTypeString = c.getOptValue("storageType");
      if (storageTypeString != null) {
        this.type = StorageType.parseStorageType(storageTypeString);
      }
      this.args = parameters.toArray(new String[parameters.size()]);
    }
    
    /**
     * 判断输入命令是否为 clrSpaceQuota 命令
     * @param cmd 以'-'开头的命令字符串
     * @return 是否匹配
     */
    public static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    public void run(Path path) throws IOException {
      if (type != null) {
        dfs.setQuotaByStorageType(path, type, HdfsConstants.QUOTA_RESET);
      } else {
        dfs.setQuota(path, HdfsConstants.QUOTA_DONT_SET, HdfsConstants.QUOTA_RESET);
      }
    }
  }
  
  /**
   * 设置目录空间配额命令实现，对应 -setSpaceQuota 子命令
   * 空间配额限制目录树下的总存储空间使用量，包含副本占用的空间
   * 支持按存储类型设置空间配额
   */
  private static class SetSpaceQuotaCommand extends DFSAdminCommand {
    private static final String NAME = "setSpaceQuota";
    private static final String USAGE =
      "-"+NAME+" <quota> [-storageType <storagetype>] <dirname>...<dirname>";
    private static final String DESCRIPTION = USAGE + ": " +
        "Set the space quota <quota> for each directory <dirName>.\n" +
        "\t\tThe space quota is a long integer that puts a hard limit\n" +
        "\t\ton the total size of all the files under the directory tree.\n" +
        "\t\tThe extra space required for replication is also counted. E.g.\n" +
        "\t\ta 1GB file with replication of 3 consumes 3GB of the quota.\n\n" +
        "\t\tQuota can also be specified with a binary prefix for terabytes,\n" +
        "\t\tpetabytes etc (e.g. 50t is 50TB, 5m is 5MB, 3p is 3PB).\n" +
        "\t\tFor each directory, attempt to set the quota. An error will be reported if\n" +
        "\t\t1. quota is not a positive integer or zero, or\n" +
        "\t\t2. user is not an administrator, or\n" +
        "\t\t3. the directory does not exist or is a file.\n" +
        "\t\tThe storage type specific quota is set when -storageType option is specified.\n" +
        "\t\tAvailable storageTypes are \n" +
        "\t\t- DISK\n" +
        "\t\t- SSD\n" +
        "\t\t- ARCHIVE\n" +
        "\t\t- PROVIDED\n" +
        "\t\t- NVDIMM";

    private long quota; // 需要设置的空间配额值
    private StorageType type; // 需要设置配额的存储类型
    
    /** 构造方法 */
    SetSpaceQuotaCommand(String[] args, int pos, Configuration conf) {
      super(conf);
      CommandFormat c = new CommandFormat(2, Integer.MAX_VALUE);
      List<String> parameters = c.parse(args, pos);
      String str = parameters.remove(0).trim();
      try {
        quota = StringUtils.TraditionalBinaryPrefix.string2long(str);
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException("\"" + str + "\" is not a valid value for a quota.");
      }
      String storageTypeString =
          StringUtils.popOptionWithArgument("-storageType", parameters);
      if (storageTypeString != null) {
        try {
          this.type = StorageType.parseStorageType(storageTypeString);
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException("Storage type "
              + storageTypeString
              + " is not available. Available storage types are "
              + StorageType.getTypesSupportingQuota());
        }
      }
      this.args = parameters.toArray(new String[parameters.size()]);
    }
    
    /**
     * 判断输入命令是否为 setSpaceQuota 命令
     * @param cmd 以'-'开头的命令字符串
     * @return 是否匹配
     */
    public static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    public void run(Path path) throws IOException {
      if (type != null) {
        dfs.setQuotaByStorageType(path, type, quota);
      } else {
        dfs.setQuota(path, HdfsConstants.QUOTA_DONT_SET, quota);
      }
    }
  }

  /**
   * 滚动升级命令实现，对应 -rollingUpgrade 子命令
   * 支持查询滚动升级状态、准备滚动升级、完成滚动升级三个操作
   */
  private static class RollingUpgradeCommand {
    static final String NAME = "rollingUpgrade";
    static final String USAGE = "-"+NAME+" [<query|prepare|finalize>]";
    static final String DESCRIPTION = USAGE + ":\n"
        + "     query: query the current rolling upgrade status.\n"
        + "   prepare: prepare a new rolling upgrade.\n"
        + "  finalize: finalize the current rolling upgrade.";

    /**
     * 判断输入命令是否为 rollingUpgrade 命令
     * @param cmd 以'-'开头的命令字符串
     * @return 是否匹配
     */
    static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    private static void printMessage(RollingUpgradeInfo info,
        PrintStream out) {
      if (info != null && info.isStarted()) {
        if (!info.createdRollbackImages() && !info.isFinalized()) {
          out.println(
              "Preparing for upgrade.