// 这个文件已经全部加上中文注释
/*
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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.top.metrics.TopMetrics;
import org.apache.hadoop.hdfs.server.namenode.visitor.INodeCountVisitor;
import org.apache.hadoop.hdfs.server.namenode.visitor.INodeCountVisitor.Counts;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.server.namenode.FsImageValidation.Cli.println;
import static org.apache.hadoop.util.Time.now;

/**
 * HDFS FSImage文件离线校验工具，负责加载指定FSImage构建完整命名空间树，
 * 并对命名空间元数据执行多项完整性校验。
 * 与离线镜像查看器不同，本工具会完整构建内存命名空间树，从而可以实现深层次的结构校验。
 */
public class FsImageValidation {
  static final Logger LOG = LoggerFactory.getLogger(FsImageValidation.class);

  static final String FS_IMAGE = "FS_IMAGE";

  /**
   * 通过环境变量PRINT_ERROR控制是否打印错误详情，默认开启
   */
  static final boolean PRINT_ERROR;

  static {
    PRINT_ERROR = getEnvBoolean("PRINT_ERROR", true);
  }

  /**
   * 从环境变量读取布尔值，读取失败或未设置时返回默认值
   * @param property 环境变量名
   * @param defaultValue 默认值
   * @return 解析后的布尔值
   */
  static boolean getEnvBoolean(String property, boolean defaultValue) {
    final String env = System.getenv().get(property);
    final boolean setToNonDefault = ("" + !defaultValue).equalsIgnoreCase(env);
    final boolean value = defaultValue != setToNonDefault;
    LOG.info("ENV: {} = {} (\"{}\")", property, value, env);
    return value;
  }

  /**
   * 从环境变量读取字符串值并日志记录
   * @param property 环境变量名
   * @return 环境变量值
   */
  static String getEnv(String property) {
    final String value = System.getenv().get(property);
    LOG.info("ENV: {} = {}", property, value);
    return value;
  }

  /**
   * 根据命令行参数创建FsImageValidation实例
   * @param args 命令行参数
   * @return FsImageValidation实例
   */
  static FsImageValidation newInstance(String... args) {
    final String f = Cli.parse(args);
    if (f == null) {
      throw new HadoopIllegalArgumentException(
          FS_IMAGE + " is not specified.");
    }
    return new FsImageValidation(new File(f));
  }

  /**
   * 初始化校验专用配置，关闭不必要功能关闭锁耗时告警
   * @param conf Hadoop配置对象
   */
  static void initConf(Configuration conf) {
    final int aDay = 24*3600_000;
    conf.setInt(DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY, aDay);
    conf.setInt(DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY, aDay);
    conf.setBoolean(DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY, false);
  }

  /** 设置伪HA配置，避免加载编辑日志，只校验镜像文件 */
  static void setHaConf(String nsId, Configuration conf) {
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, nsId);
    final String haNNKey = DFS_HA_NAMENODES_KEY_PREFIX + "." + nsId;
    conf.set(haNNKey, "nn0,nn1");
    final String rpcKey = DFS_NAMENODE_RPC_ADDRESS_KEY + "." + nsId + ".";
    conf.set(rpcKey + "nn0", "127.0.0.1:8080");
    conf.set(rpcKey + "nn1", "127.0.0.1:8080");
  }

  /**
   * 初始化日志级别，关闭无关模块日志，开启核心模块调试日志
   */
  static void initLogLevels() {
    Util.setLogLevel(FSImage.class, Level.TRACE);
    Util.setLogLevel(FileJournalManager.class, Level.TRACE);

    Util.setLogLevel(GSet.class, Level.OFF);
    Util.setLogLevel(BlockManager.class, Level.OFF);
    Util.setLogLevel(DatanodeManager.class, Level.OFF);
    Util.setLogLevel(TopMetrics.class, Level.OFF);
  }

  /**
   * 工具内部通用工具类，提供内存信息、日志级别调整、文件名过滤等能力
   */
  static class Util {
    /**
     * 获取当前JVM内存使用信息
     * @return 格式化后的内存信息字符串
     */
    static String memoryInfo() {
      final Runtime runtime = Runtime.getRuntime();
      return "Memory Info: free=" + StringUtils.byteDesc(runtime.freeMemory())
          + ", total=" + StringUtils.byteDesc(runtime.totalMemory())
          + ", max=" + StringUtils.byteDesc(runtime.maxMemory());
    }

    /**
     * 设置指定类的日志级别并记录
     * @param clazz 目标类
     * @param level 目标日志级别
     */
    static void setLogLevel(Class<?> clazz, Level level) {
      final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(clazz);
      logger.setLevel(level);
      LOG.info("setLogLevel {} to {}, getEffectiveLevel() = {}", clazz.getName(), level,
          logger.getEffectiveLevel());
    }

    /**
     * 将数字格式化为千分位分隔的字符串
     * @param n 输入数字
     * @return 格式化后的字符串
     */
    static String toCommaSeparatedNumber(long n) {
      final StringBuilder b = new StringBuilder();
      for(; n > 999;) {
        b.insert(0, String.format(",%03d", n%1000));
        n /= 1000;
      }
      return b.insert(0, n).toString();
    }

    /** 
     * 创建匹配指定NameNode文件类型的文件名过滤器 
     * @param type NameNode文件类型
     * @return 文件名过滤器
     */
    static FilenameFilter newFilenameFilter(NameNodeFile type) {
      final String prefix = type.getName() + "_";
      return new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          if (!name.startsWith(prefix)) {
            return false;
          }
          for (int i = prefix.length(); i < name.length(); i++) {
            if (!Character.isDigit(name.charAt(i))) {
              return false;
            }
          }
          return true;
        }
      };
    }
  }

  private final File fsImageFile;

  FsImageValidation(File fsImageFile) {
    this.fsImageFile = fsImageFile;
  }

  /**
   * 执行FSImage校验，使用默认配置
   * @return 校验发现的错误数量
   * @throws Exception 执行过程中的异常
   */
  int run() throws Exception {
    return run(new Configuration(), new AtomicInteger());
  }

  /**
   * 执行FSImage校验，使用默认配置，传入错误计数器
   * @param errorCount 错误计数器
   * @return 校验发现的错误数量
   * @throws Exception 执行过程中的异常
   */
  int run(AtomicInteger errorCount) throws Exception {
    return run(new Configuration(), errorCount);
  }

  /**
   * 执行FSImage校验主流程，依次执行INode引用校验和INodeMap校验
   * @param conf Hadoop配置对象
   * @param errorCount 错误计数器
   * @return 本次校验新增的错误数量
   * @throws Exception 执行过程中的异常
   */
  int run(Configuration conf, AtomicInteger errorCount) throws Exception {
    final int initCount = errorCount.get();
    LOG.info(Util.memoryInfo());
    initConf(conf);

    // 初始化NameNode指标避免NPE，然后执行INode引用校验
    NameNode.initMetrics(conf, HdfsServerConstants.NamenodeRole.NAMENODE);
    final FSNamesystem namesystem = checkINodeReference(conf, errorCount);

    // 执行INodeMap完整性校验，清理不可达节点
    final boolean changed = INodeMapValidation.run(namesystem.getFSDirectory(), errorCount);
    LOG.info(Util.memoryInfo());

    // 输出本次校验结果
    final int d = errorCount.get() - initCount;
    if (d > 0) {
      Cli.println("Found %d error(s) in %s", d, fsImageFile.getAbsolutePath());
    }
    // 如果INodeMap发生变更，保存修复后的新FSImage到临时目录
    if (changed) {
      final File dir = fsImageFile.isDirectory()? fsImageFile: fsImageFile.getParentFile();
      final Path temp = Files.createTempDirectory(dir.toPath(), "newFsImage");
      Cli.println("INodeMap changed, save a new FSImage to %s", temp);
      namesystem.getFSImage().save(namesystem, temp.toFile());
    }
    return d;
  }

  /**
   * 加载指定FSImage文件到内存构建FSNamesystem
   * @param conf Hadoop配置对象
   * @return 加载完成的FSNamesystem对象
   * @throws IOException 加载过程中的IO异常
   */
  private FSNamesystem loadImage(Configuration conf) throws IOException {
    // 定时任务输出FSImage加载进度，每分钟打印一次
    final TimerTask checkProgress = new TimerTask() {
      @Override
      public void run() {
        final double percent = NameNode.getStartupProgress().createView()
            .getPercentComplete(Phase.LOADING_FSIMAGE);
        LOG.info(String.format("%s Progress: %.1f%% (%s)",
            Phase.LOADING_FSIMAGE, 100*percent, Util.memoryInfo()));
      }
    };

    // 启动定时进度打印
    final Timer t = new Timer();
    t.scheduleAtFixedRate(checkProgress, 0, 60_000);
    final long loadStart = now();
    final FSNamesystem namesystem;
    if (fsImageFile.isDirectory()) {
      // 输入是NameNode存储目录，按目录结构加载FSImage
      Cli.println("Loading %s as a directory.", fsImageFile);
      final String dir = fsImageFile.getCanonicalPath();
      conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, dir);
      conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, dir);


      final FSImage fsImage = new FSImage(conf);
      namesystem = new FSNamesystem(conf, fsImage, true);
      // 避免回滚滚动升级信息
      namesystem.setRollingUpgradeInfo(false, 0);

      namesystem.loadFSImage(HdfsServerConstants.StartupOption.REGULAR);
    } else {
      // 输入是单个FSImage文件，手动加载
      Cli.println("Loading %s as a file.", fsImageFile);
      final FSImage fsImage = new FSImage(conf);
      namesystem = new FSNamesystem(conf, fsImage, true);

      // 初始化命名空间信息
      final NamespaceInfo namespaceInfo = NNStorage.newNamespaceInfo();
      namespaceInfo.clusterID = "cluster0";
      fsImage.getStorage().setStorageInfo(namespaceInfo);

      final FSImageFormat.LoaderDelegator loader
          = FSImageFormat.newLoader(conf, namesystem);
      // 加写锁加载FSImage
      namesystem.writeLock(RwLockMode.GLOBAL);
      namesystem.getFSDirectory().writeLock();
      try {
        loader.load(fsImageFile, false);
        fsImage.setLastAppliedTxId(loader);
      } finally {
        // 解锁
        namesystem.getFSDirectory().writeUnlock();
        namesystem.writeUnlock(RwLockMode.GLOBAL, "loadImage");
      }
    }
    // 停止进度打印
    t.cancel();
    // 输出加载完成统计
    Cli.println("Loaded %s %s with txid %d successfully in %s",
        FS_IMAGE, fsImageFile, namesystem.getFSImage().getLastAppliedTxId(),
        StringUtils.formatTime(now() - loadStart));
    return namesystem;
  }

  /**
   * 执行INode引用完整性校验，检查快照引用的INode是否存在
   * @param conf Hadoop配置对象
   * @param errorCount 错误计数器
   * @return 加载完成的FSNamesystem对象
   * @throws Exception 校验过程中的异常
   */
  FSNamesystem checkINodeReference(Configuration conf,
      AtomicInteger errorCount) throws Exception {
    INodeReferenceValidation.start();
    final FSNamesystem namesystem = loadImage(conf);
    LOG.info(Util.memoryInfo());
    INodeReferenceValidation.end(errorCount);
    LOG.info(Util.memoryInfo());
    return namesystem;
  }

  /**
   * INodeMap完整性校验类，检查INodeMap中是否存在不可达的无效INode
   */
  static class INodeMapValidation {
    /**
     * 执行INodeMap校验，移除无法从根节点访问的无效INode
     * @param fsdir FSDirectory对象
     * @param errorCount 错误计数器
     * @return INodeMap是否发生变更（是否移除了无效节点）
     */
    static boolean run(FSDirectory fsdir, AtomicInteger errorCount) {
      final String name = INodeMapValidation.class.getSimpleName();
      final int initErrorCount = errorCount.get();
      // 统计从根节点可达的所有INode数量
      final Counts counts = INodeCountVisitor.countTree(fsdir.getRoot());
      final INodeMap map = fsdir.getINodeMap();
      final int oldSize = map.size();
      println("%s INodeMap old size: %d", name, oldSize);
      // 遍历INodeMap，移除不可达INode并记录错误
      for (final Iterator<INodeWithAdditionalFields> j = map.getMapIterator(); j.hasNext();) {
        final INodeWithAdditionalFields i = j.next();
        if (counts.getCount(i) == 0) {
          j.remove();
          Cli.printError(errorCount, "%s (%d) is inaccessible (%s)",
              i, i.getId(), i.getFullPathName());
        }
      }
      // 输出校验结果统计
      final int newSize = map.size();
      println("%s INodeMap new size: %d", name, newSize);
      println("%s ended successfully: %d error(s) found.", name,
          errorCount.get() - initErrorCount);
      return newSize != oldSize;
    }
  }

  /**
   * 命令行交互工具类，实现Tool接口供ToolRunner调用，处理参数解析和输出