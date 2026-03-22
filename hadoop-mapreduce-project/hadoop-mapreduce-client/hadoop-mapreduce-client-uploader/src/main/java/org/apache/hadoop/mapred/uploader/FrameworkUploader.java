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

package org.apache.hadoop.mapred.uploader;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NotLinkException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

/**
 * 将MapReduce框架jar包打包上传到HDFS的工具类，用于YARN应用分发MapReduce框架依赖，实现集群共享框架避免每个作业携带。
 * 使用方法:
 * sudo -u mapred mapred frameworkuploader -fs hdfs://`hostname`:8020 -target /tmp/upload.tar.gz#mr-framework
*/
public class FrameworkUploader implements Runnable {
  // 环境变量替换正则匹配器
  private static final Pattern VAR_SUBBER =
      Pattern.compile(Shell.getEnvironmentVariableRegex());
  private static final Logger LOG =
      LoggerFactory.getLogger(FrameworkUploader.class);
  private Configuration conf = new Configuration();

  // 上传框架文件所需的最小权限
  private static final FsPermission FRAMEWORK_PERMISSION =
      new FsPermission(0644);

  @VisibleForTesting
  String input = null;
  @VisibleForTesting
  String whitelist = null;
  @VisibleForTesting
  String blacklist = null;
  @VisibleForTesting
  String target = null;
  @VisibleForTesting
  Path targetPath = null;
  @VisibleForTesting
  short initialReplication = 3;
  @VisibleForTesting
  short finalReplication = 10;
  @VisibleForTesting
  short acceptableReplication = 9;
  @VisibleForTesting
  int timeout = 10;
  private boolean ignoreSymlink = false;

  @VisibleForTesting
  Set<String> filteredInputFiles = new HashSet<>();
  @VisibleForTesting
  List<Pattern> whitelistedFiles = new LinkedList<>();
  @VisibleForTesting
  List<Pattern> blacklistedFiles = new LinkedList<>();

  private OutputStream targetStream = null;
  private FSDataOutputStream fsDataStream = null;
  private String alias = null;

  /**
   * 设置配置对象，覆盖默认配置。
   * @param configuration 配置对象
   */
  @VisibleForTesting
  void setConf(Configuration configuration) {
    conf = configuration;
  }

  /**
   * 打印帮助信息，输出可用命令行参数。
   * @param options 命令行选项定义
   */
  private void printHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("mapred frameworkuploader", options);
  }

  /**
   * 执行上传流程主逻辑，收集需要打包的jar、构建tar包并上传到HDFS。
   */
  public void run() {
    try {
      collectPackages();
      buildPackage();
      LOG.info("Uploaded " + target);
      System.out.println("Suggested mapreduce.application.framework.path " +
          target);
      LOG.info(
          "Suggested mapreduce.application.classpath $PWD/" + alias + "/*");
      System.out.println("Suggested classpath $PWD/" + alias + "/*");
    } catch (UploaderException|IOException|InterruptedException e) {
      LOG.error("Error in execution " + e.getMessage());
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /**
   * 收集符合白名单/黑名单规则的需要打包的MapReduce框架jar包。
   * @throws UploaderException 打包过程异常
   */
  @VisibleForTesting
  void collectPackages() throws UploaderException {
    parseLists();
    String[] list = StringUtils.split(input, File.pathSeparatorChar);
    // 遍历所有输入路径，按规则过滤jar包
    for (String item : list) {
      LOG.info("Original source " + item);
      String expanded = expandEnvironmentVariables(item, System.getenv());
      LOG.info("Expanded source " + expanded);
      // 通配符目录情况，添加目录下所有jar
      if (expanded.endsWith("*")) {
        File path = new File(expanded.substring(0, expanded.length() - 1));
        if (path.isDirectory()) {
          File[] files = path.listFiles();
          if (files != null) {
            for (File jar : files) {
              if (!jar.isDirectory()) {
                addJar(jar);
              } else {
                LOG.info("Ignored " + jar + " because it is a directory");
              }
            }
          } else {
            LOG.warn("Could not list directory " + path);
          }
        } else {
          LOG.warn("Ignored " + expanded + ". It is not a directory");
        }
      // 单个jar文件，直接添加
      } else if (expanded.endsWith(".jar")) {
        File jarFile = new File(expanded);
        addJar(jarFile);
      // 不支持的类型，跳过
      } else if (!expanded.isEmpty()) {
        LOG.warn("Ignored " + expanded + " only jars are supported");
      }
    }
  }

  /**
   * 初始化上传流程，创建目标HDFS文件并配置参数。
   * @throws IOException IO异常
   * @throws UploaderException 上传异常
   */
  @VisibleForTesting
  void beginUpload() throws IOException, UploaderException {
    if (targetStream == null) {
      // 解析目标路径，分离HDFS路径和别名
      int lastIndex = target.indexOf('#');
      targetPath =
          new Path(
              target.substring(
                  0, lastIndex == -1 ? target.length() : lastIndex));
      alias = lastIndex != -1 ?
          target.substring(lastIndex + 1) :
          targetPath.getName();
      LOG.info("Target " + targetPath);
      FileSystem fileSystem = targetPath.getFileSystem(conf);

      targetStream = null;
      // 针对HDFS分布式文件系统，配置初始副本数和禁用纠删码
      if (fileSystem instanceof DistributedFileSystem) {
        LOG.info("Set replication to " +
            initialReplication + " for path: " + targetPath);
        LOG.info("Disabling Erasure Coding for path: " + targetPath);
        DistributedFileSystem dfs = (DistributedFileSystem)fileSystem;
        DistributedFileSystem.HdfsDataOutputStreamBuilder builder =
            dfs.createFile(targetPath)
            .overwrite(true)
            .ecPolicyName(
                SystemErasureCodingPolicies.getReplicationPolicy().getName());
        if (initialReplication > 0) {
          builder.replication(initialReplication);
        }
        targetStream = builder.build();
      } else {
        LOG.warn("Cannot set replication to " +
            initialReplication + " for path: " + targetPath +
            " on a non-distributed filesystem " +
            fileSystem.getClass().getName());
      }
      // 非HDFS文件系统走默认创建流程
      if (targetStream == null) {
        targetStream = fileSystem.create(targetPath, true);
      }

      // 设置框架文件权限为所有用户可读
      if (!FRAMEWORK_PERMISSION.equals(
          FRAMEWORK_PERMISSION.applyUMask(FsPermission.getUMask(conf)))) {
        LOG.info("Modifying permissions to " + FRAMEWORK_PERMISSION);
        fileSystem.setPermission(targetPath, FRAMEWORK_PERMISSION);
      }

      fsDataStream = (FSDataOutputStream) targetStream;
      // 如果是压缩后缀，包装为GZip输出流
      if (targetPath.getName().endsWith("gz") ||
          targetPath.getName().endsWith("tgz")) {
        LOG.info("Creating GZip");
        targetStream = new GZIPOutputStream(targetStream);
      }

      // 向上遍历目录树，检查所有父目录是否对所有用户开放执行权限（保证所有用户能访问框架包）
      Path current = targetPath.getParent();
      while (current != null) {
        try {
          FileStatus fstat = fileSystem.getFileStatus(current);
          FsPermission perm = fstat.getPermission();

          // 进入目录只需要执行权限，不需要读权限
          boolean userCanEnter = perm.getUserAction()
              .implies(FsAction.EXECUTE);
          boolean groupCanEnter = perm.getGroupAction()
              .implies(FsAction.EXECUTE);
          boolean othersCanEnter = perm.getOtherAction()
              .implies(FsAction.EXECUTE);

          if (!userCanEnter || !groupCanEnter || !othersCanEnter) {
            LOG.warn("Path " + current + " is not accessible"
                + " for all users. Current permissions are: " + perm);
            LOG.warn("Please set EXECUTE permissions on this directory");
          }
          current = current.getParent();
        } catch (AccessControlException e) {
          LOG.warn("Path " + current + " is not accessible,"
              + " cannot retrieve permissions");
          LOG.warn("Please set EXECUTE permissions on this directory");
          LOG.debug("Stack trace", e);
          break;
        }
      }
    }
  }

  /**
   * 获取文件所有数据块中最小的副本数，用于检查是否达到期望副本数。
   * @return 所有块中最小的副本数
   * @throws IOException IO异常
   */
  private long getSmallestReplicatedBlockCount()
      throws IOException {
    FileSystem fileSystem = targetPath.getFileSystem(conf);
    FileStatus status = fileSystem.getFileStatus(targetPath);
    long length = status.getLen();
    HashMap<Long, Integer> blockCount = new HashMap<>();

    // 初始化每个块的副本计数为0
    for (long offset = 0; offset < length; offset +=status.getBlockSize()) {
      blockCount.put(offset, 0);
    }

    // 统计每个块的实际副本数
    BlockLocation[] locations = fileSystem.getFileBlockLocations(
        targetPath, 0, length);
    for(BlockLocation location: locations) {
      final int replicas = location.getHosts().length;
      blockCount.compute(
          location.getOffset(),
          (key, value) -> value == null ? 0 : value + replicas);
    }

    // 输出每个块的副本计数日志
    for (long offset = 0; offset < length; offset +=status.getBlockSize()) {
      LOG.info(String.format(
          "Replication counts offset:%d blocks:%d",
          offset, blockCount.get(offset)));
    }

    // 返回最小副本数
    return Collections.min(blockCount.values());
  }

  /**
   * 结束上传流程，设置最终副本数并等待副本复制完成。
   * @throws IOException IO异常
   * @throws InterruptedException 等待中断异常
   */
  private void endUpload()
      throws IOException, InterruptedException {
    FileSystem fileSystem = targetPath.getFileSystem(conf);
    // 对HDFS设置最终期望副本数
    if (fileSystem instanceof DistributedFileSystem) {
      fileSystem.setReplication(targetPath, finalReplication);
      LOG.info("Set replication to " +
          finalReplication + " for path: " + targetPath);
      // 超时为0则跳过副本检查
      if (timeout == 0) {
        LOG.info("Timeout is set to 0. Skipping replication check.");
      } else {
        // 循环等待直到达到可接受副本数或超时
        long startTime = System.currentTimeMillis();
        long endTime = startTime;
        long currentReplication = 0;
        while(endTime - startTime < timeout * 1000 &&
             currentReplication < acceptableReplication) {
          Thread.sleep(1000);
          endTime = System.currentTimeMillis();
          currentReplication = getSmallestReplicatedBlockCount();
        }
        if (endTime - startTime >= timeout * 1000) {
          LOG.error(String.format(
              "Timed out after %d seconds while waiting for acceptable" +
                  " replication of %d (current replication is %d)",
              timeout, acceptableReplication, currentReplication));
        }
      }
    } else {
      LOG.info("Cannot set replication to " +
          finalReplication + " for path: " + targetPath +
          " on a non-distributed filesystem " +
          fileSystem.getClass().getName());
    }
  }

  /**
   * 构建框架tar包并上传到HDFS，主打包流程。
   * @throws IOException IO异常
   * @throws UploaderException 打包异常
   * @throws InterruptedException 等待副本异常
   */
  @VisibleForTesting
  void buildPackage()
      throws IOException, UploaderException, InterruptedException {
    beginUpload();
    LOG.info("Compressing tarball");
    // 流式写入tar包到输出流
    try (TarArchiveOutputStream out = new TarArchiveOutputStream(
        targetStream)) {
      // 解决大文件编号问题，兼容STAR格式
      out.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_STAR);
      // 逐个添加过滤后的jar包到tar包
      for (String fullPath : filteredInputFiles) {
        LOG.info("Adding " + fullPath);
        File file = new File(fullPath);
        try (FileInputStream inputStream = new FileInputStream(file)) {
          TarArchiveEntry entry = out.createArchiveEntry(file, file.getName());
          out.putArchiveEntry(entry);
          IOUtils.copyBytes(inputStream, out, 1024 * 1024);
          out.closeArchiveEntry(entry);
        }
      }

      // 刷新缓冲区，确保数据写入，让副本统计能正确获取数据
      fsDataStream.hflush();

      endUpload();
    } finally {
      if (targetStream != null) {
        targetStream.close();
      }
    }
  }

  /**
   * 解析白名单和黑名单的正则表达式列表，展开环境变量。
   * @throws UploaderException 解析异常
   */
  private void parseLists() throws UploaderException {
    Map<String, String> env = System.getenv();
    for(Map.Entry<String, String> item : env.entrySet()) {
      LOG.info("Environment " + item.getKey() + " " + item.getValue());
    }
    // 编译白名单正则
    String[] whiteListItems = StringUtils.split(whitelist);
    for (String pattern : whiteListItems) {
      String expandedPattern =
          expandEnvironmentVariables(pattern, env);
      Pattern compiledPattern =
          Pattern.compile("^" + expandedPattern + "$");
      LOG.info("Whitelisted " + compiledPattern.toString());
      whitelistedFiles.add(compiledPattern);
    }
    // 编译黑名单正则
    String[] blacklistItems = StringUtils.split(blacklist);
    for (String pattern : blacklistItems) {
      String expandedPattern =
          expandEnvironmentVariables(pattern, env);
      Pattern compiledPattern =
          Pattern.compile("^" + expandedPattern + "$");
      LOG.info("Blacklisted " + compiledPattern.toString());
      blacklistedFiles.add(compiledPattern);
    }
  }

  /**
   * 展开输入字符串中的环境变量，递归替换直到没有可替换变量。
   * @param innerInput 输入字符串
   * @param env 环境变量Map
   * @return 替换后的字符串
   * @throws UploaderException 环境变量不存在异常
   */
  @VisibleForTesting
  String expandEnvironmentVariables(String innerInput, Map<String, String> env)