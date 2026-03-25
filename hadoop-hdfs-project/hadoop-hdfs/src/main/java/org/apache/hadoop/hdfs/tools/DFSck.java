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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.NamenodeFsck;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * HDFS 文件系统检查工具，用于检测DFS文件系统的错误和非最优状态。
 * 核心功能包括：从指定根路径扫描所有文件目录，检测块丢失、副本数不足/过量等异常，
 * 支持移动损坏文件到/lost+found、删除损坏文件等修复操作，同时生成全局文件系统统计信息。
 * 该工具通过HTTP接口调用NameNode端的NamenodeFsck完成实际检查逻辑。
 */
@InterfaceAudience.Private
public class DFSck extends Configured implements Tool {
  static{
    // 初始化HDFS配置
    HdfsConfiguration.init();
  }

  /** 使用说明字符串，定义了fsck命令的所有参数选项 */
  private static final String USAGE = "Usage: hdfs fsck <path> "
      + "[-list-corruptfileblocks | "
      + "[-move | -delete | -openforwrite] "
      + "[-files [-blocks [-locations | -racks | -replicaDetails | " +
          "-upgradedomains]]]] "
      + "[-includeSnapshots] [-showprogress] "
      + "[-storagepolicies] [-maintenance] "
      + "[-blockId <blk_Id>] [-replicate]\n"
      + "\t<path>\tstart checking from this path\n"
      + "\t-move\tmove corrupted files to /lost+found\n"
      + "\t-delete\tdelete corrupted files\n"
      + "\t-files\tprint out files being checked\n"
      + "\t-openforwrite\tprint out files opened for write\n"
      + "\t-includeSnapshots\tinclude snapshot data if the given path"
      + " indicates a snapshottable directory or there are "
      + "snapshottable directories under it\n"
      + "\t-list-corruptfileblocks\tprint out list of missing "
      + "blocks and files they belong to\n"
      + "\t-files -blocks\tprint out block report\n"
      + "\t-files -blocks -locations\tprint out locations for every block\n"
      + "\t-files -blocks -racks" 
      + "\tprint out network topology for data-node locations\n"
      + "\t-files -blocks -replicaDetails\tprint out each replica details \n"
      + "\t-files -blocks -upgradedomains\tprint out upgrade domains for " +
          "every block\n"
      + "\t-storagepolicies\tprint out storage policy summary for the blocks\n"
      + "\t-maintenance\tprint out maintenance state node details\n"
      + "\t-showprogress\tDeprecated. Progress is now shown by default\n"
      + "\t-blockId\tprint out which file this blockId belongs to, locations"
      + " (nodes, racks) of this block, and other diagnostics info"
      + " (under replicated, corrupted or not, etc)\n"
      + "\t-replicate initiate replication work to make mis-replicated"
      + " blocks satisfy block placement policy\n\n"
      + "Please Note:\n\n"
      + "\t1. By default fsck ignores files opened for write, "
      + "use -openforwrite to report such files. They are usually "
      + " tagged CORRUPT or HEALTHY depending on their block "
      + "allocation status\n"
      + "\t2. Option -includeSnapshots should not be used for comparing stats,"
      + " should be used only for HEALTH check, as this may contain duplicates"
      + " if the same file present in both original fs tree "
      + "and inside snapshots.";
  
  /** 当前用户信息，用于权限认证 */
  private final UserGroupInformation ugi;
  /** 标准输出流，用于输出检查结果 */
  private final PrintStream out;
  /** HTTP连接工厂，用于创建到NameNode的HTTP连接 */
  private final URLConnectionFactory connectionFactory;
  /** 是否启用SPNEGO认证，安全模式开启时启用 */
  private final boolean isSpnegoEnabled;

  /**
   * 构造DFSck检查工具实例，使用默认输出流System.out
   * @param conf 当前配置对象
   */
  public DFSck(Configuration conf) throws IOException {
    this(conf, System.out);
  }

  /**
   * 构造DFSck检查工具实例，指定自定义输出流
   * @param conf 当前配置对象
   * @param out 输出检查结果的流
   */
  public DFSck(Configuration conf, PrintStream out) throws IOException {
    super(conf);
    // 获取当前登录用户信息
    this.ugi = UserGroupInformation.getCurrentUser();
    this.out = out;
    // 从配置读取fsck HTTP连接超时时间
    int connectTimeout = (int) conf.getTimeDuration(
        HdfsClientConfigKeys.DFS_CLIENT_FSCK_CONNECT_TIMEOUT,
        HdfsClientConfigKeys.DFS_CLIENT_FSCK_CONNECT_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);
    // 从配置读取fsck HTTP读取超时时间
    int readTimeout = (int) conf.getTimeDuration(
        HdfsClientConfigKeys.DFS_CLIENT_FSCK_READ_TIMEOUT,
        HdfsClientConfigKeys.DFS_CLIENT_FSCK_READ_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);

    // 创建带超时配置的HTTP连接工厂
    this.connectionFactory = URLConnectionFactory
        .newDefaultURLConnectionFactory(connectTimeout, readTimeout, conf);
    // 根据安全模式状态确定是否启用SPNEGO认证
    this.isSpnegoEnabled = UserGroupInformation.isSecurityEnabled();
  }

  /**
   * 打印fsck命令使用帮助信息
   * @param out 输出流
   */
  static void printUsage(PrintStream out) {
    out.println(USAGE + "\n");
    ToolRunner.printGenericCommandUsage(out);
  }

  @Override
  public int run(final String[] args) throws IOException {
    if (args.length == 0) {
      printUsage(System.err);
      return -1;
    }

    try {
      // 在当前用户权限上下文执行检查任务
      return UserGroupInformation.getCurrentUser().doAs(
          new PrivilegedExceptionAction<Integer>() {
            @Override
            public Integer run() throws Exception {
              return doWork(args);
            }
          });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
  
  /**
   * 分页获取并输出所有损坏块列表，需要迭代调用直到服务端返回没有更多数据
   * @param dir 检查的根目录路径
   * @param baseUrl 构建好的fsck请求基础URL
   * @return 错误码，0表示没有损坏块，非0表示存在错误
   */
  private Integer listCorruptFileBlocks(String dir, String baseUrl)
      throws IOException {
    int errCode = -1;
    // 损坏块计数器
    int numCorrupt = 0;
    // 分页cookie，标记当前分页位置
    int cookie = 0;
    final String noCorruptLine = "has no CORRUPT files";
    final String noMoreCorruptLine = "has no more CORRUPT files";
    final String cookiePrefix = "Cookie:";
    boolean allDone = false;
    // 循环分页获取损坏块
    while (!allDone) {
      final StringBuilder url = new StringBuilder(baseUrl);
      // 如果不是第一页，添加分页偏移参数
      if (cookie > 0) {
        url.append("&startblockafter=").append(String.valueOf(cookie));
      }
      URL path = new URL(url.toString());
      URLConnection connection;
      try {
        // 打开HTTP连接
        connection = connectionFactory.openConnection(path, isSpnegoEnabled);
      } catch (AuthenticationException e) {
        throw new IOException(e);
      }
      InputStream stream = connection.getInputStream();
      BufferedReader input = new BufferedReader(new InputStreamReader(
          stream, StandardCharsets.UTF_8));
      try {
        String line = null;
        while ((line = input.readLine()) != null) {
          // 读取分页cookie，用于下一页请求
          if (line.startsWith(cookiePrefix)){
            try{
              cookie = Integer.parseInt(line.split("\t")[1]);
            } catch (Exception e){
              // 解析失败则结束分页
              allDone = true;
              break;
            }
            continue;
          }
          // 判断是否已经获取完所有损坏块
          if ((line.endsWith(noCorruptLine)) ||
              (line.endsWith(noMoreCorruptLine)) ||
              (line.endsWith(NamenodeFsck.NONEXISTENT_STATUS))) {
            allDone = true;
            break;
          }
          // 处理权限拒绝错误
          if (line.startsWith("Access denied for user")) {
            out.println("Failed to open path '" + dir + "': Permission denied");
            errCode = -1;
            return errCode;
          }
          // 跳过标题和统计行，只输出损坏块信息
          if ((line.isEmpty())
              || (line.startsWith("FSCK started by"))
              || (line.startsWith("FSCK ended at"))
              || (line.startsWith("The filesystem under path")))
            continue;
          // 计数并输出损坏块信息
          numCorrupt++;
          if (numCorrupt == 1) {
            out.println("The list of corrupt blocks under path '"
                + dir + "' are:");
          }
          out.println(line);
        }
      } finally {
        input.close();
      }
    }
    // 输出汇总统计信息
    out.println("The filesystem under path '" + dir + "' has " 
        + numCorrupt + " CORRUPT blocks");
    // 如果没有损坏块返回成功码
    if (numCorrupt == 0)
      errCode = 0;
    return errCode;
  }
  

  /**
   * 解析输入路径，获取去除了URI前缀的绝对路径
   * @param dir 输入的路径字符串
   * @return 解析后的路径对象
   */
  private Path getResolvedPath(String dir) throws IOException {
    Configuration conf = getConf();
    Path dirPath = new Path(dir);
    FileSystem fs = dirPath.getFileSystem(conf);
    return fs.resolvePath(dirPath);
  }

  /**
   * 从当前文件系统配置中获取活跃NameNode的HTTP服务地址
   * HA模式下会自动获取当前活跃节点地址
   * @param target 目标检查路径，用于获取对应的文件系统实例
   * @return 活跃NameNode的HTTP地址URI，失败返回null
   * @throws IOException 无法获取活跃地址时抛出异常
   */
  private URI getCurrentNamenodeAddress(Path target) throws IOException {
    Configuration conf = getConf();

    // 获取目标路径对应的文件系统实例
    final FileSystem fs = target.getFileSystem(conf);
    // 检查文件系统是否为HDFS实例
    if (!(fs instanceof DistributedFileSystem)) {
      System.err.println("FileSystem is " + fs.getUri());
      return null;
    }

    // 获取活跃NameNode的HTTP信息服务地址
    return DFSUtil.getInfoServer(HAUtil.getAddressOfActive(fs), conf,
        DFSUtil.getHttpClientScheme(conf));
  }

  /**
   * 解析命令行参数，构造HTTP请求调用NameNode的fsck接口，返回检查结果
   * @param args 命令行参数数组
   * @return 错误码，对应不同健康状态：0=健康/不存在路径/块格式错误、1=损坏、2=已退役块存在、其他对应不同异常状态
   */
  private int doWork(final String[] args) throws IOException {
    final StringBuilder url = new StringBuilder();
    
    // 添加当前用户名参数到请求URL
    url.append("/fsck?ugi=").append(ugi.getShortUserName());
    String dir = null;
    boolean doListCorruptFileBlocks = false;
    // 遍历解析所有命令行参数
    for (int idx = 0; idx < args.length; idx++) {
      if (args[idx].equals("-move")) { url.append("&move=1"); }
      else if (args[idx].equals("-delete")) { url.append("&delete=1"); }
      else if (args[idx].equals("-files")) { url.append("&files=1"); }
      else if (args[idx].equals("-openforwrite")) { url.append("&openforwrite=1"); }
      else if (args[idx].equals("-blocks")) { url.append("&blocks=1"); }
      else if (args[idx].equals("-locations")) { url.append("&locations=1"); }
      else if (args[idx].equals("-racks")) { url.append("&racks=1"); }
      else if (args[idx].equals("-replicaDetails")) {
        url.append("&replicadetails=1");
      } else if (args[idx].equals("-upgradedomains")) {
        url.append("&upgradedomains=1");
      } else if (args[idx].equals("-storagepolicies")) {
        url.append("&storagepolicies=1");
      } else if (args[idx].equals("-showprogress")) {
        url.append("&showprogress=1");
      } else if (args[idx].equals("-list-corruptfileblocks")) {
        url.append("&listcorruptfileblocks=1");
        doListCorruptFileBlocks = true;
      } else if (args[idx].equals("-includeSnapshots")) {
        url.append("&includeSnapshots=1");
      } else if (args[idx].equals("-maintenance")) {
        url.append("&maintenance=1");
      } else if (args[idx].equals("-blockId")) {
        // 解析块ID参数，支持带空格的块ID输入
        StringBuilder sb = new StringBuilder();
        idx++;
        while(idx < args.length && !args[idx].startsWith("-")){
          sb.append(args[idx]);
          sb.append(" ");
          idx++;
        }
        url.append("&blockId=").append(URLEncoder.encode(sb.toString(), "UTF-8"));
      } else if (args[idx].equals("-replicate")) {
        url.append("&replicate=1");
      } else if (!args[idx].startsWith("-")) {
        // 处理路径参数，fsck只支持一次检查一个路径
        if (null == dir) {
          dir = args[idx];
        } else {
          System.err.println("fsck: can only operate on one path at a time '"
              + args[idx] + "'");
          printUsage(System.err);
          return -1;
        }

      } else {
        // 无法识别的参数，输出错误
        System.err.println("fsck: Illegal option '" + args[idx] + "'");
        printUsage(System.err);
        return -1;
      }
    }
    // 默认检查根路径
    if (null == dir) {
      dir = "/";
    }

    Path dirpath = null;
    URI namenodeAddress = null;
    try {
      // 解析目标路径
      dirpath = getResolvedPath(dir);
      // 获取活跃NameNode地址
      namenodeAddress = getCurrentNamenodeAddress(dirpath);
    } catch (IOException ioe) {
      System.err.println("FileSystem is inaccessible due to:\n"
          + ioe.toString());
    }

    if (namenodeAddress == null) {
      // 获取NameNode地址失败，退出
      System.err.println("DFSck exiting.");
      return 0;
    }

    // 拼接完整请求URL，添加路径参数
    url.insert(0, namenodeAddress.toString());
    url.append("&path=").append(URLEncoder.encode(
        Path.getPathWithoutSchemeAndAuthority