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

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.DFSUtil.ConfiguredNNAddress;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * HDFS获取配置信息的命令行工具，从配置文件中读取并输出集群各类配置信息
 * 
 * 扩展选项说明:
 * <ul>
 * <li>
 * 如果需要添加简单选项，获取配置中某个key对应的值，直接使用通用{@link GetConf.CommandHandler}。
 * 参见{@link GetConf.Command#EXCLUDE_FILE}示例。
 * </li>
 * <li>
 * 如果需要添加不返回指定key值的自定义选项，继承{@link GetConf.CommandHandler}并在{@link GetConf.Command}中注册。
 * 参见{@link GetConf.Command#NAMENODE}示例。
 * 新增选项需要在map中添加对应的{@link GetConf.CommandHandler}条目。
 * </ul>
 */
/**
 * HDFS获取配置信息的命令行工具，从配置文件读取并输出集群配置信息
 */
public class GetConf extends Configured implements Tool {
  private static final String DESCRIPTION = "hdfs getconf is utility for "
      + "getting configuration information from the config file.\n";

  /**
   * 支持的配置查询命令枚举
   */
  enum Command {
    NAMENODE("-namenodes", "gets list of namenodes in the cluster."),
    SECONDARY("-secondaryNameNodes", 
        "gets list of secondary namenodes in the cluster."),
    BACKUP("-backupNodes", "gets list of backup nodes in the cluster."),
    JOURNALNODE("-journalNodes", "gets list of journal nodes in the cluster."),
    INCLUDE_FILE("-includeFile",
        "gets the include file path that defines the datanodes " +
        "that can join the cluster."),
    EXCLUDE_FILE("-excludeFile",
        "gets the exclude file path that defines the datanodes " +
        "that need to decommissioned."),
    NNRPCADDRESSES("-nnRpcAddresses", "gets the namenode rpc addresses"),
    CONFKEY("-confKey [key]", "gets a specific key from the configuration");

    private static final Map<String, CommandHandler> map;
    static  {
      // 初始化命令与处理器的映射表
      map = new HashMap<String, CommandHandler>();
      map.put(StringUtils.toLowerCase(NAMENODE.getName()),
          new NameNodesCommandHandler());
      map.put(StringUtils.toLowerCase(SECONDARY.getName()),
          new SecondaryNameNodesCommandHandler());
      map.put(StringUtils.toLowerCase(BACKUP.getName()),
          new BackupNodesCommandHandler());
      map.put(StringUtils.toLowerCase(JOURNALNODE.getName()),
          new JournalNodeCommandHandler());
      map.put(StringUtils.toLowerCase(INCLUDE_FILE.getName()),
          new CommandHandler(DFSConfigKeys.DFS_HOSTS));
      map.put(StringUtils.toLowerCase(EXCLUDE_FILE.getName()),
          new CommandHandler(DFSConfigKeys.DFS_HOSTS_EXCLUDE));
      map.put(StringUtils.toLowerCase(NNRPCADDRESSES.getName()),
          new NNRpcAddressesCommandHandler());
      map.put(StringUtils.toLowerCase(CONFKEY.getName()),
          new PrintConfKeyCommandHandler());
    }
    
    private final String cmd;
    private final String description;

    Command(String cmd, String description) {
      this.cmd = cmd;
      this.description = description;
    }

    public String getName() {
      return cmd.split(" ")[0];
    }
    
    public String getUsage() {
      return cmd;
    }
    
    public String getDescription() {
      return description;
    }
    
    /**
     * 根据命令名称获取对应的处理器
     * @param cmd 命令名称
     * @return 对应命令处理器
     */
    public static CommandHandler getHandler(String cmd) {
      return map.get(StringUtils.toLowerCase(cmd));
    }
  }
  
  static final String USAGE;
  static {
    HdfsConfiguration.init();
    
    // 基于命令枚举生成帮助信息
    StringBuilder usage = new StringBuilder(DESCRIPTION);
    usage.append("\nhadoop getconf \n");
    for (Command cmd : Command.values()) {
      usage.append("\t[" + cmd.getUsage() + "]\t\t\t" + cmd.getDescription()
          + "\n");
    }
    USAGE = usage.toString();
  }
  
  /** 
   * 命令处理器基类，负责返回指定配置key对应的值
   */
  static class CommandHandler {
    String key; // 需要查询的配置key
    
    CommandHandler() {
      this(null);
    }
    
    CommandHandler(String key) {
      this.key = key;
    }

    /**
     * 执行命令的入口方法，处理异常
     * @param tool GetConf工具实例
     * @param args 命令参数
     * @return 执行结果状态码，0成功，-1失败
     */
    final int doWork(GetConf tool, String[] args) {
      try {
        checkArgs(args);
        return doWorkInternal(tool, args);
      } catch (Exception e) {
        tool.printError(e.getMessage());
      }
      return -1;
    }

    /**
     * 参数校验，默认不接受额外参数
     * @param args 命令参数
     */
    protected void checkArgs(String args[]) {
      if (args.length > 0) {
        throw new HadoopIllegalArgumentException(
            "Did not expect argument: " + args[0]);
      }
    }

    
    /**
     * 具体执行逻辑，子类可重写实现自定义行为
     * @param tool GetConf工具实例
     * @param args 命令参数
     * @return 执行结果状态码，0成功，-1失败
     * @throws Exception 执行异常
     */
    int doWorkInternal(GetConf tool, String[] args) throws Exception {
      // 获取配置值并输出
      String value = tool.getConf().getTrimmed(key);
      if (value != null) {
        tool.printOut(value);
        return 0;
      }
      tool.printError("Configuration " + key + " is missing.");
      return -1;
    }
  }
  
  /**
   * 获取Namenode地址的命令处理器
   */
  static class NameNodesCommandHandler extends CommandHandler {
    @Override
    int doWorkInternal(GetConf tool, String []args) throws IOException {
      tool.printMap(DFSUtil.getNNServiceRpcAddressesForCluster(tool.getConf()));
      return 0;
    }
  }
  
  /**
   * 获取BackupNode地址的命令处理器
   */
  static class BackupNodesCommandHandler extends CommandHandler {
    @Override
    public int doWorkInternal(GetConf tool, String []args) throws IOException {
      tool.printMap(DFSUtil.getBackupNodeAddresses(tool.getConf()));
      return 0;
    }
  }

  /**
   * 获取JournalNode地址的命令处理器
   */
  static class JournalNodeCommandHandler extends CommandHandler {
    @Override
    public int doWorkInternal(GetConf tool, String[] args)
        throws URISyntaxException, IOException {
      tool.printSet(DFSUtil.getJournalNodeAddresses(tool.getConf()));
      return 0;
    }
  }

  /**
   * 获取SecondaryNamenode地址的命令处理器
   */
  static class SecondaryNameNodesCommandHandler extends CommandHandler {
    @Override
    public int doWorkInternal(GetConf tool, String []args) throws IOException {
      tool.printMap(DFSUtil.getSecondaryNameNodeAddresses(tool.getConf()));
      return 0;
    }
  }
  
  /**
   * 获取Namenode RPC地址的命令处理器
   * 如果配置中定义了RPC地址则输出，否则返回空
   */
  static class NNRpcAddressesCommandHandler extends CommandHandler {
    @Override
    public int doWorkInternal(GetConf tool, String []args) throws IOException {
      Configuration config = tool.getConf();
      // 获取所有Namenode配置地址并扁平化处理
      List<ConfiguredNNAddress> cnnlist = DFSUtil.flattenAddressMap(
          DFSUtil.getNNServiceRpcAddressesForCluster(config));
      // 逐个输出格式化后的RPC地址
      if (!cnnlist.isEmpty()) {
        for (ConfiguredNNAddress cnn : cnnlist) {
          InetSocketAddress rpc = cnn.getAddress();
          tool.printOut(rpc.getHostName()+":"+rpc.getPort());
        }
        return 0;
      }
      tool.printError("Did not get namenode service rpc addresses.");
      return -1;
    }
  }
  
  /**
   * 查询指定配置key的命令处理器
   */
  static class PrintConfKeyCommandHandler extends CommandHandler {
    @Override
    protected void checkArgs(String[] args) {
      // 必须传入一个待查询的key作为参数
      if (args.length != 1) {
        throw new HadoopIllegalArgumentException(
            "usage: " + Command.CONFKEY.getUsage());
      }
    }

    @Override
    int doWorkInternal(GetConf tool, String[] args) throws Exception {
      // 从参数中获取待查询的配置key
      this.key = args[0];
      return super.doWorkInternal(tool, args);
    }
  }
  
  private final PrintStream out; // 标准输出流
  private final PrintStream err; // 错误输出流

  /**
   * 构造方法，使用默认系统输出流
   * @param conf Hadoop配置对象
   */
  GetConf(Configuration conf) {
    this(conf, System.out, System.err);
  }

  /**
   * 构造方法，指定自定义输出流
   * @param conf Hadoop配置对象
   * @param out 标准输出流
   * @param err 错误输出流
   */
  GetConf(Configuration conf, PrintStream out, PrintStream err) {
    super(conf);
    this.out = out;
    this.err = err;
  }

  /**
   * 输出错误信息
   * @param message 错误信息
   */
  void printError(String message) {
    err.println(message);
  }

  /**
   * 输出正常信息
   * @param message 输出内容
   */
  void printOut(String message) {
    out.println(message);
  }
  
  /**
   * 打印Namenode地址集合，输出为空格分隔的主机名
   * @param map 扁平化后的地址映射
   */
  void printMap(Map<String, Map<String, InetSocketAddress>> map) {
    StringBuilder buffer = new StringBuilder();

    List<ConfiguredNNAddress> cnns = DFSUtil.flattenAddressMap(map);
    for (ConfiguredNNAddress cnn : cnns) {
      InetSocketAddress address = cnn.getAddress();
      if (buffer.length() > 0) {
        buffer.append(" ");
      }
      buffer.append(address.getHostName());
    }
    printOut(buffer.toString());
  }

  /**
   * 打印JournalNode地址集合，输出为空格分隔的地址字符串
   * @param journalnodes JournalNode地址集合
   */
  void printSet(Set<String> journalnodes) {
    StringBuilder buffer = new StringBuilder();

    for (String journalnode : journalnodes) {
      if (buffer.length() > 0) {
        buffer.append(" ");
      }
      buffer.append(journalnode);
    }
    printOut(buffer.toString());
  }

  /**
   * 打印工具使用帮助信息
   */
  private void printUsage() {
    printError(USAGE);
  }

  /**
   * 处理命令行参数，分发到对应处理器执行
   * @param args 命令行参数
   * @return 执行结果状态码
   */
  private int doWork(String[] args) {
    if (args.length >= 1) {
      CommandHandler handler = Command.getHandler(args[0]);
      if (handler != null) {
        // 截取剩余参数传递给处理器
        return handler.doWork(this,
            Arrays.copyOfRange(args, 1, args.length));
      }
    }
    // 参数不合法，打印帮助信息返回错误
    printUsage();
    return -1;
  }

  @Override
  /**
   * Tool接口的run方法，以当前用户身份执行命令
   * @param args 命令行参数
   * @return 执行结果状态码
   * @throws Exception 执行异常
   */
  public int run(final String[] args) throws Exception {
    try {
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
   * 工具主入口方法
   * @param args 命令行参数
   * @throws Exception 执行异常
   */
  public static void main(String[] args) throws Exception {
    // 处理-help参数，输出帮助信息后退出
    if (DFSUtil.parseHelpArgument(args, USAGE, System.out, true)) {
      System.exit(0);
    }
    
    // 通过ToolRunner运行工具
    int res = ToolRunner.run(new GetConf(new HdfsConfiguration()), args);
    System.exit(res);
  }
}