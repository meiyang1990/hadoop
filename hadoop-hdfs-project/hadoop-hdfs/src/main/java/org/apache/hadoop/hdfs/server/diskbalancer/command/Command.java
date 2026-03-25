// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdfs.server.diskbalancer.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.TextStringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.server.diskbalancer.DiskBalancerConstants;
import org.apache.hadoop.hdfs.server.diskbalancer.DiskBalancerException;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.ClusterConnector;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.ConnectorFactory;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerCluster;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerDataNode;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolume;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolumeSet;
import org.apache.hadoop.hdfs.tools.DiskBalancerCLI;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * 文件说明：磁盘平衡器所有命令的抽象基类，定义了命令执行的通用接口和公共基础能力
 * 所有具体磁盘平衡器命令均继承此类，复用通用的集群信息读取、节点解析、输出路径管理等能力
 */
public abstract class Command extends Configured implements Closeable {
  // JSON解析读取器，用于解析数据节点返回的卷UUID与物理路径映射
  private static final ObjectReader READER =
      new ObjectMapper().readerFor(HashMap.class);
  static final Logger LOG = LoggerFactory.getLogger(Command.class);
  // 存储当前命令支持的合法参数，key为参数长名，value为参数描述
  private Map<String, String> validArgs = new HashMap<>();
  // 目标HDFS集群的URI地址
  private URI clusterURI;
  // 操作输出目录所在的文件系统实例
  private FileSystem fs = null;
  // 当前集群的磁盘平衡器数据模型实例
  private DiskBalancerCluster cluster = null;
  // 限制本次操作处理的节点数量，0表示不限制，仅处理不平衡度最高的topN个节点
  private int topNodes;
  // 标准输出流，用于命令输出
  private PrintStream ps;

  // HDFS上磁盘平衡器日志/计划文件的默认根目录
  private static final Path DEFAULT_LOG_DIR = new Path("/system/diskbalancer");

  // 当前命令的输出目录，用于存储计划文件和快照
  private Path diskBalancerLogs;

  /**
   * 构造命令对象，使用默认标准输出流
   * @param conf Hadoop配置对象
   */
  public Command(Configuration conf) {
    this(conf, System.out);
  }

  /**
   * 构造命令对象，可指定输出流
   * @param conf Hadoop配置对象
   * @param ps 输出流
   */
  public Command(Configuration conf, final PrintStream ps) {
    super(conf);
    // These arguments are valid for all commands.
    topNodes = 0;
    this.ps = ps;
  }

  /**
   * 清理命令占用的资源，关闭文件系统连接
   * 主要用于删除标记磁盘平衡器运行的锁文件，避免连续运行冲突
   */
  @Override
  public void close() throws IOException {
    if (fs != null) {
      fs.close();
    }
  }

  /**
   * 获取当前命令的输出流
   * @return 输出流实例
   */
  PrintStream getPrintStream() {
    return ps;
  }

  /**
   * 执行命令的抽象接口，具体命令需实现此方法完成自身逻辑
   * @param cmd 解析后的命令行参数
   * @throws Exception 执行过程中抛出的异常
   */
  public abstract void execute(CommandLine cmd) throws Exception;

  /**
   * 打印当前命令的帮助信息抽象接口
   */
  public abstract void printHelp();

  /**
   * 从集群读取完整的节点和磁盘信息，构建磁盘平衡器集群数据模型
   * 所有命令都调用此方法获取集群拓扑信息
   * @param cmd 命令行参数
   * @return 构建完成的磁盘平衡器集群数据模型
   * @throws Exception 读取过程中抛出的异常
   */
  protected DiskBalancerCluster readClusterInfo(CommandLine cmd) throws
      Exception {
    Preconditions.checkNotNull(cmd);

    setClusterURI(FileSystem.getDefaultUri(getConf()));
    LOG.debug("using name node URI : {}", this.getClusterURI());
    // 根据集群URI获取对应类型的集群连接器
    ClusterConnector connector = ConnectorFactory.getCluster(this.clusterURI,
        getConf());

    cluster = new DiskBalancerCluster(connector);

    LOG.debug("Reading cluster info");
    // 读取集群节点和磁盘信息填充数据模型
    cluster.readClusterInfo();
    return cluster;
  }

  /**
   * 初始化当前命令的输出目录，用于存储计划文件和输出日志
   * @param path 用户指定的输出路径，为空则使用默认路径
   * @throws IOException 文件系统操作异常
   */
  protected void setOutputPath(String path) throws IOException {
    // 用当前时间戳生成唯一输出目录，避免冲突
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MMM-dd-HH-mm-ss");
    Date now = new Date();

    fs = FileSystem.get(getClusterURI(), getConf());
    if (path == null || path.isEmpty()) {
      // 如果是本地文件模式，输出到当前工作目录下
      if (getClusterURI().getScheme().startsWith("file")) {
        diskBalancerLogs = new Path(
            System.getProperty("user.dir") + DEFAULT_LOG_DIR.toString() +
                Path.SEPARATOR + format.format(now));
      } else {
        // HDFS模式使用默认根目录下生成时间戳目录
        diskBalancerLogs = new Path(DEFAULT_LOG_DIR.toString() +
            Path.SEPARATOR + format.format(now));
      }
    } else {
      // 使用用户指定的输出路径
      diskBalancerLogs = new Path(path);
    }
    // 检查输出目录是否已存在，避免覆盖已有数据
    if (fs.exists(diskBalancerLogs)) {
      LOG.debug("Another Diskbalancer instance is running ? - Target " +
          "Directory already exists. {}", diskBalancerLogs);
      throw new IOException("Another DiskBalancer files already exist at the " +
          "target location. " + diskBalancerLogs.toString());
    }
    // 创建输出目录
    fs.mkdirs(diskBalancerLogs);
  }

  /**
   * 设置需要处理的单个目标节点
   * @param node 目标数据节点
   */
  protected void setNodesToProcess(DiskBalancerDataNode node) {
    List<DiskBalancerDataNode> nodelist = new LinkedList<>();
    nodelist.add(node);
    setNodesToProcess(nodelist);
  }

  /**
   * 设置需要处理的目标节点列表，覆盖集群默认所有节点
   * @param nodes 目标数据节点列表
   */
  protected void setNodesToProcess(List<DiskBalancerDataNode> nodes) {
    if (cluster == null) {
      throw new IllegalStateException("Set nodes to process invoked before " +
          "initializing cluster. Illegal usage.");
    }
    cluster.setNodesToProcess(nodes);
  }

  /**
   * 根据名称/IP/UUID从集群中查找匹配的数据节点
   * @param nodeName 节点的主机名、IP地址或UUID
   * @return 匹配到的数据节点，未找到返回null
   */
  DiskBalancerDataNode getNode(String nodeName) {
    DiskBalancerDataNode node = null;
    if (nodeName == null || nodeName.isEmpty()) {
      return node;
    }
    if (cluster.getNodes().size() == 0) {
      return node;
    }
    // 优先按主机名查找
    node = cluster.getNodeByName(nodeName);
    if (node != null) {
      return node;
    }
    // 主机名找不到按IP查找
    node = cluster.getNodeByIPAddress(nodeName);
    if (node != null) {
      return node;
    }
    // IP找不到按UUID查找
    node = cluster.getNodeByUUID(nodeName);
    return node;
  }

  /**
   * 从输入参数解析出目标节点名称集合，支持文件URL或逗号分隔字符串两种格式
   * @param listArg 输入参数，可为file://开头的文件路径或逗号分隔的节点列表
   * @return 解析后的节点名称集合
   * @throws IOException 文件读取或参数解析异常
   */
  protected Set<String> getNodeList(String listArg) throws IOException {
    URL listURL;
    String nodeData;
    Set<String> resultSet = new TreeSet<>();

    if ((listArg == null) || listArg.isEmpty()) {
      return resultSet;
    }
    // 如果是file://协议，从本地文件读取节点列表
    if (listArg.startsWith("file://")) {
      listURL = new URL(listArg);
      try {
        HostsFileReader.readFileToSet("include",
            Paths.get(listURL.getPath()).toString(), resultSet);
      } catch (NoSuchFileException e) {
        String warnMsg = String
            .format("The input host file path '%s' is not a valid path. "
                + "Please make sure the host file exists.", listArg);
        throw new DiskBalancerException(warnMsg,
            DiskBalancerException.Result.INVALID_HOST_FILE_PATH);
      }
    } else {
      // 逗号分隔的节点列表，直接分割解析
      nodeData = listArg;
      String[] nodes = nodeData.split(",");

      if (nodes.length == 0) {
        String warnMsg = "The number of input nodes is 0. "
            + "Please input the valid nodes.";
        throw new DiskBalancerException(warnMsg,
            DiskBalancerException.Result.INVALID_NODE);
      }

      Collections.addAll(resultSet, nodes);
    }

    return resultSet;
  }

  /**
   * 根据输入参数解析获取匹配的目标数据节点列表
   * @param listArg 输入参数，可为file://开头的文件路径或逗号分隔的节点列表
   * @return 解析后的目标数据节点列表
   * @throws IOException 节点不存在或解析异常
   */
  protected List<DiskBalancerDataNode> getNodes(String listArg)
      throws IOException {
    Set<String> nodeNames = null;
    List<DiskBalancerDataNode> nodeList = Lists.newArrayList();
    List<String> invalidNodeList = Lists.newArrayList();

    if ((listArg == null) || listArg.isEmpty()) {
      return nodeList;
    }
    // 先解析得到节点名称集合
    nodeNames = getNodeList(listArg);

    DiskBalancerDataNode node = null;
    if (!nodeNames.isEmpty()) {
      // 逐个查找节点，收集无效节点
      for (String name : nodeNames) {
        node = getNode(name);

        if (node != null) {
          nodeList.add(node);
        } else {
          invalidNodeList.add(name);
        }
      }
    }
    // 如果存在无效节点，抛出异常提示用户
    if (!invalidNodeList.isEmpty()) {
      String invalidNodes = StringUtils.join(invalidNodeList.toArray(), ",");
      String warnMsg = String.format(
          "The node(s) '%s' not found. "
          + "Please make sure that '%s' exists in the cluster.",
          invalidNodes, invalidNodes);
      throw new DiskBalancerException(warnMsg,
          DiskBalancerException.Result.INVALID_NODE);
    }

    return nodeList;
  }

  /**
   * 校验命令行参数，检查是否存在当前命令不支持的非法参数
   * @param commandName 当前命令名称
   * @param cmd 解析后的命令行参数
   */
  protected void verifyCommandOptions(String commandName, CommandLine cmd) {
    @SuppressWarnings("unchecked")
    Iterator<Option> iter = cmd.iterator();
    while (iter.hasNext()) {
      Option opt = iter.next();
      // 检查每个参数是否在当前命令的合法参数列表中
      if (!validArgs.containsKey(opt.getLongOpt())) {
        // 拼接错误信息和合法参数提示
        String errMessage = String
            .format("%nInvalid argument found for command %s : %s%n",
                commandName, opt.getLongOpt());
        StringBuilder validArguments = new StringBuilder();
        validArguments.append(String.format("Valid arguments are : %n"));
        for (Map.Entry<String, String> args : validArgs.entrySet()) {
          String key = args.getKey();
          String desc = args.getValue();
          String s = String.format("\t %s : %s %n", key, desc);
          validArguments.append(s);
        }
        LOG.error(errMessage + validArguments.toString());
        throw new IllegalArgumentException("Invalid Arguments found.");
      }
    }
  }

  /**
   * 获取当前操作集群的URI地址
   * @return 集群URI
   */
  public URI getClusterURI() {
    return clusterURI;
  }

  /**
   * 设置当前操作集群的URI地址
   * @param clusterURI 集群URI
   */
  public void setClusterURI(URI clusterURI) {
    this.clusterURI = clusterURI;
  }

  /**
   * 创建到指定数据节点的RPC代理连接，用于调用数据节点接口
   * @param datanode 数据节点地址，格式为ip:port
   * @return 数据节点协议代理对象
   * @throws IOException 创建连接异常
   */
  public ClientDatanodeProtocol getDataNodeProxy(String datanode)
      throws IOException {
    InetSocketAddress datanodeAddr = NetUtils.createSocketAddr(datanode);

    // 设置数据节点的安全认证用户名，使用数据节点自己的Kerberos主体
    getConf().set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY,
        getConf().get(DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, ""));

    // 创建RPC代理客户端
    ClientDatanodeProtocol dnProtocol =
        DFSUtilClient.createClientDatanodeProtocolProxy(datanodeAddr, getUGI(),
            getConf(), NetUtils.getSocketFactory(getConf(),
                ClientDatanodeProtocol
                    .class));
    return dnProtocol;
  }

  /**
   * 获取当前调用用户的用户组信息
   * @return 当前用户UGI对象
   * @throws IOException 获取UGI异常
   */
  private static UserGroupInformation getUGI()
      throws IOException {
    return UserGroupInformation.getCurrentUser();
  }

  /**
   * 在当前命令输出目录创建指定文件，并返回输出流
   * @param fileName 文件名
   * @return 文件输出流
   * @throws IOException 文件创建异常
   */
  protected FSDataOutputStream create(String fileName) throws IOException {
    Preconditions.checkNotNull(fileName);
    if(fs == null) {
      fs = FileSystem.get(getConf());
    }
    return fs.create(new Path(this.diskBalancerLogs, fileName));
  }

  /**
   * 打开指定文件，返回输入流用于读取
   * @param fileName 要打开的文件路径
   * @return 文件输入流
   * @throws IOException 文件打开异常
   */
  protected FSDataInputStream open(String fileName) throws IOException {
    Preconditions.checkNotNull(fileName);
    if(fs == null