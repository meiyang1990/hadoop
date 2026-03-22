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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.tools.GetGroupsBase;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.util.ToolRunner;

/**
 * 文件概要：HDFS端获取用户所属用户组信息的命令行工具
 * 实现了Hadoop通用获取用户分组查询功能，通过NameNode服务查询指定用户所属的用户组列表
 */
@InterfaceAudience.Private
public class GetGroups extends GetGroupsBase {
  
  private static final Logger LOG = LoggerFactory.getLogger(GetGroups.class);
  
  static final String USAGE = "Usage: hdfs groups [username ...]";

  static{
    // 初始化HDFS配置
    HdfsConfiguration.init();
  }

  
  /**
   * 构造函数，基于指定配置创建GetGroups工具实例
   * @param conf Hadoop配置对象
   */
  public GetGroups(Configuration conf) {
    super(conf);
  }

  /**
   * 构造函数，基于指定配置和输出流创建GetGroups工具实例
   * @param conf Hadoop配置对象
   * @param out 结果输出流
   */
  public GetGroups(Configuration conf, PrintStream out) {
    super(conf, out);
  }
  
  @Override
  /**
   * 获取NameNode服务地址，用于连接用户分组信息服务
   * @param conf Hadoop配置对象
   * @return NameNode的地址
   * @throws IOException 获取地址失败时抛出异常
   */
  protected InetSocketAddress getProtocolAddress(Configuration conf)
      throws IOException {
    return DFSUtilClient.getNNAddress(conf);
  }
  
  @Override
  /**
   * 配置初始化，加载HDFS配置并设置安全相关参数
   * @param conf 原始配置对象
   */
  public void setConf(Configuration conf) {
    // 基于原始配置创建HDFS专属配置对象
    conf = new HdfsConfiguration(conf);
    // 从配置中读取NameNode的Kerberos主体名称
    String nameNodePrincipal = conf.get(
        DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, "");
    
    if (LOG.isDebugEnabled()) {
      // 调试日志输出当前使用的NameNode主体名称
      LOG.debug("Using NN principal: " + nameNodePrincipal);
    }

    // 将NameNode主体名称设置为安全服务用户名，用于认证
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY,
        nameNodePrincipal);
    
    super.setConf(conf);
  }
  
  @Override
  /**
   * 获取用户分组信息协议代理，用于和NameNode通信查询用户分组
   * @return 用户分组信息协议代理对象
   * @throws IOException 创建代理失败时抛出异常
   */
  protected GetUserMappingsProtocol getUgmProtocol() throws IOException {
    return NameNodeProxies.createProxy(getConf(), FileSystem.getDefaultUri(getConf()),
        GetUserMappingsProtocol.class).getProxy();
  }

  /**
   * 工具主入口，解析参数并执行获取用户分组查询
   * @param argv 命令行参数（用户名列表
   * @throws Exception 执行过程中发生的异常
   */
  public static void main(String[] argv) throws Exception {
    // 解析-help参数，输出帮助信息并退出
    if (DFSUtil.parseHelpArgument(argv, USAGE, System.out, true)) {
      System.exit(0);
    }
    
    // 运行GetGroups工具，获取执行结果
    int res = ToolRunner.run(new GetGroups(new HdfsConfiguration()), argv);
    System.exit(res);
  }
}