// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.aliasmap;

import org.apache.hadoop.thirdparty.protobuf.BlockingService;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;
import org.apache.hadoop.hdfs.protocolPB.AliasMapProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.AliasMapProtocolServerSideTranslatorPB;
import org.apache.hadoop.ipc.RPC;
import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_BIND_HOST;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_SERVER_LOG;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_SERVER_LOG_DEFAULT;
import static org.apache.hadoop.hdfs.DFSUtil.getBindAddress;
import static org.apache.hadoop.hdfs.protocol.proto.AliasMapProtocolProtos.*;
import static org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMap.CheckedFunction2;

/**
 * 别名映射服务端实现，是NameNode访问InMemoryAliasMap的入口点
 * 负责提供别名映射的RPC服务，接收NameNode查询，将数据块映射到外部存储位置
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class InMemoryLevelDBAliasMapServer implements InMemoryAliasMapProtocol,
    Configurable, Closeable {

  private static final Logger LOG = LoggerFactory
      .getLogger(InMemoryLevelDBAliasMapServer.class);
  // 别名映射初始化函数，用于创建InMemoryAliasMap实例
  private final CheckedFunction2<Configuration, String, InMemoryAliasMap>
      initFun;
  // RPC服务端实例
  private RPC.Server aliasMapServer;
  // Hadoop配置对象
  private Configuration conf;
  // 内存别名字映射实例
  private InMemoryAliasMap aliasMap;
  // 当前服务所属的块池ID
  private String blockPoolId;

  /**
   * 构造别名映射RPC服务端
   * @param initFun 别名映射初始化函数，接收配置和块池ID，返回InMemoryAliasMap实例
   * @param blockPoolId 当前服务所属的块池ID
   */
  public InMemoryLevelDBAliasMapServer(
          CheckedFunction2<Configuration, String, InMemoryAliasMap> initFun,
      String blockPoolId) {
    this.initFun = initFun;
    this.blockPoolId = blockPoolId;
  }

  /**
   * 启动别名映射RPC服务，完成服务初始化并开始监听请求
   * @throws IOException 启动过程中发生IO异常时抛出
   */
  public void start() throws IOException {
    // 设置协议引擎为ProtobufRpcEngine2
    RPC.setProtocolEngine(getConf(), AliasMapProtocolPB.class,
        ProtobufRpcEngine2.class);
    // 创建协议PB转换器，处理Protobuf消息编解码
    AliasMapProtocolServerSideTranslatorPB aliasMapProtocolXlator =
        new AliasMapProtocolServerSideTranslatorPB(this);

    // 创建阻塞式PB服务实例
    BlockingService aliasMapProtocolService =
        AliasMapProtocolService
            .newReflectiveBlockingService(aliasMapProtocolXlator);

    // 从配置获取RPC服务绑定地址
    InetSocketAddress rpcAddress = getBindAddress(conf,
        DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS,
        DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS_DEFAULT,
        DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_BIND_HOST);

    // 从配置读取日志详细程度设置
    boolean setVerbose = conf.getBoolean(
        DFS_PROVIDED_ALIASMAP_INMEMORY_SERVER_LOG,
        DFS_PROVIDED_ALIASMAP_INMEMORY_SERVER_LOG_DEFAULT);

    // 构建RPC服务端
    aliasMapServer = new RPC.Builder(conf)
        .setProtocol(AliasMapProtocolPB.class)
        .setInstance(aliasMapProtocolService)
        .setBindAddress(rpcAddress.getHostName())
        .setPort(rpcAddress.getPort())
        .setNumHandlers(1)
        .setVerbose(setVerbose)
        .build();

    LOG.info("Starting InMemoryLevelDBAliasMapServer on {}", rpcAddress);
    // 启动RPC服务开始监听请求
    aliasMapServer.start();
  }

  @Override
  public InMemoryAliasMap.IterationResult list(Optional<Block> marker)
      throws IOException {
    return aliasMap.list(marker);
  }

  @Nonnull
  @Override
  public Optional<ProvidedStorageLocation> read(@Nonnull Block block)
      throws IOException {
    return aliasMap.read(block);
  }

  @Override
  public void write(@Nonnull Block block,
      @Nonnull ProvidedStorageLocation providedStorageLocation)
      throws IOException {
    aliasMap.write(block, providedStorageLocation);
  }

  @Override
  public String getBlockPoolId() {
    return blockPoolId;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    try {
      // 使用初始化函数创建别名字映射实例
      this.aliasMap = initFun.apply(conf, blockPoolId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * 获取当前服务使用的内存别名字映射实例
   * @return 当前服务的InMemoryAliasMap实例
   */
  public InMemoryAliasMap getAliasMap() {
    return aliasMap;
  }

  @Override
  public void close() {
    LOG.info("Stopping InMemoryLevelDBAliasMapServer");
    try {
      if (aliasMap != null) {
        // 关闭别名映射实例
        aliasMap.close();
      }
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
    if (aliasMapServer != null) {
      // 停止RPC服务
      aliasMapServer.stop();
    }
  }

}