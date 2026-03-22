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
package org.apache.hadoop.hdfs.protocolPB;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMap;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMapProtocol;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.namenode.ha.AbstractNNFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.namenode.ha.InMemoryAliasMapFailoverProxyProvider;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS;
import static org.apache.hadoop.hdfs.DFSUtil.addKeySuffixes;
import static org.apache.hadoop.hdfs.DFSUtil.createUri;
import static org.apache.hadoop.hdfs.DFSUtilClient.getNameServiceIds;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX;
import static org.apache.hadoop.hdfs.protocol.proto.AliasMapProtocolProtos.*;
import static org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.*;
import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.ipc;

/**
 * InMemory别名映射协议客户端侧Protobuf转换器，将面向业务的InMemoryAliasMapProtocol请求
 * 转换为PB格式RPC请求，调用服务端实现，同时将PB格式响应转换回业务对象。
 * 用于HDFS提供存储块位置别名映射的客户端RPC协议转换。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class InMemoryAliasMapProtocolClientSideTranslatorPB
    implements InMemoryAliasMapProtocol, Closeable {

  private static final Logger LOG =
      LoggerFactory
          .getLogger(InMemoryAliasMapProtocolClientSideTranslatorPB.class);

  // RPC代理对象，实际调用服务端PB接口
  private AliasMapProtocolPB rpcProxy;

  /**
   * 构造函数，使用已创建的RPC代理初始化转换器
   * @param rpcProxy 已初始化的AliasMapProtocolPB RPC代理
   */
  public InMemoryAliasMapProtocolClientSideTranslatorPB(
      AliasMapProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  /**
   * 根据配置初始化所有已配置的InMemory别名映射协议客户端连接
   * 遍历所有配置的命名服务，同时支持独立配置的别名映射服务，返回所有可用客户端连接集合
   * @param conf Hadoop配置对象
   * @return 所有成功连接的InMemoryAliasMapProtocol客户端集合
   */
  public static Collection<InMemoryAliasMapProtocol> init(Configuration conf) {
    Collection<InMemoryAliasMapProtocol> aliasMaps = new ArrayList<>();
    // 遍历所有配置的命名服务，尝试连接每个命名服务下的别名映射服务
    for (String nsId : getNameServiceIds(conf)) {
      try {
        URI namenodeURI = null;
        Configuration newConf = new Configuration(conf);
        if (HAUtil.isHAEnabled(conf, nsId)) {
          // HA模式下设置专属的故障转移代理提供者
          newConf.setClass(
              addKeySuffixes(PROXY_PROVIDER_KEY_PREFIX, nsId),
              InMemoryAliasMapFailoverProxyProvider.class,
              AbstractNNFailoverProxyProvider.class);
          namenodeURI = new URI(HdfsConstants.HDFS_URI_SCHEME + "://" + nsId);
        } else {
          // 非HA模式下从配置获取RPC地址构造URI
          String key =
              addKeySuffixes(DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS, nsId);
          String addr = conf.get(key);
          if (addr != null) {
            namenodeURI = createUri(HdfsConstants.HDFS_URI_SCHEME,
                NetUtils.createSocketAddr(addr));
          }
        }
        // 如果成功获取URI，创建代理并添加到结果集合
        if (namenodeURI != null) {
          aliasMaps.add(NameNodeProxies
              .createProxy(newConf, namenodeURI, InMemoryAliasMapProtocol.class)
              .getProxy());
          LOG.info("Connected to InMemoryAliasMap at {}", namenodeURI);
        }
      } catch (IOException | URISyntaxException e) {
        LOG.warn("Exception in connecting to InMemoryAliasMap for nameservice "
            + "{}: {}", nsId, e);
      }
    }
    // 检查是否配置了独立的全局别名映射RPC地址，尝试单独连接
    if (conf.get(DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS) != null) {
      URI uri = createUri("hdfs", NetUtils.createSocketAddr(
          conf.get(DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS)));
      try {
        aliasMaps.add(NameNodeProxies
            .createProxy(conf, uri, InMemoryAliasMapProtocol.class).getProxy());
        LOG.info("Connected to InMemoryAliasMap at {}", uri);
      } catch (IOException e) {
        LOG.warn("Exception in connecting to InMemoryAliasMap at {}: {}", uri,
            e);
      }
    }
    return aliasMaps;
  }

  /**
   * 分页列举别名映射中的文件区域信息
   * @param marker 分页标记，当前页的起始块，空表示从头开始列举
   * @return 列举结果，包含当前页文件区域列表和下一页起始标记
   * @throws IOException RPC调用异常
   */
  @Override
  public InMemoryAliasMap.IterationResult list(Optional<Block> marker)
      throws IOException {
    ListRequestProto.Builder builder = ListRequestProto.newBuilder();
    // 如果有分页标记，转换为PB格式放入请求
    if (marker.isPresent()) {
      builder.setMarker(PBHelperClient.convert(marker.get()));
    }
    ListRequestProto request = builder.build();
    // 发起RPC调用获取响应
    ListResponseProto response = ipc(() -> rpcProxy.list(null, request));
    List<KeyValueProto> fileRegionsList = response.getFileRegionsList();

    // 将PB格式的键值对转换为业务层FileRegion对象
    List<FileRegion> fileRegions = fileRegionsList
        .stream()
        .map(kv -> new FileRegion(
            PBHelperClient.convert(kv.getKey()),
            PBHelperClient.convert(kv.getValue())
        ))
        .collect(Collectors.toList());
    BlockProto nextMarker = response.getNextMarker();

    // 处理下一页标记，转换为业务对象返回
    if (nextMarker.isInitialized()) {
      return new InMemoryAliasMap.IterationResult(fileRegions,
          Optional.of(PBHelperClient.convert(nextMarker)));
    } else {
      return new InMemoryAliasMap.IterationResult(fileRegions,
          Optional.empty());
    }
  }

  /**
   * 根据数据块查询对应的提供存储位置
   * @param block 要查询的数据块，不能为空
   * @return 数据块对应的提供存储位置，不存在则返回Optional.empty()
   * @throws IOException 参数错误或RPC调用异常
   */
  @Nonnull
  @Override
  public Optional<ProvidedStorageLocation> read(@Nonnull Block block)
      throws IOException {

    // 参数校验
    if (block == null) {
      throw new IOException("Block cannot be null");
    }
    // 构造PB读请求
    ReadRequestProto request =
        ReadRequestProto
            .newBuilder()
            .setKey(PBHelperClient.convert(block))
            .build();
    // 发起RPC调用
    ReadResponseProto response = ipc(() -> rpcProxy.read(null, request));

    ProvidedStorageLocationProto providedStorageLocation =
        response.getValue();
    // 转换PB响应为业务对象返回
    if (providedStorageLocation.isInitialized()) {
      return Optional.of(PBHelperClient.convert(providedStorageLocation));
    }
    return Optional.empty();

  }

  /**
   * 写入数据块到提供存储位置的别名映射关系
   * @param block 数据块对象，不能为空
   * @param providedStorageLocation 数据块对应的提供存储位置，不能为空
   * @throws IOException 参数错误或RPC调用异常
   */
  @Override
  public void write(@Nonnull Block block,
      @Nonnull ProvidedStorageLocation providedStorageLocation)
      throws IOException {
    // 参数校验
    if (block == null || providedStorageLocation == null) {
      throw new IOException("Provided block and location cannot be null");
    }
    // 构造PB写请求
    WriteRequestProto request =
        WriteRequestProto
            .newBuilder()
            .setKeyValuePair(KeyValueProto.newBuilder()
                .setKey(PBHelperClient.convert(block))
                .setValue(PBHelperClient.convert(providedStorageLocation))
                .build())
            .build();

    // 发起RPC调用
    ipc(() -> rpcProxy.write(null, request));
  }

  /**
   * 获取当前别名映射服务对应的块池ID
   * @return 块池ID字符串
   * @throws IOException RPC调用异常
   */
  @Override
  public String getBlockPoolId() throws IOException {
    BlockPoolResponseProto response = ipc(() -> rpcProxy.getBlockPoolId(null,
        BlockPoolRequestProto.newBuilder().build()));
    return response.getBlockPoolId();
  }

  /**
   * 关闭RPC代理，释放资源
   * @throws IOException 关闭操作异常
   */
  @Override
  public void close() throws IOException {
    LOG.info("Stopping rpcProxy in" +
        "InMemoryAliasMapProtocolClientSideTranslatorPB");
    if (rpcProxy != null) {
      RPC.stopProxy(rpcProxy);
    }
  }
}