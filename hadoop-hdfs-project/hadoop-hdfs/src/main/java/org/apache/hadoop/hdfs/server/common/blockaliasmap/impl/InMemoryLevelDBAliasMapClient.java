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
package org.apache.hadoop.hdfs.server.common.blockaliasmap.impl;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;
import org.apache.hadoop.hdfs.protocolPB.InMemoryAliasMapProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMap;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMapProtocol;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.ipc.RPC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * 文件路径: hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/common/blockaliasmap/impl/InMemoryLevelDBAliasMapClient.java
 * <p>
 * 内存级别DB块别名映射服务的RPC客户端，用于对接远程InMemoryAliasMap服务端，
 * 支持DataNode和fs2img根据数据节点和fsimg根据块ID查询和存储外部提供存储的文件区域信息，
 * 实现HDFS外部提供存储（Provided Storage）的块位置映射功能。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class InMemoryLevelDBAliasMapClient extends BlockAliasMap<FileRegion>
    implements Configurable {

  private static final Logger LOG =
      LoggerFactory.getLogger(InMemoryLevelDBAliasMapClient.class);
  private Configuration conf;
  private Collection<InMemoryAliasMapProtocol> aliasMaps;

  /**
   * 关闭客户端，停止所有RPC代理连接释放资源。
   */
  @Override
  public void close() {
    if (aliasMaps != null) {
      for (InMemoryAliasMapProtocol aliasMap : aliasMaps) {
        RPC.stopProxy(aliasMap);
      }
    }
  }

  /**
   * 块别名映射读取器实现，对接远程内存别名映射服务，实现文件区域查询能力。
   */
  class LevelDbReader extends BlockAliasMap.Reader<FileRegion> {

    private InMemoryAliasMapProtocol aliasMap;

    LevelDbReader(InMemoryAliasMapProtocol aliasMap) {
      this.aliasMap = aliasMap;
    }

    /**
     * 根据块查询对应的文件区域信息。
     * @param block 目标数据块
     * @return 包含文件区域信息的Optional，不存在则返回空
     * @throws IOException RPC调用异常
     */
    @Override
    public Optional<FileRegion> resolve(Block block) throws IOException {
      Optional<ProvidedStorageLocation> read = aliasMap.read(block);
      return read.map(psl -> new FileRegion(block, psl));
    }

    @Override
    public void close() throws IOException {
    }

    /**
     * 分页迭代器实现，分批从服务端获取文件区域列表，支持遍历所有别名映射。
     */
    private class LevelDbIterator
        extends BlockAliasMap<FileRegion>.ImmutableIterator {

      private Iterator<FileRegion> iterator;
      private Optional<Block> nextMarker;

      LevelDbIterator()  {
        batch(Optional.empty());
      }

      /**
       * 从服务端批量拉取下一批文件区域数据。
       * @param newNextMarker 本次查询起始标记，为空表示从开头查询
       */
      private void batch(Optional<Block> newNextMarker) {
        try {
          InMemoryAliasMap.IterationResult iterationResult =
              aliasMap.list(newNextMarker);
          List<FileRegion> fileRegions = iterationResult.getFileRegions();
          this.iterator = fileRegions.iterator();
          this.nextMarker = iterationResult.getNextBlock();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public boolean hasNext() {
        // 当前批次还有元素，或者还有下一批次需要拉取
        return iterator.hasNext() || nextMarker.isPresent();
      }

      @Override
      public FileRegion next() {
        if (iterator.hasNext()) {
          return iterator.next();
        } else {
          if (nextMarker.isPresent()) {
            // 当前批次已遍历完，拉取下一批再继续返回
            batch(nextMarker);
            return next();
          } else {
            throw new NoSuchElementException();
          }
        }
      }
    }

    /**
     * 获取所有文件区域的迭代器。
     * @return 支持分页迭代器实例
     */
    @Override
    public Iterator<FileRegion> iterator() {
      return new LevelDbIterator();
    }
  }

  /**
   * 块别名映射写入器实现，对接远程内存别名映射服务，实现文件区域存储能力。
   */
  static class LevelDbWriter extends BlockAliasMap.Writer<FileRegion> {

    private InMemoryAliasMapProtocol aliasMap;

    LevelDbWriter(InMemoryAliasMapProtocol aliasMap) {
      this.aliasMap = aliasMap;
    }

    /**
     * 将块与对应文件区域存储到别名映射服务。
     * @param fileRegion 要存储的文件区域信息
     * @throws IOException RPC调用异常
     */
    @Override
    public void store(FileRegion fileRegion) throws IOException {
      aliasMap.write(fileRegion.getBlock(),
          fileRegion.getProvidedStorageLocation());
    }

    @Override
    public void close() throws IOException {
    }
  }

  /**
   * 初始化客户端，初始化别名映射协议客户端集合。
   */
  InMemoryLevelDBAliasMapClient() {
    aliasMaps = new ArrayList<>();
  }

  /**
   * 根据块池ID获取对应块池的别名映射协议代理。
   * @param blockPoolID 目标块池ID
   * @return 对应块池的别名映射协议代理
   * @throws IOException 未找到对应块池的别名映射或参数错误
   */
  private InMemoryAliasMapProtocol getAliasMap(String blockPoolID)
      throws IOException {
    if (blockPoolID == null) {
      throw new IOException("Block pool id required to get aliasmap reader");
    }
    // 遍历所有别名映射集合，找到匹配块池ID匹配的实例
    for (InMemoryAliasMapProtocol aliasMap : aliasMaps) {
      try {
        String aliasMapBlockPoolId = aliasMap.getBlockPoolId();
        if (aliasMapBlockPoolId != null &&
            aliasMapBlockPoolId.equals(blockPoolID)) {
          return aliasMap;
        }
      } catch (IOException e) {
        LOG.error("Exception in retrieving block pool id {}", e);
      }
    }
    throw new IOException(
        "Unable to retrieve InMemoryAliasMap for block pool id " + blockPoolID);
  }

  /**
   * 获取指定块池的块别名映射读取器。
   * @param opts 读取器配置选项
   * @param blockPoolID 目标块池ID
   * @return 块别名映射读取器实例
   * @throws IOException 获取读取器失败
   */
  @Override
  public Reader<FileRegion> getReader(Reader.Options opts, String blockPoolID)
      throws IOException {
    InMemoryAliasMapProtocol aliasMap = getAliasMap(blockPoolID);
    LOG.info("Loading InMemoryAliasMapReader for block pool id {}",
        blockPoolID);
    return new LevelDbReader(aliasMap);
  }

  /**
   * 获取指定块池的块别名映射写入器。
   * @param opts 写入器配置选项
   * @param blockPoolID 目标块池ID
   * @return 块别名映射写入器实例
   * @throws IOException 获取写入器失败
   */
  @Override
  public Writer<FileRegion> getWriter(Writer.Options opts, String blockPoolID)
      throws IOException {
    InMemoryAliasMapProtocol aliasMap = getAliasMap(blockPoolID);
    LOG.info("Loading InMemoryAliasMapWriter for block pool id {}",
        blockPoolID);
    return new LevelDbWriter(aliasMap);
  }

  /**
   * 设置配置并初始化所有别名映射RPC代理。
   * @param conf Hadoop配置对象
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    aliasMaps = InMemoryAliasMapProtocolClientSideTranslatorPB.init(conf);
  }

  /**
   * 获取当前客户端配置。
   * @return 当前Hadoop配置对象
   */
  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * 刷新别名映射，当前客户端不需要刷新操作。
   * @throws IOException 刷新异常
   */
  @Override
  public void refresh() throws IOException {
  }
}