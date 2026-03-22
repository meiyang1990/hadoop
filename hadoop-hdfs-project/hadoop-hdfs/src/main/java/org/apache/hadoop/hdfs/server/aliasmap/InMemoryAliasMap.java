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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ProvidedStorageLocationProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.namenode.ImageServlet;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Lists;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;

/**
 * 文件说明：基于LevelDB实现的内存别名映射，用于Provided存储场景中存储块到存储位置的映射关系，支持备NameNode启动时的元数据同步
 * 该类是InMemoryAliasMapProtocol接口的LevelDB实现，维护提供存储块与对应存储位置的索引映射，支持读写和快照传输能力
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class InMemoryAliasMap implements InMemoryAliasMapProtocol,
    Configurable {

  private static final Logger LOG = LoggerFactory
      .getLogger(InMemoryAliasMap.class);

  private static final String SNAPSHOT_COPY_DIR = "aliasmap_snapshot";
  private static final String TAR_NAME = "aliasmap.tar.gz";
  private final URI aliasMapURI;
  private final DB levelDb;
  private Configuration conf;
  private String blockPoolID;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * 初始化基于LevelDB的别名映射实例，根据配置创建或打开LevelDB存储
   * @param conf Hadoop配置对象
   * @param blockPoolID 块池ID，对应NameNode块池隔离
   * @return 初始化完成的InMemoryAliasMap实例
   * @throws IOException 初始化过程中IO异常，如目录创建失败、LevelDB打开失败
   */
  public static @Nonnull InMemoryAliasMap init(Configuration conf,
      String blockPoolID) throws IOException {
    Options options = new Options();
    options.createIfMissing(true);
    String directory =
        conf.get(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_LEVELDB_DIR);
    if (directory == null) {
      throw new IOException("InMemoryAliasMap location is null");
    }
    File levelDBpath;
    if (blockPoolID != null) {
      levelDBpath = new File(directory, blockPoolID);
    } else {
      levelDBpath = new File(directory);
    }
    if (!levelDBpath.exists()) {
      LOG.warn("InMemoryAliasMap location {} is missing. Creating it.",
          levelDBpath);
      if(!levelDBpath.mkdirs()) {
        throw new IOException(
            "Unable to create missing aliasmap location: " + levelDBpath);
      }
    }
    DB levelDb = JniDBFactory.factory.open(levelDBpath, options);
    InMemoryAliasMap aliasMap =  new InMemoryAliasMap(levelDBpath.toURI(),
        levelDb, blockPoolID);
    aliasMap.setConf(conf);
    return aliasMap;
  }

  @VisibleForTesting
  InMemoryAliasMap(URI aliasMapURI, DB levelDb, String blockPoolID) {
    this.aliasMapURI = aliasMapURI;
    this.levelDb = levelDb;
    this.blockPoolID = blockPoolID;
  }

  @Override
  /**
   * 分页批量列出别名映射中的所有块位置信息，用于遍历同步
   * @param marker 分页起始标记，为空表示从第一条开始
   * @return 分页结果，包含当前批次条目和下一页标记
   * @throws IOException LevelDB遍历过程中IO异常
   */
  public IterationResult list(Optional<Block> marker) throws IOException {
    try (DBIterator iterator = levelDb.iterator()) {
      Integer batchSize =
          conf.getInt(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_BATCH_SIZE,
              DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_BATCH_SIZE_DEFAULT);
      if (marker.isPresent()) {
        iterator.seek(toProtoBufBytes(marker.get()));
      } else {
        iterator.seekToFirst();
      }
      int i = 0;
      ArrayList<FileRegion> batch =
          Lists.newArrayListWithExpectedSize(batchSize);
      while (iterator.hasNext() && i < batchSize) {
        Map.Entry<byte[], byte[]> entry = iterator.next();
        Block block = fromBlockBytes(entry.getKey());
        ProvidedStorageLocation providedStorageLocation =
            fromProvidedStorageLocationBytes(entry.getValue());
        batch.add(new FileRegion(block, providedStorageLocation));
        ++i;
      }
      if (iterator.hasNext()) {
        Block nextMarker = fromBlockBytes(iterator.next().getKey());
        return new IterationResult(batch, Optional.of(nextMarker));
      } else {
        return new IterationResult(batch, Optional.empty());
      }
    }
  }

  /**
   * 根据块查询对应的提供存储位置
   * @param block 需要查询的块对象
   * @return 存在则返回存储位置Optional，不存在返回空Optional
   * @throws IOException LevelDB查询反序列化过程中IO异常
   */
  public @Nonnull Optional<ProvidedStorageLocation> read(@Nonnull Block block)
      throws IOException {

    byte[] extendedBlockDbFormat = toProtoBufBytes(block);
    byte[] providedStorageLocationDbFormat = levelDb.get(extendedBlockDbFormat);
    if (providedStorageLocationDbFormat == null) {
      return Optional.empty();
    } else {
      ProvidedStorageLocation providedStorageLocation =
          fromProvidedStorageLocationBytes(providedStorageLocationDbFormat);
      return Optional.of(providedStorageLocation);
    }
  }

  /**
   * 写入块到提供存储位置的映射关系到LevelDB
   * @param block 块对象
   * @param providedStorageLocation 对应的提供存储位置
   * @throws IOException 序列化或LevelDB写入过程中IO异常
   */
  public void write(@Nonnull Block block,
      @Nonnull ProvidedStorageLocation providedStorageLocation)
      throws IOException {
    byte[] extendedBlockDbFormat = toProtoBufBytes(block);
    byte[] providedStorageLocationDbFormat =
        toProtoBufBytes(providedStorageLocation);
    levelDb.put(extendedBlockDbFormat, providedStorageLocationDbFormat);
  }

  @Override
  public String getBlockPoolId() {
    return blockPoolID;
  }

  /**
   * 关闭LevelDB存储，释放资源
   * @throws IOException LevelDB关闭过程中IO异常
   */
  public void close() throws IOException {
    levelDb.close();
  }

  /**
   * 将LevelDB存储的字节反序列化为ProvidedStorageLocation对象
   * @param providedStorageLocationDbFormat LevelDB存储的字节数组
   * @return 反序列化后的ProvidedStorageLocation对象
   * @throws InvalidProtocolBufferException Protobuf解析错误
   */
  @Nonnull
  public static ProvidedStorageLocation fromProvidedStorageLocationBytes(
      @Nonnull byte[] providedStorageLocationDbFormat)
      throws InvalidProtocolBufferException {
    ProvidedStorageLocationProto providedStorageLocationProto =
        ProvidedStorageLocationProto
            .parseFrom(providedStorageLocationDbFormat);
    return PBHelperClient.convert(providedStorageLocationProto);
  }

  /**
   * 将LevelDB存储的字节反序列化为Block对象
   * @param blockDbFormat LevelDB存储的字节数组
   * @return 反序列化后的Block对象
   * @throws InvalidProtocolBufferException Protobuf解析错误
   */
  @Nonnull
  public static Block fromBlockBytes(@Nonnull byte[] blockDbFormat)
      throws InvalidProtocolBufferException {
    BlockProto blockProto = BlockProto.parseFrom(blockDbFormat);
    return PBHelperClient.convert(blockProto);
  }

  /**
   * 将ProvidedStorageLocation对象序列化为Protobuf字节数组用于存储
   * @param providedStorageLocation 需要序列化的对象
   * @return 序列化后的字节数组
   * @throws IOException 序列化过程中IO异常
   */
  public static byte[] toProtoBufBytes(@Nonnull ProvidedStorageLocation
      providedStorageLocation) throws IOException {
    ProvidedStorageLocationProto providedStorageLocationProto =
        PBHelperClient.convert(providedStorageLocation);
    ByteArrayOutputStream providedStorageLocationOutputStream =
        new ByteArrayOutputStream();
    providedStorageLocationProto.writeTo(providedStorageLocationOutputStream);
    return providedStorageLocationOutputStream.toByteArray();
  }

  /**
   * 将Block对象序列化为Protobuf字节数组用于存储
   * @param block 需要序列化的块对象
   * @return 序列化后的字节数组
   * @throws IOException 序列化过程中IO异常
   */
  public static byte[] toProtoBufBytes(@Nonnull Block block)
      throws IOException {
    BlockProto blockProto =
        PBHelperClient.convert(block);
    ByteArrayOutputStream blockOutputStream = new ByteArrayOutputStream();
    blockProto.writeTo(blockOutputStream);
    return blockOutputStream.toByteArray();
  }

  /**
   * 将别名映射打包传输给备NameNode完成启动引导，以tar.gz压缩包形式输出到HTTP响应
   * 用于备NameNode同步主NameNode的别名映射元数据
   * @param response HTTP响应对象，用于输出压缩包
   * @param conf Hadoop配置对象
   * @param aliasMap 需要传输的别名映射实例
   * @throws IOException 快照创建、压缩、传输过程中IO异常
   */
  public static void transferForBootstrap(HttpServletResponse response,
      Configuration conf, InMemoryAliasMap aliasMap) throws IOException {
    File aliasMapSnapshot = null;
    File compressedAliasMap = null;
    try {
      aliasMapSnapshot = createSnapshot(aliasMap);
      // 对对应块池的快照目录进行压缩
      compressedAliasMap = getCompressedAliasMap(
          new File(aliasMapSnapshot, aliasMap.blockPoolID));
      try (FileInputStream fis = new FileInputStream(compressedAliasMap)) {
        ImageServlet.setVerificationHeadersForGet(response, compressedAliasMap);
        ImageServlet.setFileNameHeaders(response, compressedAliasMap);
        // 获取传输限速器
        DataTransferThrottler throttler =
            ImageServlet.getThrottlerForBootstrapStandby(conf);
        TransferFsImage.copyFileToStream(response.getOutputStream(),
            compressedAliasMap, fis, throttler);
      }
    } finally {
      // 清理临时快照和压缩文件
      StringBuilder errMessage = new StringBuilder();
      if (compressedAliasMap != null
          && !FileUtil.fullyDelete(compressedAliasMap)) {
        errMessage.append("Failed to fully delete compressed aliasmap ")
            .append(compressedAliasMap.getAbsolutePath()).append("\n");
      }
      if (aliasMapSnapshot != null && !FileUtil.fullyDelete(aliasMapSnapshot)) {
        errMessage.append("Failed to fully delete the aliasmap snapshot ")
            .append(aliasMapSnapshot.getAbsolutePath()).append("\n");
      }
      if (errMessage.length() > 0) {
        throw new IOException(errMessage.toString());
      }
    }
  }

  /**
   * 创建当前别名映射的LevelDB快照，用于传输给备NameNode
   * 使用LevelDB快照机制保证数据一致性
   * @param aliasMap 原始别名映射实例
   * @return 快照存储目录的File对象
   * @throws IOException 快照创建、数据拷贝过程中IO异常
   */
  static File createSnapshot(InMemoryAliasMap aliasMap) throws IOException {
    File originalAliasMapDir = new File(aliasMap.aliasMapURI);
    String bpid = originalAliasMapDir.getName();
    File snapshotDir =
        new File(originalAliasMapDir.getParent(), SNAPSHOT_COPY_DIR);
    File newLevelDBDir = new File(snapshotDir, bpid);
    if (!newLevelDBDir.mkdirs()) {
      throw new IOException(
          "Unable to create aliasmap snapshot directory " + newLevelDBDir);
    }
    // 获取原始DB的快照对象
    DB originalDB = aliasMap.levelDb;
    try (Snapshot snapshot = originalDB.getSnapshot()) {
      // 创建新的LevelDB实例存储快照数据
      Options options = new Options();
      options.createIfMissing(true);
      try (DB snapshotDB = JniDBFactory.factory.open(newLevelDBDir, options)) {
        try (DBIterator iterator =
            originalDB.iterator(new ReadOptions().snapshot(snapshot))) {
          iterator.seekToFirst();
          while (iterator.hasNext()) {
            Map.Entry<byte[], byte[]> entry = iterator.next();
            snapshotDB.put(entry.getKey(), entry.getValue());
          }
        }
      }
    }

    return snapshotDir;
  }

  /**
   * 将别名映射目录压缩为tar.gz格式压缩包
   * @param aliasMapDir 需要压缩的别名映射目录
   * @return 压缩完成的压缩包文件引用
   * @throws IOException 压缩过程中IO异常
   */
  private static File getCompressedAliasMap(File aliasMapDir)
      throws IOException {
    File outCompressedFile = new File(aliasMapDir.getParent(), TAR_NAME);

    try (BufferedOutputStream bOut = new BufferedOutputStream(
            Files.newOutputStream(outCompressedFile.toPath()));
         GzipCompressorOutputStream gzOut = new GzipCompressorOutputStream(bOut);
         TarArchiveOutputStream tOut = new TarArchiveOutputStream(gzOut)){

      addFileToTarGzRecursively(tOut, aliasMapDir, "", new Configuration());
    }

    return outCompressedFile;
  }

  /**
   * 递归将文件/目录添加到tar.gz压缩包中，跳过LevelDB的LOCK文件
   * @param tOut tar压缩输出流
   * @param file 当前需要添加的文件/目录
   * @param prefix 路径前缀，用于维护目录结构
   * @param conf Hadoop配置对象
   * @throws IOException 文件读取或压缩过程中IO异常
   */
  private static void addFileToTarGzRecursively(TarArchiveOutputStream tOut,
      File file, String prefix, Configuration conf) throws IOException {
    String entryName = prefix + file.getName();
    TarArchiveEntry tarEntry = new TarArchiveEntry(file, entryName);
    tOut.putArchiveEntry(tarEntry);

    LOG.debug("Adding entry {} to alias map archive", entryName);
    if (file.isFile()) {
      try (FileInputStream in = new FileInputStream(file)) {
        IOUtils.copyBytes(in, tOut, conf, false);
      }
      tOut.closeArchiveEntry();
    } else {
      tOut.closeArchiveEntry();
      File[] children = file.listFiles();
      if (children != null) {
        for (File child : children) {
          // 跳过LevelDB LOCK文件，不需要同步
          if (!child.getName().equals("LOCK")) {
            addFileToTarGzRecursively(tOut, child, entryName + "/", conf);
          }
        }
      }
    }
  }

  /**
   * 在备NameNode侧完成别名映射引导，解压传输过来的tar.gz压缩包到目标目录
   * 解压完成后删除临时压缩包
   * @param aliasMap 别名映射存储目录，压缩包已传输到此目录
   * @throws IOException 解压过程中IO异常
   */
  public static void completeBootstrapTransfer(File aliasMap)