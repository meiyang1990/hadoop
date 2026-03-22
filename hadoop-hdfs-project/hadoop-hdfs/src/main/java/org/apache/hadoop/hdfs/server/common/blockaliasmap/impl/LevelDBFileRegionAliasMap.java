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

package org.apache.hadoop.hdfs.server.common.blockaliasmap.impl;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_LEVELDB_PATH;
import static org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMap.fromBlockBytes;
import static org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMap.fromProvidedStorageLocationBytes;
import static org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMap.toProtoBufBytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件块别名映射的LevelDB持久化实现，基于LevelDB存储外部提供存储的块位置信息，
 * 用于HDFS提供存储功能，将HDFS块元数据映射到外部存储系统的实际数据位置。
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class LevelDBFileRegionAliasMap
      extends BlockAliasMap<FileRegion> implements Configurable {

  private Configuration conf;
  private LevelDBOptions opts = new LevelDBOptions();

  public static final Logger LOG =
      LoggerFactory.getLogger(LevelDBFileRegionAliasMap.class);

  @Override
  public void setConf(Configuration conf) {
    opts.setConf(conf);
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * 获取LevelDB块别名映射的读取器，用于查询块位置信息
   * @param opts 读取选项
   * @param blockPoolID 块池ID，不同块池对应不同的LevelDB目录
   * @return LevelDB读取器实例
   * @throws IOException 创建LevelDB连接失败时抛出异常
   */
  @Override
  public Reader<FileRegion> getReader(Reader.Options opts, String blockPoolID)
      throws IOException {
    if (null == opts) {
      opts = this.opts;
    }
    if (!(opts instanceof LevelDBOptions)) {
      throw new IllegalArgumentException("Invalid options " + opts.getClass());
    }
    LevelDBOptions o = (LevelDBOptions) opts;
    return new LevelDBFileRegionAliasMap.LevelDBReader(
        createDB(o.levelDBPath, false, blockPoolID));
  }

  /**
   * 获取LevelDB块别名映射的写入器，用于存储块位置信息
   * @param opts 写入选项
   * @param blockPoolID 块池ID，不同块池对应不同的LevelDB目录
   * @return LevelDB写入器实例
   * @throws IOException 创建LevelDB连接失败时抛出异常
   */
  @Override
  public Writer<FileRegion> getWriter(Writer.Options opts, String blockPoolID)
      throws IOException {
    if (null == opts) {
      opts = this.opts;
    }
    if (!(opts instanceof LevelDBOptions)) {
      throw new IllegalArgumentException("Invalid options " + opts.getClass());
    }
    LevelDBOptions o = (LevelDBOptions) opts;
    return new LevelDBFileRegionAliasMap.LevelDBWriter(
        createDB(o.levelDBPath, true, blockPoolID));
  }

  /**
   * 创建并打开LevelDB数据库实例，按块池分隔存储目录
   * @param levelDBPath LevelDB根目录路径
   * @param createIfMissing 数据库不存在时是否创建
   * @param blockPoolID 块池ID，用于隔离不同块池的数据
   * @return 打开的LevelDB实例
   * @throws IOException 路径无效、目录创建失败或打开数据库失败时抛出异常
   */
  private static DB createDB(String levelDBPath, boolean createIfMissing,
      String blockPoolID) throws IOException {
    // 检查LevelDB路径是否配置
    if (levelDBPath == null || levelDBPath.length() == 0) {
      throw new IllegalArgumentException(
          "A valid path needs to be specified for "
              + LevelDBFileRegionAliasMap.class + " using the parameter "
              + DFS_PROVIDED_ALIASMAP_LEVELDB_PATH);
    }
    org.iq80.leveldb.Options options = new org.iq80.leveldb.Options();
    // 设置是否自动创建不存在的数据库
    options.createIfMissing(createIfMissing);
    File dbFile;
    // 按块池ID组织目录结构，不同块池分开存储
    if (blockPoolID != null) {
      dbFile = new File(levelDBPath, blockPoolID);
    } else {
      dbFile = new File(levelDBPath);
    }
    // 需要创建时，如果目录不存在则创建多级目录
    if (createIfMissing && !dbFile.exists()) {
      if (!dbFile.mkdirs()) {
        throw new IOException("Unable to create " + dbFile);
      }
    }
    // 打开LevelDB数据库
    return factory.open(dbFile, options);
  }

  @Override
  public void refresh() throws IOException {
  }

  @Override
  public void close() throws IOException {
    // Do nothing.
  }

  /**
   * LevelDB块别名映射的配置选项类，同时支持读取器和写入器的配置
   * 实现Configurable接口从Hadoop配置加载参数
   */
  public static class LevelDBOptions implements LevelDBReader.Options,
      LevelDBWriter.Options, Configurable {
    private Configuration conf;
    private String levelDBPath;

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      // 从配置中读取LevelDB存储路径
      this.levelDBPath = conf.get(DFS_PROVIDED_ALIASMAP_LEVELDB_PATH);
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public LevelDBOptions filename(String levelDBPath) {
      // 手动设置LevelDB存储路径，支持链式调用
      this.levelDBPath = levelDBPath;
      return this;
    }
  }

  /**
   * LevelDB块别名映射的读取器实现，负责从LevelDB查询块对应的文件区域信息
   */
  public static class LevelDBReader extends Reader<FileRegion> {

    /**
     * LevelDB读取器选项接口，定义设置存储路径的方法
     */
    public interface Options extends Reader.Options {
      Options filename(String levelDBPath);
    }

    private DB db;

    LevelDBReader(DB db) {
      this.db = db;
    }

    @Override
    public Optional<FileRegion> resolve(Block block) throws IOException {
      if (db == null) {
        return Optional.empty();
      }
      // 将块对象序列化为Protocol Buffer字节数组作为键
      byte[] key = toProtoBufBytes(block);
      // 从LevelDB查询对应的存储位置信息
      byte[] value = db.get(key);
      // 反序列化为提供存储位置对象
      ProvidedStorageLocation psl = fromProvidedStorageLocationBytes(value);
      // 包装为FileRegion返回
      return Optional.of(new FileRegion(block, psl));
    }

    /**
     * 迭代器实现，封装LevelDB迭代器，转换为FileRegion迭代
     */
    static class FRIterator implements Iterator<FileRegion> {
      private final DBIterator internal;

      FRIterator(DBIterator internal) {
        this.internal = internal;
      }

      @Override
      public boolean hasNext() {
        return internal.hasNext();
      }

      @Override
      public FileRegion next() {
        Map.Entry<byte[], byte[]> entry = internal.next();
        if (entry == null) {
          return null;
        }
        try {
          // 反序列化键得到块对象
          Block block = fromBlockBytes(entry.getKey());
          // 反序列化值得到存储位置对象
          ProvidedStorageLocation psl =
              fromProvidedStorageLocationBytes(entry.getValue());
          // 返回包装好的FileRegion
          return new FileRegion(block, psl);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    }

    public Iterator<FileRegion> iterator() {
      if (db == null) {
        return null;
      }
      // 获取LevelDB迭代器并定位到第一条记录
      DBIterator iterator = db.iterator();
      iterator.seekToFirst();
      // 包装为自定义迭代器返回
      return new FRIterator(iterator);
    }

    @Override
    public void close() throws IOException {
      if (db != null) {
        // 关闭LevelDB连接
        db.close();
      }
    }
  }

  /**
   * LevelDB块别名映射的写入器实现，负责将块位置信息写入LevelDB
   */
  public static class LevelDBWriter extends Writer<FileRegion> {

    /**
     * LevelDB写入器选项接口，定义设置存储路径的方法
     */
    public interface Options extends Writer.Options {
      Options filename(String levelDBPath);
    }

    private final DB db;

    LevelDBWriter(DB db) {
      this.db = db;
    }

    @Override
    public void store(FileRegion token) throws IOException {
      // 序列化块对象为字节数组作为键
      byte[] key = toProtoBufBytes(token.getBlock());
      // 序列化存储位置对象为字节数组作为值
      byte[] value = toProtoBufBytes(token.getProvidedStorageLocation());
      // 写入LevelDB
      db.put(key, value);
    }

    @Override
    public void close() throws IOException {
      if (db != null) {
        // 关闭LevelDB连接
        db.close();
      }
    }
  }
}