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
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.Map;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 文件路径：hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/common/blockaliasmap/impl/TextFileRegionAliasMap.java
 * <p>
 * 基于文本文件实现的块别名映射表，用于存储外部提供存储（Provided Storage）的块位置映射关系。
 * 每个块池对应一个独立的文本文件，使用指定分隔符分隔块信息字段。
 * </p>
 * 核心职责：提供对文本格式存储的块别名映射的读写能力，支持压缩文本文件。
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class TextFileRegionAliasMap
    extends BlockAliasMap<FileRegion> implements Configurable {

  private Configuration conf;
  private ReaderOptions readerOpts = TextReader.defaults();
  private WriterOptions writerOpts = TextWriter.defaults();

  public static final Logger LOG =
      LoggerFactory.getLogger(TextFileRegionAliasMap.class);

  /**
   * 设置Hadoop配置，同时初始化读写选项。
   * @param conf Hadoop配置对象
   */
  @Override
  public void setConf(Configuration conf) {
    readerOpts.setConf(conf);
    writerOpts.setConf(conf);
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * 根据指定选项获取块别名映射读取器，用于读取指定块池的映射信息。
   * @param opts 读取选项
   * @param blockPoolID 块池ID
   * @return 块别名读取器
   * @throws IOException 创建读取器时抛出IO异常
   */
  @Override
  public Reader<FileRegion> getReader(Reader.Options opts, String blockPoolID)
      throws IOException {
    if (null == opts) {
      opts = readerOpts;
    }
    if (!(opts instanceof ReaderOptions)) {
      throw new IllegalArgumentException("Invalid options " + opts.getClass());
    }
    ReaderOptions o = (ReaderOptions) opts;
    Configuration readerConf = (null == o.getConf())
        ? new Configuration()
            : o.getConf();
    return createReader(o.file, o.delim, readerConf, blockPoolID);
  }

  @VisibleForTesting
  TextReader createReader(Path file, String delim, Configuration cfg,
      String blockPoolID) throws IOException {
    FileSystem fs = file.getFileSystem(cfg);
    if (fs instanceof LocalFileSystem) {
      fs = ((LocalFileSystem)fs).getRaw();
    }
    // 创建压缩编码工厂，检测文件是否压缩
    CompressionCodecFactory factory = new CompressionCodecFactory(cfg);
    CompressionCodec codec = factory.getCodec(file);
    // 生成对应块池的文件名
    String filename = fileNameFromBlockPoolID(blockPoolID);
    if (codec != null) {
      filename = filename + codec.getDefaultExtension();
    }
    // 拼接块池文件完整路径
    Path bpidFilePath = new Path(file.getParent(), filename);
    return new TextReader(fs, bpidFilePath, codec, delim);
  }

  /**
   * 根据指定选项获取块别名映射写入器，用于写入指定块池的映射信息。
   * @param opts 写入选项
   * @param blockPoolID 块池ID
   * @return 块别名写入器
   * @throws IOException 创建写入器时抛出IO异常
   */
  @Override
  public Writer<FileRegion> getWriter(Writer.Options opts, String blockPoolID)
      throws IOException {
    if (null == opts) {
      opts = writerOpts;
    }
    if (!(opts instanceof WriterOptions)) {
      throw new IllegalArgumentException("Invalid options " + opts.getClass());
    }
    WriterOptions o = (WriterOptions) opts;
    Configuration cfg = (null == o.getConf())
        ? new Configuration()
            : o.getConf();
    // 生成对应块池的基础文件名
    String baseName = fileNameFromBlockPoolID(blockPoolID);
    Path blocksFile = new Path(o.dir, baseName);
    // 如果配置了压缩，添加压缩扩展名
    if (o.codec != null) {
      CompressionCodecFactory factory = new CompressionCodecFactory(cfg);
      CompressionCodec codec = factory.getCodecByName(o.codec);
      blocksFile = new Path(o.dir, baseName + codec.getDefaultExtension());
      return createWriter(blocksFile, codec, o.delim, cfg);
    }
    return createWriter(blocksFile, null, o.delim, conf);
  }

  @VisibleForTesting
  TextWriter createWriter(Path file, CompressionCodec codec, String delim,
      Configuration cfg) throws IOException {
    FileSystem fs = file.getFileSystem(cfg);
    if (fs instanceof LocalFileSystem) {
      fs = ((LocalFileSystem)fs).getRaw();
    }
    // 创建文件输出流
    OutputStream tmp = fs.create(file);
    // 根据是否压缩包装输出流
    java.io.Writer out = new BufferedWriter(new OutputStreamWriter(
          (null == codec) ? tmp : codec.createOutputStream(tmp), StandardCharsets.UTF_8));
    return new TextWriter(out, delim);
  }

  /**
   * TextFileRegionAliasMap的读取选项配置类，从配置文件加载读取参数。
   */
  public static class ReaderOptions
      implements TextReader.Options, Configurable {

    private Configuration conf;
    private String delim =
        DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_DELIMITER_DEFAULT;
    private Path file = new Path(
        new File(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_READ_FILE_DEFAULT)
            .toURI().toString());

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      // 从配置中读取映射文件路径
      String tmpfile =
          conf.get(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_READ_FILE,
              DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_READ_FILE_DEFAULT);
      file = new Path(tmpfile);
      // 从配置中读取分隔符
      delim = conf.get(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_DELIMITER,
          DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_DELIMITER_DEFAULT);
      LOG.info("TextFileRegionAliasMap: read path {}", tmpfile);
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public ReaderOptions filename(Path file) {
      this.file = file;
      return this;
    }

    @Override
    public ReaderOptions delimiter(String delim) {
      this.delim = delim;
      return this;
    }
  }

  /**
   * TextFileRegionAliasMap的写入选项配置类，从配置文件加载写入参数。
   */
  public static class WriterOptions
      implements TextWriter.Options, Configurable {

    private Configuration conf;
    private String codec = null;
    private Path dir =
        new Path(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_WRITE_DIR_DEFAULT);
    private String delim =
        DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_DELIMITER_DEFAULT;

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      // 从配置中读取输出目录
      String tmpDir = conf.get(
          DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_WRITE_DIR, dir.toString());
      dir = new Path(tmpDir);
      // 从配置中读取压缩编码
      codec = conf.get(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_CODEC);
      // 从配置中读取分隔符
      delim = conf.get(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_DELIMITER,
          DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_DELIMITER_DEFAULT);
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public WriterOptions dirName(Path dir) {
      this.dir = dir;
      return this;
    }

    public String getCodec() {
      return codec;
    }

    public Path getDir() {
      return dir;
    }

    @Override
    public WriterOptions codec(String codec) {
      this.codec = codec;
      return this;
    }

    @Override
    public WriterOptions delimiter(String delim) {
      this.delim = delim;
      return this;
    }

  }

  /**
   * 文本格式块别名映射读取器，按行读取分隔符文本文件，解析为FileRegion对象。
   * 支持遍历查询和按块ID查询，支持压缩文本文件。
   */
  public static class TextReader extends Reader<FileRegion> {

    /**
     * TextReader选项接口，定义可配置参数。
     */
    public interface Options extends Reader.Options {
      Options filename(Path file);
      Options delimiter(String delim);
    }

    /**
     * 获取默认读取选项。
     * @return 默认ReaderOptions对象
     */
    public static ReaderOptions defaults() {
      return new ReaderOptions();
    }

    private final Path file;
    private final String delim;
    private final FileSystem fs;
    private final CompressionCodec codec;
    private final Map<FRIterator, BufferedReader> iterators;
    private final String blockPoolID;

    protected TextReader(FileSystem fs, Path file, CompressionCodec codec,
        String delim) {
      this(fs, file, codec, delim,
          new IdentityHashMap<FRIterator, BufferedReader>());
    }

    TextReader(FileSystem fs, Path file, CompressionCodec codec, String delim,
        Map<FRIterator, BufferedReader> iterators) {
      this.fs = fs;
      this.file = file;
      this.codec = codec;
      this.delim = delim;
      // 使用同步Map管理所有打开的迭代器，支持多线程迭代
      this.iterators = Collections.synchronizedMap(iterators);
      // 从文件名解析块池ID
      this.blockPoolID = blockPoolIDFromFileName(file);
    }

    /**
     * 根据块ID查询对应的文件区域信息。
     * @param ident 块对象
     * @return 包含FileRegion的Optional，如果未找到返回空Optional
     * @throws IOException 读取文件时抛出IO异常
     */
    @Override
    public Optional<FileRegion> resolve(Block ident) throws IOException {
      // 目前采用线性扫描查询，可后续优化为分层索引
      Iterator<FileRegion> i = iterator();
      try {
        while (i.hasNext()) {
          FileRegion f = i.next();
          if (f.getBlock().equals(ident)) {
            return Optional.of(f);
          }
        }
      } finally {
        // 迭代完成后关闭流清理资源
        BufferedReader r = iterators.remove(i);
        if (r != null) {
          r.close();
        }
      }
      return Optional.empty();
    }

    /**
     * FileRegion迭代器实现，流式读取文本文件。
     */
    class FRIterator implements Iterator<FileRegion> {

      private FileRegion pending;

      @Override
      public boolean hasNext() {
        return pending != null;
      }

      @Override
      public FileRegion next() {
        if (null == pending) {
          throw new NoSuchElementException();
        }
        FileRegion ret = pending;
        try {
          // 预读下一行
          pending = nextInternal(this);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return ret;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    }

    /**
     * 读取并解析下一行文本，转换为FileRegion对象。
     * @param i 迭代器对象
     * @return 解析后的FileRegion，读到文件末尾返回null
     * @throws IOException 读取或解析失败抛出异常
     */
    private FileRegion nextInternal(Iterator<FileRegion> i) throws IOException {
      BufferedReader r = iterators.get(i);
      if (null == r) {
        throw new IllegalStateException();
      }
      String line = r.readLine();
      if (null == line) {
        // 读到文件末尾，清理迭代器
        iterators.remove(i);
        return null;
      }
      // 按分隔符切分字段
      String[] f = line.split(delim);
      // 检查字段数量：5个基础字段 + 可选nonce字段共6个
      if (f.length != 5 && f.length != 6) {
        throw new IOException("Invalid line: " + line);
      }
      byte[] nonce = new byte[0];
      if (f.length == 6) {
        // 解析Base64编码的nonce
        nonce = Base64.getDecoder().decode(f[5]);
      }
      // 构造FileRegion对象：块ID、文件路径、偏移量、长度、块时间戳、nonce
      return new FileRegion(Long.parseLong(f[0]), new Path(f[1]),
          Long.parseLong(f[2]), Long.parseLong(f[3]), Long.parseLong(f[4]),
          nonce);
    }

    /**
     * 创建输入流，自动处理压缩。
     * @return 解码后的输入流
     * @throws IOException 打开文件时抛出IO异常
     */
    public InputStream createStream() throws IOException {
      InputStream i = fs.open(file);
      if (codec != null) {
        i = codec.createInputStream(i);
      }
      return i;
    }

    @Override
    public Iterator<FileRegion> iterator() {
      FRIterator i = new FRIterator();
      try {
        // 创建新的BufferedReader并注册到迭代器管理Map
        BufferedReader r =
            new BufferedReader(new InputStreamReader(createStream(), StandardCharsets.UTF_8));
        iterators.put(i, r);
        // 预读第一行
        i.pending = nextInternal(i);
      } catch (IOException e) {
        iterators.remove(i);
        throw new RuntimeException(e);
      }
      return i;
    }

    /**
     * 关闭所有打开的迭代器和流，清理资源。
     * @throws IOException 关闭时发生IO异常抛出，多个异常合并为MultipleIOException
     */
    @Override
    public void close() throws IOException {
      ArrayList<IOException> ex = new ArrayList<>();
      synchronized (iterators) {
        for (Iterator<BufferedReader> i = iterators.values().iterator();
             i.hasNext();) {
          try {
            BufferedReader r = i.next();
            r.close();
          } catch (IOException e) {
            ex.add(e);
          } finally {
            i.remove();
          }
        }
        iterators.clear();
      }
      if (!ex.isEmpty()) {
        throw MultipleIOException.createIOException(ex);
      }
    }
  }

  /**
   * 文本格式块别名映射写入器，将FileRegion写入分隔符文本文件。
   * 支持输出压缩文本文件。
   */
  public static class TextWriter extends Writer<FileRegion> {

    /**
     * TextWriter选项接口，定义可配置参数。
     */
    public interface Options extends Writer.Options {
      Options codec