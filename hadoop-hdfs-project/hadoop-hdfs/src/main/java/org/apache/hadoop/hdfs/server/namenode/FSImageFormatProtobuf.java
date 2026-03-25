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

package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CachePoolInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ErasureCodingPolicyProto;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.CacheManagerSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.NameSystemSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SecretManagerSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.StringTableSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.ErasureCodingSection;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FSImageFormatPBSnapshot;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.LimitInputStream;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.util.Lists;

import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.thirdparty.protobuf.CodedOutputStream;

/**
 * Protobuf格式FsImage镜像文件的读写工具类，负责NameNode元数据镜像的序列化和反序列化。
 * 该类提供了并行加载支持，提升大型集群元数据加载速度。
 */
@InterfaceAudience.Private
public final class FSImageFormatProtobuf {
  private static final Logger LOG = LoggerFactory
      .getLogger(FSImageFormatProtobuf.class);

  // 并行加载开关，volatile保证并发可见性
  private static volatile boolean enableParallelLoad = false;

  /**
   * FsImage加载上下文，保存加载过程中的共享状态数据。
   */
  public static final class LoaderContext {
    private SerialNumberManager.StringTable stringTable;
    private final ArrayList<INodeReference> refList = Lists.newArrayList();

    /**
     * 获取字符串序列化表，用于路径字符串去重压缩。
     * @return 字符串表对象
     */
    public SerialNumberManager.StringTable getStringTable() {
      return stringTable;
    }

    /**
     * 获取快照INode引用列表，保存快照中引用的原始节点。
     * @return INode引用列表
     */
    public ArrayList<INodeReference> getRefList() {
      return refList;
    }
  }

  /**
   * FsImage保存上下文，保存保存过程中的共享状态数据。
   */
  public static final class SaverContext {
    /**
     * 基于值的去重映射表，为重复对象分配唯一ID，减少序列化冗余。
     * @param <E> 去重对象类型
     */
    public static class DeduplicationMap<E> {
      private final Map<E, Integer> map = Maps.newHashMap();
      private DeduplicationMap() {}

      /**
       * 创建新的去重映射实例。
       * @param <T> 映射键类型
       * @return 新去重映射实例
       */
      static <T> DeduplicationMap<T> newMap() {
        return new DeduplicationMap<T>();
      }

      /**
       * 获取值对应的ID，如果不存在则分配新ID。
       * @param value 待去重的值
       * @return 值对应的ID，0表示null
       */
      int getId(E value) {
        if (value == null) {
          return 0;
        }
        Integer v = map.get(value);
        if (v == null) {
          int nv = map.size() + 1;
          map.put(value, nv);
          return nv;
        }
        return v;
      }

      /**
       * 获取映射中已存储的条目数量。
       * @return 条目数量
       */
      int size() {
        return map.size();
      }

      /**
       * 获取所有条目集合用于序列化输出。
       * @return 条目集合
       */
      Set<Entry<E, Integer>> entrySet() {
        return map.entrySet();
      }
    }
    private final ArrayList<INodeReference> refList = Lists.newArrayList();

    /**
     * 获取快照INode引用列表。
     * @return INode引用列表
     */
    public ArrayList<INodeReference> getRefList() {
      return refList;
    }
  }

  /**
   * Protobuf格式FsImage加载器，实现从镜像文件恢复NameNode元数据。
   * 支持并行加载大分区元数据，提升启动速度。
   */
  public static final class Loader implements FSImageFormat.AbstractLoader {
    static final int MINIMUM_FILE_LENGTH = 8;
    private final Configuration conf;
    private final FSNamesystem fsn;
    private final LoaderContext ctx;
    /** 已加载镜像文件的MD5摘要 */
    private MD5Hash imgDigest;
    /** 镜像文件包含的最后一个事务ID */
    private long imgTxId;
    /**
     * 是否要求镜像布局版本必须和当前版本完全一致，仅在滚动升级回滚时使用。
     */
    private final boolean requireSameLayoutVersion;

    private File filename;

    /**
     * 构造FsImage加载器。
     * @param conf Hadoop配置对象
     * @param fsn 目标文件系统命名空间
     * @param requireSameLayoutVersion 是否要求布局版本严格匹配
     */
    Loader(Configuration conf, FSNamesystem fsn,
        boolean requireSameLayoutVersion) {
      this.conf = conf;
      this.fsn = fsn;
      this.ctx = new LoaderContext();
      this.requireSameLayoutVersion = requireSameLayoutVersion;
    }

    @Override
    public MD5Hash getLoadedImageMd5() {
      return imgDigest;
    }

    @Override
    public long getLoadedImageTxId() {
      return imgTxId;
    }

    /**
     * 获取加载上下文对象。
     * @return 加载上下文
     */
    public LoaderContext getLoaderContext() {
      return ctx;
    }

    /**
     * 后台线程并行计算FsImage文件MD5摘要，和加载操作并行提升性能。
     */
    private static class DigestThread extends SubjectInheritingThread {

      /** 计算过程中发生的IO异常 */
      private volatile IOException ioe = null;

      /** 计算完成的MD5摘要 */
      private volatile MD5Hash digest = null;

      /** 需要计算摘要的FsImage文件 */
      private final File file;

      /**
       * 构造摘要计算线程。
       * @param inFile 待计算的FsImage文件
       */
      DigestThread(File inFile) {
        file = inFile;
        setName(inFile.getName() + " MD5 compute");
        setDaemon(true);
      }

      /**
       * 获取计算完成的摘要，如果发生异常则抛出。
       * @return MD5摘要
       * @throws IOException 计算过程中发生异常
       */
      public MD5Hash getDigest() throws IOException {
        if (ioe != null) {
          throw ioe;
        }
        return digest;
      }

      /**
       * 获取计算过程中发生的异常。
       * @return IO异常，如果无异常返回null
       */
      public IOException getException() {
        return ioe;
      }

      @Override
      public void work() {
        try {
          digest = MD5FileUtils.computeMd5ForFile(file);
        } catch (IOException e) {
          ioe = e;
        } catch (Throwable t) {
          ioe = new IOException(t);
        }
      }

      @Override
      public String toString() {
        return "DigestThread{ ThreadName=" + getName() + ", digest=" + digest
            + ", file=" + file + '}';
      }
    }

    /**
     * 加载指定FsImage文件，恢复元数据到FSNamesystem。
     * @param file 待加载的FsImage文件
     * @throws IOException 加载过程发生IO错误或格式错误
     */
    void load(File file) throws IOException {
      filename = file;
      long start = Time.monotonicNow();
      // 启动并行MD5计算线程
      DigestThread dt = new DigestThread(file);
      dt.start();
      RandomAccessFile raFile = new RandomAccessFile(file, "r");
      FileInputStream fin = new FileInputStream(file);
      try {
        // 执行核心加载逻辑
        loadInternal(raFile, fin);
        // 等待MD5计算完成并获取结果
        try {
          dt.join();
          imgDigest = dt.getDigest();
        } catch (InterruptedException ie) {
          throw new IOException(ie);
        }
        long end = Time.monotonicNow();
        LOG.info("Loaded FSImage in {} seconds.", (end - start) / 1000);
      } finally {
        // 确保资源关闭
        fin.close();
        raFile.close();
      }
    }

    /**
     * 根据section描述创建对应输入流，定位到section起始位置并限制长度。
     * @param section section描述信息，包含偏移和长度
     * @param compressionCodec 使用的压缩算法，如果未压缩为null
     * @return 包装好的section输入流
     * @throws IOException 创建输入流发生IO错误
     */
    public InputStream getInputStreamForSection(FileSummary.Section section,
                                                String compressionCodec)
        throws IOException {
      FileInputStream fin = new FileInputStream(filename);
      try {
          FileChannel channel = fin.getChannel();
          // 定位到section起始偏移
          channel.position(section.getOffset());
          // 创建限制长度的输入流
          InputStream in = new BufferedInputStream(new LimitInputStream(fin,
                  section.getLength()));
          // 根据配置包装压缩流
          in = FSImageUtil.wrapInputStreamForCompression(conf,
                  compressionCodec, in);
          return in;
      } catch (IOException e) {
          fin.close();
          throw e;
      }
    }

    /**
     * 从所有section列表中提取出子section，修改原列表并返回提取结果。
     * 子section名称以_SUB结尾，用于并行加载。
     * @param sections 包含所有section和子section的列表
     * @return 提取出的子section列表，无则返回空列表
     */
    private ArrayList<FileSummary.Section> getAndRemoveSubSections(
        ArrayList<FileSummary.Section> sections) {
      ArrayList<FileSummary.Section> subSections = new ArrayList<>();
      Iterator<FileSummary.Section> iter = sections.iterator();
      while (iter.hasNext()) {
        FileSummary.Section s = iter.next();
        String name = s.getName();
        // 过滤出名称以_SUB结尾的子section
        if (name.matches(".*_SUB$")) {
          subSections.add(s);
          iter.remove();
        }
      }
      return subSections;
    }

    /**
     * 从子section列表中筛选指定名称的子section。
     * @param sections 待筛选的子section列表
     * @param name 需要筛选的section名称
     * @return 匹配名称的子section列表，无则返回空列表
     */
    private ArrayList<FileSummary.Section> getSubSectionsOfName(
        ArrayList<FileSummary.Section> sections, SectionName name) {
      ArrayList<FileSummary.Section> subSec = new ArrayList<>();
      for (FileSummary.Section s : sections) {
        String n = s.getName();
        SectionName sectionName = SectionName.fromString(n);
        if (sectionName == name) {
          subSec.add(s);
        }
      }
      return subSec;
    }

    /**
     * 根据配置创建并行加载使用的线程池，处理非法配置并重置为默认值。
     * @return 配置好线程数的线程池
     */
    private ExecutorService getParallelExecutorService() {
      int threads = conf.getInt(DFSConfigKeys.DFS_IMAGE_PARALLEL_THREADS_KEY,
          DFSConfigKeys.DFS_IMAGE_PARALLEL_THREADS_DEFAULT);
      if (threads < 1) {
        LOG.warn("Parallel is enabled and {} is set to {}. Setting to the " +
            "default value {}", DFSConfigKeys.DFS_IMAGE_PARALLEL_THREADS_KEY,
            threads, DFSConfigKeys.DFS_IMAGE_PARALLEL_THREADS_DEFAULT);
        threads = DFSConfigKeys.DFS_IMAGE_PARALLEL_THREADS_DEFAULT;
      }
      ExecutorService executorService = Executors.newFixedThreadPool(
          threads);
      LOG.info("The fsimage will be loaded in parallel using {} threads",
          threads);
      return executorService;
    }

    /**
     * FsImage核心加载逻辑，按顺序加载各个section恢复元数据。
     * @param raFile 随机访问文件句柄，用于读取文件头
     * @param fin 文件输入流，用于读取section内容
     * @throws IOException 读取或解析发生错误
     */
    private void loadInternal(RandomAccessFile raFile, FileInputStream fin)
        throws IOException {
      // 检查文件魔数验证格式
      if (!FSImageUtil.checkFileFormat(raFile)) {
        throw new IOException("Unrecognized file format");
      }
      // 加载文件摘要信息
      FileSummary summary = FSImageUtil.loadSummary(raFile);
      // 如果要求版本严格匹配，检查布局版本
      if (requireSameLayoutVersion && summary.getLayoutVersion() !=
          HdfsServerConstants.NAMENODE_LAYOUT_VERSION) {
        throw new IOException("Image version " + summary.getLayoutVersion() +
            " is not equal to the software version " +
            HdfsServerConstants.NAMENODE_LAYOUT_VERSION);
      }

      FileChannel channel = fin.getChannel();

      // 创建INode和快照加载器实例
      FSImageFormatPBINode.Loader inodeLoader = new FSImageFormatPBINode.Loader(
          fsn, this);
      FSImageFormatPBSnapshot.Loader snapshotLoader = new FSImageFormatPBSnapshot.Loader(
          fsn, this);

      // 获取所有section并按枚举顺序排序，保证加载顺序正确
      ArrayList<FileSummary.Section> sections = Lists.newArrayList(summary
          .getSectionsList());
      Collections.sort(sections, new Comparator<FileSummary.Section>() {
        @Override
        public int compare(FileSummary.Section s1, FileSummary.Section s2) {
          SectionName n1 = SectionName.fromString(s1.getName());
          SectionName n2 = SectionName.fromString(s2.getName());
          if (n1 == null) {
            return n2 == null ? 0 : -1;
          } else if (n2 == null) {
            return -1;
          } else