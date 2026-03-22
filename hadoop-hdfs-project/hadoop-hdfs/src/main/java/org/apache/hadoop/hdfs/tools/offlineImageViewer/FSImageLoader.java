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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.XAttrNotFoundException;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf;
import org.apache.hadoop.hdfs.server.namenode.FSImageUtil;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.hdfs.server.namenode.SerialNumberManager;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.resources.XAttrEncodingParam;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.LimitInputStream;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.thirdparty.protobuf.CodedInputStream;
import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件系统镜像加载器，负责加载HDFS fsimage文件到内存，并提供JSON格式的文件系统元数据查询能力，用于离线fsimage查看工具
 */
class FSImageLoader {
  public static final Logger LOG =
      LoggerFactory.getLogger(FSImageHandler.class);

  private final SerialNumberManager.StringTable stringTable;
  // 按inode id排序的inode字节序列化数据数组
  private final byte[][] inodes;
  // 目录inode id -> 目录下所有子inode id数组的映射
  private final Map<Long, long[]> dirmap;
  // inode字节数据按id排序的比较器
  private static final Comparator<byte[]> INODE_BYTES_COMPARATOR = new
          Comparator<byte[]>() {
    @Override
    public int compare(byte[] o1, byte[] o2) {
      try {
        final FsImageProto.INodeSection.INode l = FsImageProto.INodeSection
                .INode.parseFrom(o1);
        final FsImageProto.INodeSection.INode r = FsImageProto.INodeSection
                .INode.parseFrom(o2);
        if (l.getId() < r.getId()) {
          return -1;
        } else if (l.getId() > r.getId()) {
          return 1;
        } else {
          return 0;
        }
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  };

  private FSImageLoader(SerialNumberManager.StringTable stringTable,
                        byte[][] inodes, Map<Long, long[]> dirmap) {
    this.stringTable = stringTable;
    this.inodes = inodes;
    this.dirmap = dirmap;
  }

  /**
   * 从指定文件路径加载fsimage到内存，构建FSImageLoader实例
   * @param inputFile 待加载的fsimage文件路径
   * @return 加载完成的FSImageLoader实例
   * @throws IOException 加载fsimage失败时抛出
   */
  static FSImageLoader load(String inputFile) throws IOException {
    Configuration conf = new Configuration();
    RandomAccessFile file = new RandomAccessFile(inputFile, "r");
    if (!FSImageUtil.checkFileFormat(file)) {
      throw new IOException("Unrecognized FSImage");
    }

    FsImageProto.FileSummary summary = FSImageUtil.loadSummary(file);


    try (FileInputStream fin = new FileInputStream(file.getFD())) {
      // 存储inode引用对应的被引用inode id列表
      ImmutableList<Long> refIdList = null;
      SerialNumberManager.StringTable stringTable = null;
      byte[][] inodes = null;
      Map<Long, long[]> dirmap = null;

      // 从摘要中获取所有fsimage节
      ArrayList<FsImageProto.FileSummary.Section> sections =
          Lists.newArrayList(summary.getSectionsList());
      // 按节类型序号排序节
      Collections.sort(sections,
          new Comparator<FsImageProto.FileSummary.Section>() {
            @Override
            public int compare(FsImageProto.FileSummary.Section s1,
                               FsImageProto.FileSummary.Section s2) {
              FSImageFormatProtobuf.SectionName n1 =
                  FSImageFormatProtobuf.SectionName.fromString(s1.getName());
              FSImageFormatProtobuf.SectionName n2 =
                  FSImageFormatProtobuf.SectionName.fromString(s2.getName());
              if (n1 == null) {
                return n2 == null ? 0 : -1;
              } else if (n2 == null) {
                return -1;
              } else {
                return n1.ordinal() - n2.ordinal();
              }
            }
          });

      // 遍历处理每个节，加载对应数据
      for (FsImageProto.FileSummary.Section s : sections) {
        // 将输入流定位到当前节的起始偏移
        fin.getChannel().position(s.getOffset());
        // 包装输入流，处理压缩并限制读取长度为当前节长度
        InputStream is = FSImageUtil.wrapInputStreamForCompression(conf,
            summary.getCodec(), new BufferedInputStream(new LimitInputStream(
            fin, s.getLength())));

        if (LOG.isDebugEnabled()) {
          LOG.debug("Loading section " + s.getName() + " length: " + s.getLength
              ());
        }

        // 获取节类型
        FSImageFormatProtobuf.SectionName sectionName
            = FSImageFormatProtobuf.SectionName.fromString(s.getName());
        if (sectionName == null) {
          throw new IOException("Unrecognized section " + s.getName());
        }
        // 根据节类型加载对应数据
        switch (sectionName) {
          case STRING_TABLE:
            stringTable = loadStringTable(is);
            break;
          case INODE:
            inodes = loadINodeSection(is);
            break;
          case INODE_REFERENCE:
            refIdList = loadINodeReferenceSection(is);
            break;
          case INODE_DIR:
            dirmap = loadINodeDirectorySection(is, refIdList);
            break;
          default:
            break;
        }
      }
      return new FSImageLoader(stringTable, inodes, dirmap);
    }
  }

  /**
   * 加载inode目录节，构建目录id到子inode id列表的映射
   * @param in 目录节输入流
   * @param refIdList inode引用id列表
   * @return 目录映射表
   * @throws IOException 加载失败时抛出
   */
  private static Map<Long, long[]> loadINodeDirectorySection
          (InputStream in, List<Long> refIdList)
      throws IOException {
    LOG.info("Loading inode directory section");
    Map<Long, long[]> dirs = Maps.newHashMap();
    long counter = 0;
    while (true) {
      // 从流中解析一个目录条目
      FsImageProto.INodeDirectorySection.DirEntry e =
          FsImageProto.INodeDirectorySection.DirEntry.parseDelimitedFrom(in);
      // LimitedInputStream读到末尾会返回null，结束循环
      if (e == null) {
        break;
      }
      ++counter;

      // 合并普通子inode和引用子inode的id
      long[] l = new long[e.getChildrenCount() + e.getRefChildrenCount()];
      for (int i = 0; i < e.getChildrenCount(); ++i) {
        l[i] = e.getChildren(i);
      }
      for (int i = e.getChildrenCount(); i < l.length; i++) {
        int refId = e.getRefChildren(i - e.getChildrenCount());
        l[i] = refIdList.get(refId);
      }
      dirs.put(e.getParent(), l);
    }
    LOG.info("Loaded " + counter + " directories");
    return dirs;
  }

  /**
   * 加载inode引用节，获取所有引用指向的原始inode id
   * @param in inode引用节输入流
   * @return 不可变的被引用inode id列表
   * @throws IOException 加载失败时抛出
   */
  static ImmutableList<Long> loadINodeReferenceSection(InputStream in)
      throws IOException {
    LOG.info("Loading inode references");
    ImmutableList.Builder<Long> builder = ImmutableList.builder();
    long counter = 0;
    while (true) {
      FsImageProto.INodeReferenceSection.INodeReference e =
          FsImageProto.INodeReferenceSection.INodeReference
              .parseDelimitedFrom(in);
      if (e == null) {
        break;
      }
      ++counter;
      builder.add(e.getReferredId());
    }
    LOG.info("Loaded " + counter + " inode references");
    return builder.build();
  }

  /**
   * 加载inode节，读取所有inode的原始字节并按id排序
   * @param in inode节输入流
   * @return 按id排序的inode字节数组
   * @throws IOException 加载失败时抛出
   */
  private static byte[][] loadINodeSection(InputStream in)
          throws IOException {
    FsImageProto.INodeSection s = FsImageProto.INodeSection
        .parseDelimitedFrom(in);
    LOG.info("Loading " + s.getNumInodes() + " inodes.");
    final byte[][] inodes = new byte[(int) s.getNumInodes()][];

    // 读取每个inode的原始字节，不解析，节省内存
    for (int i = 0; i < s.getNumInodes(); ++i) {
      int size = CodedInputStream.readRawVarint32(in.read(), in);
      byte[] bytes = new byte[size];
      IOUtils.readFully(in, bytes, 0, size);
      inodes[i] = bytes;
    }
    LOG.debug("Sorting inodes");
    // 按inode id排序，方便后续二分查找
    Arrays.sort(inodes, INODE_BYTES_COMPARATOR);
    LOG.debug("Finished sorting inodes");
    return inodes;
  }

  /**
   * 加载字符串表节，反序列化字符串序列号到字符串的映射表
   * @param in 字符串表节输入流
   * @return 字符串表实例
   * @throws IOException 加载失败时抛出
   */
  static SerialNumberManager.StringTable loadStringTable(InputStream in)
        throws IOException {
    FsImageProto.StringTableSection s = FsImageProto.StringTableSection
        .parseDelimitedFrom(in);
    LOG.info("Loading " + s.getNumEntry() + " strings");
    SerialNumberManager.StringTable stringTable =
        SerialNumberManager.newStringTable(s.getNumEntry(), s.getMaskBits());
    // 将所有字符串存入字符串表
    for (int i = 0; i < s.getNumEntry(); ++i) {
      FsImageProto.StringTableSection.Entry e = FsImageProto
          .StringTableSection.Entry.parseDelimitedFrom(in);
      stringTable.put(e.getId(), e.getStr());
    }
    return stringTable;
  }

  /**
   * 获取指定路径的文件状态，返回JSON格式字符串
   * @param path 目标文件路径
   * @return JSON格式的文件状态
   * @throws IOException 序列化或查找失败时抛出
   */
  String getFileStatus(String path) throws IOException {
    FsImageProto.INodeSection.INode inode = fromINodeId(lookup(path));
    return "{\"FileStatus\":\n"
        + JsonUtil.toJsonString(getFileStatus(inode, false)) + "\n}\n";
  }

  /**
   * 列出指定目录下所有文件的状态，返回JSON格式字符串
   * @param path 目标目录路径
   * @return JSON格式的文件状态列表
   * @throws IOException 序列化或查找失败时抛出
   */
  String listStatus(String path) throws IOException {
    StringBuilder sb = new StringBuilder();
    List<Map<String, Object>> fileStatusList = getFileStatusList(path);
    sb.append("{\"FileStatuses\":{\"FileStatus\":[\n");
    int i = 0;
    for (Map<String, Object> fileStatusMap : fileStatusList) {
      if (i++ != 0) {
        sb.append(',');
      }
      sb.append(JsonUtil.toJsonString(fileStatusMap));
    }
    sb.append("\n]}}\n");
    return sb.toString();
  }

  /**
   * 获取指定路径下所有文件状态列表
   * @param path 目标路径
   * @return 文件状态map列表
   * @throws IOException 查找失败时抛出
   */
  private List<Map<String, Object>> getFileStatusList(String path)
          throws IOException {
    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
    long id = lookup(path);
    FsImageProto.INodeSection.INode inode = fromINodeId(id);
    // 如果是目录，遍历所有子节点添加到列表
    if (inode.getType() == FsImageProto.INodeSection.INode.Type.DIRECTORY) {
      if (!dirmap.containsKey(id)) {
        // 空目录返回空列表
        return list;
      }
      long[] children = dirmap.get(id);
      for (long cid : children) {
        list.add(getFileStatus(fromINodeId(cid), true));
      }
    } else {
      // 非目录直接返回自身状态
      list.add(getFileStatus(inode, false));
    }
    return list;
  }

  /**
   * 获取指定路径的内容摘要，返回JSON格式字符串
   * @param path 目标路径
   * @return JSON格式的内容摘要
   * @throws IOException 序列化或查找失败时抛出
   */
  String getContentSummary(String path) throws IOException {
    return "{\"ContentSummary\":\n"
        + JsonUtil.toJsonString(getContentSummaryMap(path)) + "\n}\n";
  }

  /**
   * 计算指定路径的内容摘要，填充到map中
   * @param path 目标路径
   * @return 内容摘要map
   * @throws IOException 查找失败时抛出
   */
  private Map<String, Object> getContentSummaryMap(String path)
      throws IOException {
    long id = lookup(path);
    INode inode = fromINodeId(id);
    long spaceQuota = 0;
    long nsQuota = 0;
    // 数据数组：[目录数, 文件数, 总大小, 总空间消耗]
    long[] data = new long[4];
    FsImageProto.INodeSection.INodeFile f = inode.getFile();
    switch (inode.getType()) {
    case FILE:
      data[0] = 0;
      data[1] = 1;
      data[2] = getFileSize(f);
      nsQuota = -1;
      data[3] = data[2] * f.getReplication();
      spaceQuota = -1;
      return fillSummaryMap(spaceQuota, nsQuota, data);
    case DIRECTORY:
      fillDirSummary(id, data);
      nsQuota = inode.getDirectory().getNsQuota();
      spaceQuota = inode.getDirectory().getDsQuota();
      return fillSummaryMap(spaceQuota, nsQuota, data);
    case SYMLINK:
      data[0] = 0;
      data[1] = 1;
      data[2] = 0;
      nsQuota = -1;
      data[3] = 0;
      spaceQuota = -1;
      return fillSummaryMap(spaceQuota, nsQuota, data);
    default:
      return null;
    }

  }

  /**
   * 将内容摘要数据填充到