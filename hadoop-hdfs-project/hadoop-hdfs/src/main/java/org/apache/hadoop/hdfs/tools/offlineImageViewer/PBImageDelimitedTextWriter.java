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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.ErasureCodingPolicyManager;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INode;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeSymlink;

import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;

/**
 * 文件级注释：PB fsimage的分隔符文本格式输出器，将fsimage中的inode信息转换为分隔符分隔的文本格式
 * 输出所有inode通用属性，不包含单个文件的块信息，默认分隔符为制表符，支持通过构造函数自定义分隔符
 */
public class PBImageDelimitedTextWriter extends PBImageTextWriter {
  private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm";
  private boolean printStoragePolicy;
  private boolean printECPolicy;
  private ErasureCodingPolicyManager ecManager;

  /**
   * 输出条目构建器，负责将inode信息组装为分隔符分隔的单行输出文本
   */
  static class OutputEntryBuilder {
    private final SimpleDateFormat dateFormatter =
        new SimpleDateFormat(DATE_FORMAT);

    private PBImageDelimitedTextWriter writer;
    private Path path;
    private int replication = 0;
    private long modificationTime;
    private long accessTime = 0;
    private long preferredBlockSize = 0;
    private int blocksCount = 0;
    private long fileSize = 0;
    private long nsQuota = 0;
    private long dsQuota = 0;
    private int storagePolicy = 0;
    private String ecPolicy = "-";

    private String dirPermission = "-";
    private PermissionStatus permissionStatus;
    private String aclPermission = "";

    /**
     * 根据inode类型，提取对应属性构建输出条目
     * @param writer PBImageDelimitedTextWriter实例，提供权限解析方法
     * @param inode 待输出的inode原始数据
     */
    OutputEntryBuilder(PBImageDelimitedTextWriter writer, INode inode) {
      this.writer = writer;
      switch (inode.getType()) {
      case FILE:
        // 处理文件类型inode，提取文件相关属性
        INodeFile file = inode.getFile();
        replication = file.getReplication();
        modificationTime = file.getModificationTime();
        accessTime = file.getAccessTime();
        preferredBlockSize = file.getPreferredBlockSize();
        blocksCount = file.getBlocksCount();
        fileSize = FSImageLoader.getFileSize(file);
        permissionStatus = writer.getPermission(file.getPermission());
        if (file.hasAcl() && file.getAcl().getEntriesCount() > 0){
          aclPermission = "+";
        }
        storagePolicy = file.getStoragePolicyID();
        if (writer.printECPolicy && file.hasErasureCodingPolicyID()) {
          // 从纠删码管理器获取策略名称
          ErasureCodingPolicy policy = writer.ecManager.
              getByID((byte) file.getErasureCodingPolicyID());
          if (policy != null) {
            ecPolicy = policy.getName();
          }
        }
        break;
      case DIRECTORY:
        // 处理目录类型inode，提取目录相关属性
        INodeDirectory dir = inode.getDirectory();
        modificationTime = dir.getModificationTime();
        nsQuota = dir.getNsQuota();
        dsQuota = dir.getDsQuota();
        dirPermission = "d";
        permissionStatus = writer.getPermission(dir.getPermission());
        if (dir.hasAcl() && dir.getAcl().getEntriesCount() > 0) {
          aclPermission = "+";
        }
        storagePolicy = writer.getStoragePolicy(dir.getXAttrs());
        if (writer.printECPolicy) {
          // 从扩展属性获取目录纠删码策略名称
          String name= writer.getErasureCodingPolicyName(dir.getXAttrs());
          if (name != null) {
            ecPolicy = name;
          }
        }
        break;
      case SYMLINK:
        // 处理符号链接类型inode，提取符号链接相关属性
        INodeSymlink s = inode.getSymlink();
        modificationTime = s.getModificationTime();
        accessTime = s.getAccessTime();
        permissionStatus = writer.getPermission(s.getPermission());
        storagePolicy = HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED;
        break;
      default:
        break;
      }
    }

    void setPath(Path path) {
      this.path = path;
    }

    /**
     * 组装所有属性为分隔符分隔的输出文本
     * @return 格式化后的单行输出字符串
     */
    public String build() {
      assert permissionStatus != null : "The PermissionStatus is null!";
      assert permissionStatus.getUserName() != null : "User name is null!";
      assert permissionStatus.getGroupName() != null : "Group name is null!";

      StringBuffer buffer = new StringBuffer();
      writer.append(buffer, path.toString());
      writer.append(buffer, replication);
      writer.append(buffer, dateFormatter.format(modificationTime));
      writer.append(buffer, dateFormatter.format(accessTime));
      writer.append(buffer, preferredBlockSize);
      writer.append(buffer, blocksCount);
      writer.append(buffer, fileSize);
      writer.append(buffer, nsQuota);
      writer.append(buffer, dsQuota);
      writer.append(buffer, dirPermission +
          permissionStatus.getPermission().toString() + aclPermission);
      writer.append(buffer, permissionStatus.getUserName());
      writer.append(buffer, permissionStatus.getGroupName());
      if (writer.printStoragePolicy) {
        writer.append(buffer, storagePolicy);
      }
      if (writer.printECPolicy) {
        writer.append(buffer, ecPolicy);
      }
      return buffer.substring(1);
    }
  }

  /**
   * 构造函数，默认不输出存储策略
   * @param out 输出流
   * @param delimiter 字段分隔符
   * @param tempPath 临时文件路径
   * @throws IOException
   */
  PBImageDelimitedTextWriter(PrintStream out, String delimiter, String tempPath)
      throws IOException {
    this(out, delimiter, tempPath, false);
  }

  /**
   * 构造函数，支持配置是否输出存储策略，默认不输出纠删码策略
   * @param out 输出流
   * @param delimiter 字段分隔符
   * @param tempPath 临时文件路径
   * @param printStoragePolicy 是否输出存储策略
   * @throws IOException
   */
  PBImageDelimitedTextWriter(PrintStream out, String delimiter,
                             String tempPath, boolean printStoragePolicy)
      throws IOException {
    this(out, delimiter, tempPath, printStoragePolicy, false, 1, "-", null);
  }

  /**
   * 完整构造函数，支持所有配置选项
   * @param out 输出流
   * @param delimiter 字段分隔符
   * @param tempPath 临时文件路径
   * @param printStoragePolicy 是否输出存储策略
   * @param printECPolicy 是否输出纠删码策略
   * @param threads 并行处理线程数
   * @param parallelOut 并行输出路径
   * @param conf Hadoop配置
   * @throws IOException
   */
  PBImageDelimitedTextWriter(PrintStream out, String delimiter,
                             String tempPath, boolean printStoragePolicy,
                             boolean printECPolicy, int threads,
                             String parallelOut, Configuration conf)
      throws IOException {
    super(out, delimiter, tempPath, threads, parallelOut);
    this.printStoragePolicy = printStoragePolicy;
    if (printECPolicy && conf != null) {
      // 开启纠删码策略输出，初始化纠删码策略管理器
      this.printECPolicy = true;
      ecManager = ErasureCodingPolicyManager.getInstance();
      ecManager.init(conf);
    }
  }

  @Override
  /**
   * 根据父路径和inode生成完整输出条目
   * @param parent 父目录路径
   * @param inode inode原始数据
   * @return 格式化后的完整条目字符串
   */
  public String getEntry(String parent, INode inode) {
    OutputEntryBuilder entryBuilder =
        new OutputEntryBuilder(this, inode);

    String inodeName = inode.getName().toStringUtf8();
    Path path = new Path(parent.isEmpty() ? "/" : parent,
      inodeName.isEmpty() ? "/" : inodeName);
    entryBuilder.setPath(path);

    return entryBuilder.build();
  }

  @Override
  /**
   * 生成输出文件的表头，包含所有输出字段名
   * @return 表头字符串
   */
  public String getHeader() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("Path");
    append(buffer, "Replication");
    append(buffer, "ModificationTime");
    append(buffer, "AccessTime");
    append(buffer, "PreferredBlockSize");
    append(buffer, "BlocksCount");
    append(buffer, "FileSize");
    append(buffer, "NSQUOTA");
    append(buffer, "DSQUOTA");
    append(buffer, "Permission");
    append(buffer, "UserName");
    append(buffer, "GroupName");
    if (printStoragePolicy) {
      append(buffer, "StoragePolicyId");
    }
    if (printECPolicy) {
      append(buffer, "ErasureCodingPolicy");
    }
    return buffer.toString();
  }

  @Override
  /**
   * 输出完成后的回调方法，本实现无需处理
   */
  public void afterOutput() {
    // do nothing
  }
}