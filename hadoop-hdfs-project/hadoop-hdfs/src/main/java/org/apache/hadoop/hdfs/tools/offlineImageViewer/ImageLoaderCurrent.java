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

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LayoutFlags;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.FSImageSerialization;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.hdfs.tools.offlineImageViewer.ImageVisitor.ImageElement;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.security.token.delegation.DelegationKey;

/**
 * 文件级注释：处理当前版本HDFS FSImage镜像文件，通过访问者模式遍历FSImage中所有元素并调用访问者回调，
 * 是离线FSImage查看工具的核心加载器，支持多个布局版本的FSImage解析。
 * 
 * ImageLoaderCurrent processes Hadoop FSImage files and walks over
 * them using a provided ImageVisitor, calling the visitor at each element
 * enumerated below.
 *
 * The only difference between v18 and v19 was the utilization of the
 * stickybit.  Therefore, the same viewer can reader either format.
 *
 * Versions -19 fsimage layout (with changes from -16 up):
 * Image version (int)
 * Namepsace ID (int)
 * NumFiles (long)
 * Generation stamp (long)
 * INodes (count = NumFiles)
 *  INode
 *    Path (String)
 *    Replication (short)
 *    Modification Time (long as date)
 *    Access Time (long) // added in -16
 *    Block size (long)
 *    Num blocks (int)
 *    Blocks (count = Num blocks)
 *      Block
 *        Block ID (long)
 *        Num bytes (long)
 *        Generation stamp (long)
 *    Namespace Quota (long)
 *    Diskspace Quota (long) // added in -18
 *    Permissions
 *      Username (String)
 *      Groupname (String)
 *      OctalPerms (short -> String)  // Modified in -19
 *    Symlink (String) // added in -23
 * NumINodesUnderConstruction (int)
 * INodesUnderConstruction (count = NumINodesUnderConstruction)
 *  INodeUnderConstruction
 *    Path (bytes as string)
 *    Replication (short)
 *    Modification time (long as date)
 *    Preferred block size (long)
 *    Num blocks (int)
 *    Blocks
 *      Block
 *        Block ID (long)
 *        Num bytes (long)
 *        Generation stamp (long)
 *    Permissions
 *      Username (String)
 *      Groupname (String)
 *      OctalPerms (short -> String)
 *    Client Name (String)
 *    Client Machine (String)
 *    NumLocations (int)
 *    DatanodeDescriptors (count = numLocations) // not loaded into memory
 *      short                                    // but still in file
 *      long
 *      string
 *      long
 *      int
 *      string
 *      string
 *      enum
 *    CurrentDelegationKeyId (int)
 *    NumDelegationKeys (int)
 *      DelegationKeys (count = NumDelegationKeys)
 *        DelegationKeyLength (vint)
 *        DelegationKey (bytes)
 *    DelegationTokenSequenceNumber (int)
 *    NumDelegationTokens (int)
 *    DelegationTokens (count = NumDelegationTokens)
 *      DelegationTokenIdentifier
 *        owner (String)
 *        renewer (String)
 *        realUser (String)
 *        issueDate (vlong)
 *        maxDate (vlong)
 *        sequenceNumber (vint)
 *        masterKeyId (vint)
 *      expiryTime (long)     
 *
 */
/**
 * 类级注释：当前版本FSImage加载器，实现ImageLoader接口，负责解析新版本格式的FSImage文件，
 * 通过访问者模式将解析到的各个元素交给传入的ImageVisitor处理，支持快照、权限、代理令牌、缓存等新特性。
 */
class ImageLoaderCurrent implements ImageLoader {
  protected final DateFormat dateFormat = 
                                      new SimpleDateFormat("yyyy-MM-dd HH:mm");
  // 支持加载的FSImage布局版本列表
  private static int[] versions = { -16, -17, -18, -19, -20, -21, -22, -23,
      -24, -25, -26, -27, -28, -30, -31, -32, -33, -34, -35, -36, -37, -38, -39,
      -40, -41, -42, -43, -44, -45, -46, -47, -48, -49, -50, -51 };
  private int imageVersion = 0;
  
  // 快照子树访问标记，记录inode是否已被访问，避免重复处理
  private final Map<Long, Boolean> subtreeMap = new HashMap<Long, Boolean>();
  // 目录inode id到完整路径的映射，用于快照处理
  private final Map<Long, String> dirNodeMap = new HashMap<Long, String>();

  /* (non-Javadoc)
   * @see ImageLoader#canProcessVersion(int)
   */
  /**
   * 函数级注释：检查当前加载器是否支持指定的FSImage版本
   * @param version FSImage版本号
   * @return true表示支持该版本加载，false表示不支持
   */
  @Override
  public boolean canLoadVersion(int version) {
    for(int v : versions)
      if(v == version) return true;

    return false;
  }

  /* (non-Javadoc)
   * @see ImageLoader#processImage(java.io.DataInputStream, ImageVisitor, boolean)
   */
  /**
   * 函数级注释：加载并遍历FSImage，按结构解析各个元素并调用访问者回调
   * @param in FSImage输入流
   * @param v 访问器，处理解析到的各个元素
   * @param skipBlocks 是否跳过块信息解析，提升解析速度
   * @throws IOException 解析或IO异常
   */
  @Override
  public void loadImage(DataInputStream in, ImageVisitor v,
      boolean skipBlocks) throws IOException {
    boolean done = false;
    try {
      // 启动访问流程
      v.start();
      v.visitEnclosingElement(ImageElement.FS_IMAGE);

      // 读取FSImage版本
      imageVersion = in.readInt();
      if( !canLoadVersion(imageVersion))
        throw new IOException("Cannot process fslayout version " + imageVersion);
      // 支持布局标记特性，读取布局标记
      if (NameNodeLayoutVersion.supports(Feature.ADD_LAYOUT_FLAGS, imageVersion)) {
        LayoutFlags.read(in);
      }

      v.visit(ImageElement.IMAGE_VERSION, imageVersion);
      v.visit(ImageElement.NAMESPACE_ID, in.readInt());

      // 读取inode总数
      long numInodes = in.readLong();

      v.visit(ImageElement.GENERATION_STAMP, in.readLong());

      // 支持顺序块ID特性，读取相关世代戳信息
      if (NameNodeLayoutVersion.supports(Feature.SEQUENTIAL_BLOCK_ID, imageVersion)) {
        v.visit(ImageElement.GENERATION_STAMP_V2, in.readLong());
        v.visit(ImageElement.GENERATION_STAMP_V1_LIMIT, in.readLong());
        v.visit(ImageElement.LAST_ALLOCATED_BLOCK_ID, in.readLong());
      }

      // 支持存储事务ID特性，读取最新事务ID
      if (NameNodeLayoutVersion.supports(Feature.STORED_TXIDS, imageVersion)) {
        v.visit(ImageElement.TRANSACTION_ID, in.readLong());
      }
      
      // 支持inode ID特性，读取最后分配的inode ID
      if (NameNodeLayoutVersion.supports(Feature.ADD_INODE_ID, imageVersion)) {
        v.visit(ImageElement.LAST_INODE_ID, in.readLong());
      }
      
      // 检查是否支持快照特性
      boolean supportSnapshot = NameNodeLayoutVersion.supports(Feature.SNAPSHOT,
          imageVersion);
      if (supportSnapshot) {
        // 读取快照计数器和快照数量，遍历处理每个快照
        v.visit(ImageElement.SNAPSHOT_COUNTER, in.readInt());
        int numSnapshots = in.readInt();
        v.visit(ImageElement.NUM_SNAPSHOTS_TOTAL, numSnapshots);
        for (int i = 0; i < numSnapshots; i++) {
          processSnapshot(in, v);
        }
      }
      
      // 支持FSImage压缩特性，处理压缩镜像
      if (NameNodeLayoutVersion.supports(Feature.FSIMAGE_COMPRESSION, imageVersion)) {
        boolean isCompressed = in.readBoolean();
        v.visit(ImageElement.IS_COMPRESSED, String.valueOf(isCompressed));
        if (isCompressed) {
          // 读取压缩编解码器类名，创建解压流
          String codecClassName = Text.readString(in);
          v.visit(ImageElement.COMPRESS_CODEC, codecClassName);
          CompressionCodecFactory codecFac = new CompressionCodecFactory(
              new Configuration());
          CompressionCodec codec = codecFac.getCodecByClassName(codecClassName);
          if (codec == null) {
            throw new IOException("Image compression codec not supported: "
                + codecClassName);
          }
          // 替换输入流为解压流，后续从解压流读取数据
          in = new DataInputStream(codec.createInputStream(in));
        }
      }
      // 处理所有已完成的inode
      processINodes(in, v, numInodes, skipBlocks, supportSnapshot);
      // 清理临时缓存
      subtreeMap.clear();
      dirNodeMap.clear();

      // 处理构建中的inode（未完成写入的文件）
      processINodesUC(in, v, skipBlocks);

      // 支持代理令牌特性，处理代理令牌信息
      if (NameNodeLayoutVersion.supports(Feature.DELEGATION_TOKEN, imageVersion)) {
        processDelegationTokens(in, v);
      }
      
      // 支持缓存特性，处理缓存管理器状态
      if (NameNodeLayoutVersion.supports(Feature.CACHING, imageVersion)) {
        processCacheManagerState(in, v);
      }
      // 离开FSImage根元素
      v.leaveEnclosingElement(); // FSImage
      done = true;
    } finally {
      // 根据处理结果调用对应完成方法
      if (done) {
        v.finish();
      } else {
        v.finishAbnormally();
      }
    }
  }

  /**
   * 函数级注释：处理FSImage中缓存管理器状态，解析缓存池和缓存项信息
   * @param in FSImage输入流
   * @param v 访问器
   * @throws IOException IO异常
   */
  private void processCacheManagerState(DataInputStream in, ImageVisitor v)
      throws IOException {
    v.visit(ImageElement.CACHE_NEXT_ENTRY_ID, in.readLong());
    final int numPools = in.readInt();
    for (int i=0; i<numPools; i++) {
      v.visit(ImageElement.CACHE_POOL_NAME, Text.readString(in));
      processCachePoolPermission(in, v);
      v.visit(ImageElement.CACHE_POOL_WEIGHT, in.readInt());
    }
    final int numEntries = in.readInt();
    for (int i=0; i<numEntries; i++) {
      v.visit(ImageElement.CACHE_ENTRY_PATH, Text.readString(in));
      v.visit(ImageElement.CACHE_ENTRY_REPLICATION, in.readShort());
      v.visit(ImageElement.CACHE_ENTRY_POOL_NAME, Text.readString(in));
    }
  }

  /**
   * 函数级注释：处理FSImage中代理令牌相关信息，解析代理密钥和代理令牌
   * @param in FSImage输入流
   * @param v 访问器
   * @throws IOException IO异常
   */
  private void processDelegationTokens(DataInputStream in, ImageVisitor v)
      throws IOException {
    v.visit(ImageElement.CURRENT_DELEGATION_KEY_ID, in.readInt());
    int numDKeys = in.readInt();
    v.visitEnclosingElement(ImageElement.DELEGATION_KEYS,
        ImageElement.NUM_DELEGATION_KEYS, numDKeys);
    for(int i =0; i < numDKeys; i++) {
      DelegationKey key = new DelegationKey();
      key.readFields(in);
      v.visit(ImageElement.DELEGATION_KEY, key.toString());
    }
    v.leaveEnclosingElement();
    v.visit(ImageElement.DELEGATION_TOKEN_SEQUENCE_NUMBER, in.readInt());
    int numDTokens = in.readInt();
    v.visitEnclosingElement(ImageElement.DELEGATION_TOKENS,
        ImageElement.NUM_DELEGATION_TOKENS, numDTokens);
    for(int i=0; i<numDTokens; i++){
      DelegationTokenIdentifier id = new  DelegationTokenIdentifier();
      id.readFields(in);
      long expiryTime = in.readLong();
      v.visitEnclosingElement(ImageElement.DELEGATION_TOKEN_IDENTIFIER);
      v.visit(ImageElement.DELEGATION_TOKEN_IDENTIFIER_KIND,
          id.getKind().toString());
      v.visit(ImageElement.DELEGATION_TOKEN_IDENTIFIER_SEQNO,
          id.getSequenceNumber());
      v.visit(ImageElement.DELEGATION_TOKEN_IDENTIFIER_OWNER,
          id.getOwner().toString());
      v.visit(ImageElement.DELEGATION_TOKEN_IDENTIFIER_RENEWER,
          id.getRenewer().toString());
      v.visit(ImageElement.DELEGATION_TOKEN_IDENTIFIER_REALUSER,
          id.getRealUser().toString());
      v.visit(ImageElement.DELEGATION_TOKEN_IDENTIFIER_ISSUE_DATE,
          id.getIssueDate());
      v.visit(ImageElement.DELEGATION_TOKEN_IDENTIFIER_MAX_DATE,
          id.getMaxDate());
      v.visit(ImageElement.DELEGATION_TOKEN_IDENTIFIER_EXPIRY_TIME,
          expiryTime);
      v.visit(ImageElement.DELEGATION_TOKEN_IDENTIFIER_MASTER_KEY_ID,
          id.getMasterKeyId());
      v.leaveEnclosingElement(); // DELEGATION_TOKEN_IDENTIFIER
    }
    v.leaveEnclosingElement(); // DELEGATION_TOKENS
  }

  /**
   * 函数级注释：处理FSImage中构建中inode（未完成写入的文件）区域
   * @param in FSImage输入流
   * @param v 访问器
   * @param skipBlocks 是否跳过块解析
   * @throws IOException IO异常
   */
  private void processINodesUC(DataInputStream in, ImageVisitor v,
      boolean skipBlocks) throws IOException {
    int numINUC = in.readInt();

    v.visitEnclosingElement(ImageElement.INODES_UNDER_CONSTRUCTION,
                           ImageElement.NUM_INODES_UNDER_CONSTRUCTION, numINUC);

    for(int i = 0; i < numINUC; i++) {
      v.visitEnclosingElement(ImageElement.INODE_UNDER_CONSTRUCTION);
      byte [] name = FSImageSerialization.readBytes(in);
      String n = new String(name, StandardCharsets.UTF_8);
      v.visit(ImageElement.INODE_PATH, n);
      
      if (NameNodeLayoutVersion.supports(Feature.ADD_INODE_ID, imageVersion)) {
        long inodeId = in.readLong();
        v.visit(ImageElement.INODE_ID, inodeId);
      }
      
      v.visit(ImageElement.REPLICATION, in.readShort());
      v.visit(ImageElement.MODIFICATION_TIME, formatDate(in.readLong()));

      v.visit(ImageElement.PREFERRED_BLOCK_SIZE, in.readLong());
      int numBlocks = in.readInt();
      processBlocks(in, v, numBlocks, skipBlocks);

      processPermission(in, v);
      v.visit(Image