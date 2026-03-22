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

import java.io.IOException;

/**
 * 离线fsimage查看器的访问者抽象基类，定义了遍历HDFS fsimage结构的访问接口
 * 具体实现类可以通过实现这些接口，对fsimage中的不同结构执行自定义处理逻辑
 */
abstract class ImageVisitor {

  /**
   * fsimage文件中可被访问的结构元素枚举，定义了fsimage中所有可能出现的数据结构类型
   */
  public enum ImageElement {
    FS_IMAGE,
    IMAGE_VERSION,
    NAMESPACE_ID,
    IS_COMPRESSED,
    COMPRESS_CODEC,
    LAYOUT_VERSION,
    NUM_INODES,
    GENERATION_STAMP,
    GENERATION_STAMP_V2,
    GENERATION_STAMP_V1_LIMIT,
    LAST_ALLOCATED_BLOCK_ID,
    INODES,
    INODE,
    INODE_PATH,
    REPLICATION,
    MODIFICATION_TIME,
    ACCESS_TIME,
    BLOCK_SIZE,
    NUM_BLOCKS,
    BLOCKS,
    BLOCK,
    BLOCK_ID,
    NUM_BYTES,
    NS_QUOTA,
    DS_QUOTA,
    PERMISSIONS,
    SYMLINK,
    NUM_INODES_UNDER_CONSTRUCTION,
    INODES_UNDER_CONSTRUCTION,
    INODE_UNDER_CONSTRUCTION,
    PREFERRED_BLOCK_SIZE,
    CLIENT_NAME,
    CLIENT_MACHINE,
    USER_NAME,
    GROUP_NAME,
    PERMISSION_STRING,
    CURRENT_DELEGATION_KEY_ID,
    NUM_DELEGATION_KEYS,
    DELEGATION_KEYS,
    DELEGATION_KEY,
    DELEGATION_TOKEN_SEQUENCE_NUMBER,
    NUM_DELEGATION_TOKENS,
    DELEGATION_TOKENS,
    DELEGATION_TOKEN_IDENTIFIER,
    DELEGATION_TOKEN_IDENTIFIER_KIND,
    DELEGATION_TOKEN_IDENTIFIER_SEQNO,
    DELEGATION_TOKEN_IDENTIFIER_OWNER,
    DELEGATION_TOKEN_IDENTIFIER_RENEWER,
    DELEGATION_TOKEN_IDENTIFIER_REALUSER,
    DELEGATION_TOKEN_IDENTIFIER_ISSUE_DATE,
    DELEGATION_TOKEN_IDENTIFIER_MAX_DATE,
    DELEGATION_TOKEN_IDENTIFIER_EXPIRY_TIME,
    DELEGATION_TOKEN_IDENTIFIER_MASTER_KEY_ID,
    TRANSACTION_ID,
    LAST_INODE_ID,
    INODE_ID,

    SNAPSHOT_COUNTER,
    NUM_SNAPSHOTS_TOTAL,
    NUM_SNAPSHOTS,
    SNAPSHOTS,
    SNAPSHOT,
    SNAPSHOT_ID,
    SNAPSHOT_ROOT,
    SNAPSHOT_QUOTA,
    NUM_SNAPSHOT_DIR_DIFF,
    SNAPSHOT_DIR_DIFFS,
    SNAPSHOT_DIR_DIFF,
    SNAPSHOT_DIFF_SNAPSHOTID,
    SNAPSHOT_DIR_DIFF_CHILDREN_SIZE,
    SNAPSHOT_INODE_FILE_ATTRIBUTES,
    SNAPSHOT_INODE_DIRECTORY_ATTRIBUTES,
    SNAPSHOT_DIR_DIFF_CREATEDLIST,
    SNAPSHOT_DIR_DIFF_CREATEDLIST_SIZE,
    SNAPSHOT_DIR_DIFF_CREATED_INODE,
    SNAPSHOT_DIR_DIFF_DELETEDLIST,
    SNAPSHOT_DIR_DIFF_DELETEDLIST_SIZE,
    SNAPSHOT_DIR_DIFF_DELETED_INODE,
    IS_SNAPSHOTTABLE_DIR,
    IS_WITHSNAPSHOT_DIR,
    SNAPSHOT_FILE_DIFFS,
    SNAPSHOT_FILE_DIFF,
    NUM_SNAPSHOT_FILE_DIFF,
    SNAPSHOT_FILE_SIZE,
    SNAPSHOT_DST_SNAPSHOT_ID,
    SNAPSHOT_LAST_SNAPSHOT_ID,
    SNAPSHOT_REF_INODE_ID,
    SNAPSHOT_REF_INODE,

    CACHE_NEXT_ENTRY_ID,
    CACHE_NUM_POOLS,
    CACHE_POOL_NAME,
    CACHE_POOL_OWNER_NAME,
    CACHE_POOL_GROUP_NAME,
    CACHE_POOL_PERMISSION_STRING,
    CACHE_POOL_WEIGHT,
    CACHE_NUM_ENTRIES,
    CACHE_ENTRY_PATH,
    CACHE_ENTRY_REPLICATION,
    CACHE_ENTRY_POOL_NAME
  }
  
  /**
   * 开始遍历fsimage结构，供具体实现类完成初始化工作
   * @throws IOException 初始化过程中可能出现IO异常
   */
  abstract void start() throws IOException;

  /**
   * 完成正常遍历fsimage结构，供具体实现类完成清理工作
   * @throws IOException 清理过程中可能出现IO异常
   */
  abstract void finish() throws IOException;

  /**
   * 遍历过程发生异常后结束访问，供具体实现类完成异常清理工作
   * @throws IOException 清理过程中可能出现IO异常
   */
  abstract void finishAbnormally() throws IOException;

  /**
   * 访问fsimage中不包含子元素的叶子节点元素
   * @param element 要访问的fsimage元素类型
   * @param value 元素的值，字符串形式
   * @throws IOException 访问过程中可能出现IO异常
   */
  abstract void visit(ImageElement element, String value) throws IOException;

  // 数值类型的便捷访问方法，自动转换为字符串后调用通用visit
  void visit(ImageElement element, int value) throws IOException {
    visit(element, Integer.toString(value));
  }

  void visit(ImageElement element, long value) throws IOException {
    visit(element, Long.toString(value));
  }

  /**
   * 开始访问包含子元素的容器元素（例如文件的块列表）
   * @param element 要访问的容器元素类型
   * @throws IOException 访问过程中可能出现IO异常
   */
  abstract void visitEnclosingElement(ImageElement element)
     throws IOException;

  /**
   * 开始访问包含子元素的容器元素，并附带额外的键值对信息（例如容器内元素数量）
   * @param element 要访问的容器元素类型
   * @param key 附加信息的键
   * @param value 附加信息的值，字符串形式
   * @throws IOException 访问过程中可能出现IO异常
   */
  abstract void visitEnclosingElement(ImageElement element,
      ImageElement key, String value) throws IOException;

  // 数值类型的便捷容器访问方法，自动转换为字符串后调用通用方法
  void visitEnclosingElement(ImageElement element,
      ImageElement key, int value)
     throws IOException {
    visitEnclosingElement(element, key, Integer.toString(value));
  }

  void visitEnclosingElement(ImageElement element,
      ImageElement key, long value)
     throws IOException {
    visitEnclosingElement(element, key, Long.toString(value));
  }

  /**
   * 离开当前容器元素，在完成容器内所有子元素处理后调用，例如处理完文件的所有块后调用
   * @throws IOException 离开容器过程中可能出现IO异常
   */
  abstract void leaveEnclosingElement() throws IOException;
}