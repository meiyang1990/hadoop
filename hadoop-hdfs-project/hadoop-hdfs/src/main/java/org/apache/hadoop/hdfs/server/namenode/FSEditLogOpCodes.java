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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.*;

/**
 * HDFS编辑日志操作码枚举，定义了编辑日志中所有支持的文件系统操作类型
 * 每个操作码对应一种命名空间修改操作，用于编辑日志的读写和回放
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum FSEditLogOpCodes {
  // 新增文件操作
  OP_ADD                        ((byte)  0, AddOp.class),
  // 已废弃的旧版重命名操作
  OP_RENAME_OLD                 ((byte)  1, RenameOldOp.class),
  // 删除文件/目录操作
  OP_DELETE                     ((byte)  2, DeleteOp.class),
  // 创建目录操作
  OP_MKDIR                      ((byte)  3, MkdirOp.class),
  // 设置文件副本数操作
  OP_SET_REPLICATION            ((byte)  4, SetReplicationOp.class),
  @Deprecated OP_DATANODE_ADD   ((byte)  5), // 已废弃，不再使用
  @Deprecated OP_DATANODE_REMOVE((byte)  6), // 已废弃，不再使用
  // 设置权限操作
  OP_SET_PERMISSIONS            ((byte)  7, SetPermissionsOp.class),
  // 设置所有者操作
  OP_SET_OWNER                  ((byte)  8, SetOwnerOp.class),
  // 关闭文件操作
  OP_CLOSE                      ((byte)  9, CloseOp.class),
  // V1版本设置生成 stamps操作，已被V2取代
  OP_SET_GENSTAMP_V1            ((byte) 10, SetGenstampV1Op.class),
  OP_SET_NS_QUOTA               ((byte) 11, SetNSQuotaOp.class), // 已废弃
  OP_CLEAR_NS_QUOTA             ((byte) 12, ClearNSQuotaOp.class), // 已废弃
  // 设置访问时间和修改时间操作
  OP_TIMES                      ((byte) 13, TimesOp.class), // set atime, mtime
  // 设置配额操作
  OP_SET_QUOTA                  ((byte) 14, SetQuotaOp.class),
  // 文件上下文重命名操作
  OP_RENAME                     ((byte) 15, RenameOp.class),
  // 文件合并后删除原文件操作
  OP_CONCAT_DELETE              ((byte) 16, ConcatDeleteOp.class),
  // 创建符号链接操作
  OP_SYMLINK                    ((byte) 17, SymlinkOp.class),
  // 获取代理令牌操作
  OP_GET_DELEGATION_TOKEN       ((byte) 18, GetDelegationTokenOp.class),
  // 更新代理令牌有效期操作
  OP_RENEW_DELEGATION_TOKEN     ((byte) 19, RenewDelegationTokenOp.class),
  // 取消代理令牌操作
  OP_CANCEL_DELEGATION_TOKEN    ((byte) 20, CancelDelegationTokenOp.class),
  // 更新主密钥操作
  OP_UPDATE_MASTER_KEY          ((byte) 21, UpdateMasterKeyOp.class),
  // 重新分配租约操作
  OP_REASSIGN_LEASE             ((byte) 22, ReassignLeaseOp.class),
  // 结束日志分段操作
  OP_END_LOG_SEGMENT            ((byte) 23, EndLogSegmentOp.class),
  // 开始日志分段操作
  OP_START_LOG_SEGMENT          ((byte) 24, StartLogSegmentOp.class),
  // 更新块信息操作
  OP_UPDATE_BLOCKS              ((byte) 25, UpdateBlocksOp.class),
  // 创建快照操作
  OP_CREATE_SNAPSHOT            ((byte) 26, CreateSnapshotOp.class),
  // 删除快照操作
  OP_DELETE_SNAPSHOT            ((byte) 27, DeleteSnapshotOp.class),
  // 重命名快照操作
  OP_RENAME_SNAPSHOT            ((byte) 28, RenameSnapshotOp.class),
  // 允许目录创建快照操作
  OP_ALLOW_SNAPSHOT             ((byte) 29, AllowSnapshotOp.class),
  // 禁止目录创建快照操作
  OP_DISALLOW_SNAPSHOT          ((byte) 30, DisallowSnapshotOp.class),
  // V2版本设置生成 stamps操作
  OP_SET_GENSTAMP_V2            ((byte) 31, SetGenstampV2Op.class),
  // 分配块ID操作
  OP_ALLOCATE_BLOCK_ID          ((byte) 32, AllocateBlockIdOp.class),
  // 新增块操作
  OP_ADD_BLOCK                  ((byte) 33, AddBlockOp.class),
  // 添加缓存指令操作
  OP_ADD_CACHE_DIRECTIVE        ((byte) 34, AddCacheDirectiveInfoOp.class),
  // 移除缓存指令操作
  OP_REMOVE_CACHE_DIRECTIVE     ((byte) 35, RemoveCacheDirectiveInfoOp.class),
  // 添加缓存池操作
  OP_ADD_CACHE_POOL             ((byte) 36, AddCachePoolOp.class),
  // 修改缓存池操作
  OP_MODIFY_CACHE_POOL          ((byte) 37, ModifyCachePoolOp.class),
  // 移除缓存池操作
  OP_REMOVE_CACHE_POOL          ((byte) 38, RemoveCachePoolOp.class),
  // 修改缓存指令操作
  OP_MODIFY_CACHE_DIRECTIVE     ((byte) 39, ModifyCacheDirectiveInfoOp.class),
  // 设置ACL权限操作
  OP_SET_ACL                    ((byte) 40, SetAclOp.class),
  // 开始滚动升级操作
  OP_ROLLING_UPGRADE_START      ((byte) 41, RollingUpgradeStartOp.class),
  // 完成滚动升级操作
  OP_ROLLING_UPGRADE_FINALIZE   ((byte) 42, RollingUpgradeFinalizeOp.class),
  // 设置扩展属性操作
  OP_SET_XATTR                  ((byte) 43, SetXAttrOp.class),
  // 移除扩展属性操作
  OP_REMOVE_XATTR               ((byte) 44, RemoveXAttrOp.class),
  // 设置存储策略操作
  OP_SET_STORAGE_POLICY         ((byte) 45, SetStoragePolicyOp.class),
  // 截断文件操作
  OP_TRUNCATE                   ((byte) 46, TruncateOp.class),
  // 追加文件操作
  OP_APPEND                     ((byte) 47, AppendOp.class),
  // 按存储类型设置配额操作
  OP_SET_QUOTA_BY_STORAGETYPE   ((byte) 48, SetQuotaByStorageTypeOp.class),
  // 添加纠删码策略操作
  OP_ADD_ERASURE_CODING_POLICY  ((byte) 49, AddErasureCodingPolicyOp.class),
  // 启用纠删码策略操作
  OP_ENABLE_ERASURE_CODING_POLICY((byte) 50, EnableErasureCodingPolicyOp.class),
  // 禁用纠删码策略操作
  OP_DISABLE_ERASURE_CODING_POLICY((byte) 51,
      DisableErasureCodingPolicyOp.class),
  // 移除纠删码策略操作
  OP_REMOVE_ERASURE_CODING_POLICY((byte) 52, RemoveErasureCodingPolicyOp.class),

  // 注意：合法操作码范围为0~127
  OP_INVALID                    ((byte) -1);

  private final byte opCode;
  private final Class<? extends FSEditLogOp> opClass;

  /**
   * 构造没有对应操作类的枚举实例，用于废弃操作
   * @param opCode 操作码字节值
   */
  FSEditLogOpCodes(byte opCode) {
    this(opCode, null);
  }

  /**
   * 构造完整枚举实例，绑定操作码和对应操作类
   * @param opCode 操作码字节值
   * @param opClass 对应编辑日志操作类
   */
  FSEditLogOpCodes(byte opCode, Class<? extends FSEditLogOp> opClass) {
    this.opCode = opCode;
    this.opClass = opClass;
  }

  /**
   * 获取操作码的字节值
   * @return 操作码字节值
   */
  public byte getOpCode() {
    return opCode;
  }

  /**
   * 获取当前操作码对应的编辑日志操作类
   * @return 操作类的Class对象
   */
  public Class<? extends FSEditLogOp> getOpClass() {
    return opClass;
  }

  private static final FSEditLogOpCodes[] VALUES;
  
  static {
    // 查找最大操作码字节值
    byte max = 0;
    for (FSEditLogOpCodes code : FSEditLogOpCodes.values()) {
      if (code.getOpCode() > max) {
        max = code.getOpCode();
      }
    }
    // 初始化按操作码索引的查找数组
    VALUES = new FSEditLogOpCodes[max + 1];
    for (FSEditLogOpCodes code : FSEditLogOpCodes.values()) {
      if (code.getOpCode() >= 0) {
        VALUES[code.getOpCode()] = code;
      }
    }
  }

  /**
   * 根据字节操作码转换为对应的枚举实例
   * @param opCode 字节形式的操作码
   * @return 对应操作码的枚举实例，无效操作码返回null，-1返回OP_INVALID
   */
  public static FSEditLogOpCodes fromByte(byte opCode) {
    if (opCode >= 0 && opCode < VALUES.length) {
      return VALUES[opCode];
    }
    return opCode == -1 ? OP_INVALID : null;
  }
}