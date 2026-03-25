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

import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_ADD;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_ADD_ERASURE_CODING_POLICY;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_APPEND;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_ADD_BLOCK;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_ADD_CACHE_DIRECTIVE;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_ADD_CACHE_POOL;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_ALLOCATE_BLOCK_ID;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_ALLOW_SNAPSHOT;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_CANCEL_DELEGATION_TOKEN;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_CLEAR_NS_QUOTA;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_CLOSE;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_CONCAT_DELETE;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_CREATE_SNAPSHOT;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_DELETE;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_DELETE_SNAPSHOT;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_DISABLE_ERASURE_CODING_POLICY;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_DISALLOW_SNAPSHOT;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_ENABLE_ERASURE_CODING_POLICY;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_END_LOG_SEGMENT;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_GET_DELEGATION_TOKEN;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_INVALID;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_MKDIR;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_MODIFY_CACHE_DIRECTIVE;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_MODIFY_CACHE_POOL;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_REASSIGN_LEASE;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_REMOVE_CACHE_DIRECTIVE;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_REMOVE_CACHE_POOL;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_REMOVE_ERASURE_CODING_POLICY;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_REMOVE_XATTR;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_RENAME;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_RENAME_OLD;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_RENAME_SNAPSHOT;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_RENEW_DELEGATION_TOKEN;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_ROLLING_UPGRADE_FINALIZE;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_ROLLING_UPGRADE_START;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SET_ACL;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SET_GENSTAMP_V1;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SET_GENSTAMP_V2;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SET_NS_QUOTA;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SET_OWNER;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SET_PERMISSIONS;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SET_QUOTA;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SET_REPLICATION;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SET_XATTR;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_START_LOG_SEGMENT;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SYMLINK;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_TIMES;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_TRUNCATE;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_UPDATE_BLOCKS;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_UPDATE_MASTER_KEY;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SET_STORAGE_POLICY;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.OP_SET_QUOTA_BY_STORAGETYPE;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DeprecatedUTF8;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.protocol.proto.EditLogProtos.AclEditLogProto;
import org.apache.hadoop.hdfs.protocol.proto.EditLogProtos.XAttrEditLogProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.util.XMLUtils;
import org.apache.hadoop.hdfs.util.XMLUtils.InvalidXmlException;
import org.apache.hadoop.hdfs.util.XMLUtils.Stanza;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.ipc.ClientId;
import org.apache.hadoop.ipc.RpcConstants;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.StringUtils;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * HDFS编辑日志操作基类，定义了所有编辑日志操作的通用结构和方法，每个具体文件系统操作对应一个子类。
 * 所有操作实例都只能通过Reader#readOp()方法创建，用于记录NameNode上的文件系统元数据变更。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class FSEditLogOp {
  /** 当前操作对应的操作码 */
  public final FSEditLogOpCodes opCode;
  /** 当前事务ID */
  long txid;
  /** RPC调用客户端ID，用于幂等性重试 */
  byte[] rpcClientId;
  /** RPC调用ID，用于幂等性重试 */
  int rpcCallId;

  /**
   * 线程本地操作实例缓存，复用操作对象减少对象创建开销
   */
  public static class OpInstanceCache {
    /** 线程本地缓存，每个线程维护一个操作码到操作实例的映射 */
    private static final ThreadLocal<OpInstanceCacheMap> CACHE =
        new ThreadLocal<OpInstanceCacheMap>() {
      @Override
      protected OpInstanceCacheMap initialValue() {
        return new OpInstanceCacheMap();
      }
    };

    @SuppressWarnings("serial")
    /** 操作实例缓存映射，按操作码存储预创建的操作实例 */
    static final class OpInstanceCacheMap extends
        EnumMap<FSEditLogOpCodes, FSEditLogOp> {
      /** 构造函数，为每个操作码预创建实例 */
      OpInstanceCacheMap() {
        super(FSEditLogOpCodes.class);
        for (FSEditLogOpCodes opCode : FSEditLogOpCodes.values()) {
          put(opCode, newInstance(opCode));
        }
      }
    }

    /** 是否使用缓存，默认开启 */
    private boolean useCache = true;

    /** 禁用实例缓存，每次获取都创建新实例 */
    void disableCache() {
      useCache = false;
    }

    /** 获取当前缓存实例 */
    public OpInstanceCache get() {
      return this;
    }

    /**
     * 根据操作码获取操作实例，优先从缓存获取，否则新建
     * @param opCode 操作码
     * @return 操作实例
     */
    @SuppressWarnings("unchecked")
    public <T extends FSEditLogOp> T get(FSEditLogOpCodes opCode) {
      return useCache ? (T)CACHE.get().get(opCode) : (T)newInstance(opCode);
    }

    /**
     * 根据操作码创建新的操作实例
     * @param opCode 操作码
     * @return 新操作实例
     */
    private static FSEditLogOp newInstance(FSEditLogOpCodes opCode) {
      FSEditLogOp instance = null;
      Class<? extends FSEditLogOp> clazz = opCode.getOpClass();
      if (clazz != null) {
        try {
          instance = clazz.newInstance();
        } catch (Exception ex) {
          throw new RuntimeException("Failed to instantiate "+opCode, ex);
        }
      }
      return instance;
    }
  }

  /** 重置操作的公共字段到初始状态，以便复用对象 */
  final void reset() {
    txid = HdfsServerConstants.INVALID_TXID;
    rpcClientId = RpcConstants.DUMMY_CLIENT_ID;
    rpcCallId = RpcConstants.INVALID_CALL_ID;
    resetSubFields();
  }

  /** 重置子类特有字段，由子类实现 */
  abstract void resetSubFields();

  /** 构建FsAction符号到枚举实例的映射表 */
  private static ImmutableMap<String, FsAction> fsActionMap() {
    ImmutableMap.Builder<String, FsAction> b = ImmutableMap.builder();
    for (FsAction v : FsAction.values())
      b.put(v.SYMBOL, v);
    return b.build();
  }

  /** FsAction符号到枚举实例的全局映射 */
  private static final ImmutableMap<String, FsAction> FSACTION_SYMBOL_MAP
    = fsActionMap();

  /**
   * 构造编辑日志操作，仅允许子类调用，只能通过Reader#readOp实例化
   * @param opCode 操作码
   */
  @VisibleForTesting
  protected FSEditLogOp(FSEditLogOpCodes opCode) {
    this.opCode = opCode;
    reset();
  }

  /**
   * 获取当前操作的事务ID
   * @return 事务ID
   */
  public long getTransactionId() {
    Preconditions.checkState(txid != HdfsServerConstants.INVALID_TXID);
    return txid;
  }

  /**
   * 获取事务ID的字符串表示，用于日志输出
   * @return 事务ID字符串
   */
  public String getTransactionIdStr() {
    return (txid == HdfsServerConstants.INVALID_TXID) ? "(none)" : "" + txid;
  }
  
  /**
   * 判断当前操作是否有有效的事务ID
   * @return true如果有有效事务ID，否则false
   */
  public boolean hasTransactionId() {
    return (txid != HdfsServerConstants.INVALID_TXID);
  }

  /**
   * 设置当前操作的事务ID
   * @param txid 事务ID
   */
  public void setTransactionId(long txid) {
    this.txid = txid;
  }
  
  /**
   * 判断当前操作是否包含有效的RPC调用标识（用于重试幂等性）
   * @return true如果有有效RPC标识，否则false
   */
  public boolean hasRpcIds() {
    return rpcClientId != RpcConstants.DUMMY_CLIENT_ID
        && rpcCallId != RpcConstants.INVALID_CALL_ID;
  }
  
  /**
   * 获取RPC客户端ID，调用前必须先调用hasRpcIds()确认有效
   * @return RPC客户端ID字节数组
   */
  public byte[] getClientId() {
    Preconditions.checkState(rpcClientId != RpcConstants.DUMMY_CLIENT_ID);
    return rpcClientId;
  }
  
  /**
   * 设置RPC客户端ID
   * @param clientId RPC客户端ID
   */
  public void setRpcClientId(byte[] clientId) {
    this.rpcClientId = clientId;
  }
  
  /**
   * 获取RPC调用ID，调用前必须先调用hasRpcIds()确认有效
   * @return RPC调用ID
   */
  public int getCallId() {
    Preconditions.checkState(rpcCallId != RpcConstants.INVALID_CALL_ID);
    return rpcCallId;
  }
  
  /**
   * 设置RPC调用ID
   * @param callId RPC调用ID
   */
  public void setRpcCallId(int callId) {
    this.rpcCallId = callId;
  }

  /**
   * 从输入流读取操作字段，由子类实现
   * @param in 输入流
   * @param logVersion 编辑日志版本
   * @throws IOException 读取异常
   */
  abstract void readFields(DataInputStream in, int logVersion)
      throws IOException;

  /**
   * 将操作字段写入输出流，由子类实现
   * @param out 输出流
   * @throws IOException 写入异常
   */
  public abstract void writeFields(DataOutputStream out)
      throws IOException;

  /**
   * 按指定日志版本写入操作字段
   * @param out 输出流
   * @param logVersion 日志版本