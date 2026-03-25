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
package org.apache.hadoop.hdfs.server.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.MetaRecoveryContext;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.util.StringUtils;

/**
 * HDFS服务端内部通用常量接口，定义了HDFS核心服务端运行所需的各类常量、枚举类型
 * 包含节点类型、启动选项、副本状态、块状态等核心服务端定义
 */
/************************************
 * Some handy internal HDFS constants
 *
 ************************************/

@InterfaceAudience.Private
public interface HdfsServerConstants {
  // Will be set by
  // {@code DFSConfigKeys.DFS_NAMENODE_BLOCKPLACEMENTPOLICY_MIN_BLOCKS_FOR_WRITE_KEY}.
  int MIN_BLOCKS_FOR_WRITE = 1;

  long LEASE_RECOVER_PERIOD = 10 * 1000; // in ms
  // We need to limit the length and depth of a path in the filesystem.
  // HADOOP-438
  // Currently we set the maximum length to 8k characters and the maximum depth
  // to 1k.
  int MAX_PATH_LENGTH = 8000;
  int MAX_PATH_DEPTH = 1000;
  // An invalid transaction ID that will never be seen in a real namesystem.
  long INVALID_TXID = -12345;
  // Number of generation stamps reserved for legacy blocks.
  long RESERVED_LEGACY_GENERATION_STAMPS = 1024L * 1024 * 1024 * 1024;
  /**
   * Current layout version for NameNode.
   * Please see {@link NameNodeLayoutVersion.Feature} on adding new layout version.
   */
  int NAMENODE_LAYOUT_VERSION
      = NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION;
  /**
  * Current minimum compatible version for NameNode
  * Please see {@link NameNodeLayoutVersion.Feature} on adding new layout version.
  */
  int MINIMUM_COMPATIBLE_NAMENODE_LAYOUT_VERSION
      = NameNodeLayoutVersion.MINIMUM_COMPATIBLE_LAYOUT_VERSION;
  /**
   * Path components that are reserved in HDFS.
   * <p>
   * .reserved is only reserved under root ("/").
   */
  String[] RESERVED_PATH_COMPONENTS = new String[] {
      HdfsConstants.DOT_SNAPSHOT_DIR,
      FSDirectory.DOT_RESERVED_STRING
  };
  byte[] DOT_SNAPSHOT_DIR_BYTES
              = DFSUtil.string2Bytes(HdfsConstants.DOT_SNAPSHOT_DIR);

  /**
   * HDFS节点类型枚举，定义集群中不同类型节点
   */
  enum NodeType {
    /** NameNode节点，负责元数据管理 */
    NAME_NODE,
    /** DataNode节点，负责实际数据存储 */
    DATA_NODE,
    /** JournalNode节点，负责共享编辑日志存储 */
    JOURNAL_NODE
  }

  /**
   * 滚动升级启动选项枚举，定义滚动升级过程中的可选操作
   */
  enum RollingUpgradeStartupOption{
    /** 回滚滚动升级 */
    ROLLBACK,
    /** 滚动升级已启动 */
    STARTED;

    /**
     * 获取启动参数字符串格式
     * @return 格式化后的启动选项字符串
     */
    public String getOptionString() {
      return StartupOption.ROLLINGUPGRADE.getName() + " "
          + StringUtils.toLowerCase(name());
    }

    /**
     * 检查当前选项是否匹配给定启动选项
     * @param option 待匹配的启动选项
     * @return 是否匹配
     */
    public boolean matches(StartupOption option) {
      return option == StartupOption.ROLLINGUPGRADE
          && option.getRollingUpgradeStartupOption() == this;
    }

    private static final RollingUpgradeStartupOption[] VALUES = values();

    /**
     * 从字符串解析得到滚动升级启动选项
     * @param s 输入字符串
     * @return 解析后的滚动升级启动选项
     * @throws IllegalArgumentException 当解析失败或传入不支持的选项时抛出
     */
    static RollingUpgradeStartupOption fromString(String s) {
      if ("downgrade".equalsIgnoreCase(s)) {
        throw new IllegalArgumentException(
            "The \"downgrade\" option is no longer supported"
                + " since it may incorrectly finalize an ongoing rolling upgrade."
                + " For downgrade instruction, please see the documentation"
                + " (http://hadoop.apache.org/docs/current/hadoop-project-dist/"
                + "hadoop-hdfs/HdfsRollingUpgrade.html#Downgrade).");
      }
      for(RollingUpgradeStartupOption opt : VALUES) {
        if (opt.name().equalsIgnoreCase(s)) {
          return opt;
        }
      }
      throw new IllegalArgumentException("Failed to convert \"" + s
          + "\" to " + RollingUpgradeStartupOption.class.getSimpleName());
    }

    /**
     * 获取所有支持选项的字符串表示
     * @return 所有选项拼接后的格式字符串
     */
    public static String getAllOptionString() {
      final StringBuilder b = new StringBuilder("<");
      for(RollingUpgradeStartupOption opt : VALUES) {
        b.append(StringUtils.toLowerCase(opt.name())).append("|");
      }
      b.setCharAt(b.length() - 1, '>');
      return b.toString();
    }
  }

  /**
   * NameNode启动选项枚举，定义了NameNode支持的所有启动模式与参数
   */
  enum StartupOption{
    /** 格式化NameNode */
    FORMAT  ("-format"),
    /** 指定集群ID */
    CLUSTERID ("-clusterid"),
    /** 生成新的集群ID */
    GENCLUSTERID ("-genclusterid"),
    /** 常规启动 */
    REGULAR ("-regular"),
    /** 备份节点启动 */
    BACKUP  ("backup"),
    /** 检查点节点启动 */
    CHECKPOINT("-checkpoint"),
    /** 常规升级启动 */
    UPGRADE ("-upgrade"),
    /** 回滚上一次升级 */
    ROLLBACK("-rollback"),
    /** 滚动升级启动 */
    ROLLINGUPGRADE("-rollingUpgrade"),
    /** 从检查点导入元数据 */
    IMPORT  ("-importCheckpoint"),
    /** 引导备用节点元数据 */
    BOOTSTRAPSTANDBY("-bootstrapStandby"),
    /** 初始化共享编辑日志 */
    INITIALIZESHAREDEDITS("-initializeSharedEdits"),
    /** 恢复元数据 */
    RECOVER  ("-recover"),
    /** 强制操作 */
    FORCE("-force"),
    /** 非交互模式 */
    NONINTERACTIVE("-nonInteractive"),
    /** 跳过共享编辑日志检查 */
    SKIPSHAREDEDITSCHECK("-skipSharedEditsCheck"),
    /** 重命名保留路径组件 */
    RENAMERESERVED("-renameReserved"),
    /** 查看元数据版本 */
    METADATAVERSION("-metadataVersion"),
    /** 仅执行升级 */
    UPGRADEONLY("-upgradeOnly"),
    // The -hotswap constant should not be used as a startup option, it is
    // only used for StorageDirectory.analyzeStorage() in hot swap drive scenario.
    // TODO refactor StorageDirectory.analyzeStorage() so that we can do away with
    // this in StartupOption.
    /** 热交换驱动器，仅内部使用 */
    HOTSWAP("-hotswap"),
    /** 观察者模式启动NameNode */
    OBSERVER("-observer");

    /** 匹配带滚动升级选项的枚举正则表达式，格式为枚举名(选项名) */
    private static final Pattern ENUM_WITH_ROLLING_UPGRADE_OPTION = Pattern.compile(
        "(\\w+)\\((\\w+)\\)");

    /** 命令行参数名称 */
    private final String name;
    
    // 仅在format和upgrade选项时使用，存储集群ID
    private String clusterId = null;
    
    // 仅滚动升级时使用，存储滚动升级子选项
    private RollingUpgradeStartupOption rollingUpgradeStartupOption;

    // 仅format选项时使用，是否强制格式化
    private boolean isForceFormat = false;
    private boolean isInteractiveFormat = true;
    
    // 仅恢复选项时使用，强制级别
    private int force = 0;

    StartupOption(String arg) {this.name = arg;}

    /**
     * 获取启动选项对应的命令行参数名
     * @return 命令行参数字符串
     */
    public String getName() {return name;}

    /**
     * 将启动选项转换为对应NameNode角色
     * @return 对应的NameNode角色枚举
     */
    public NamenodeRole toNodeRole() {
      switch(this) {
      case BACKUP: 
        return NamenodeRole.BACKUP;
      case CHECKPOINT: 
        return NamenodeRole.CHECKPOINT;
      default:
        return NamenodeRole.NAMENODE;
      }
    }
    
    /**
     * 设置集群ID
     * @param cid 集群ID字符串
     */
    public void setClusterId(String cid) {
      clusterId = cid;
    }

    /**
     * 获取设置的集群ID
     * @return 集群ID字符串
     */
    public String getClusterId() {
      return clusterId;
    }
    
    /**
     * 设置滚动升级子选项
     * @param opt 滚动升级选项字符串
     */
    public void setRollingUpgradeStartupOption(String opt) {
      Preconditions.checkState(this == ROLLINGUPGRADE);
      rollingUpgradeStartupOption = RollingUpgradeStartupOption.fromString(opt);
    }
    
    /**
     * 获取滚动升级子选项
     * @return 滚动升级启动选项枚举
     */
    public RollingUpgradeStartupOption getRollingUpgradeStartupOption() {
      Preconditions.checkState(this == ROLLINGUPGRADE);
      return rollingUpgradeStartupOption;
    }

    /**
     * 创建元数据恢复上下文对象
     * @return 元数据恢复上下文，非恢复选项返回null
     */
    public MetaRecoveryContext createRecoveryContext() {
      if (!name.equals(RECOVER.name))
        return null;
      return new MetaRecoveryContext(force);
    }

    /**
     * 设置恢复强制级别
     * @param force 强制级别
     */
    public void setForce(int force) {
      this.force = force;
    }
    
    /**
     * 获取恢复强制级别
     * @return 强制级别值
     */
    public int getForce() {
      return this.force;
    }
    
    /**
     * 获取是否强制格式化
     * @return 是否强制格式化
     */
    public boolean getForceFormat() {
      return isForceFormat;
    }
    
    /**
     * 设置是否强制格式化
     * @param force 是否强制
     */
    public void setForceFormat(boolean force) {
      isForceFormat = force;
    }
    
    /**
     * 获取是否交互式格式化
     * @return 是否交互式
     */
    public boolean getInteractiveFormat() {
      return isInteractiveFormat;
    }
    
    /**
     * 设置是否交互式格式化
     * @param interactive 是否交互式
     */
    public void setInteractiveFormat(boolean interactive) {
      isInteractiveFormat = interactive;
    }
    
    @Override
    public String toString() {
      if (this == ROLLINGUPGRADE) {
        return new StringBuilder(super.toString())
            .append("(").append(getRollingUpgradeStartupOption()).append(")")
            .toString();
      }
      return super.toString();
    }

    /**
     * 从字符串解析得到启动选项枚举
     * @param value 输入字符串，支持带滚动升级选项的格式
     * @return 解析后的启动选项枚举
     * @throws IllegalArgumentException 当字符串无法解析时抛出
     */
    static public StartupOption getEnum(String value) {
      Matcher matcher = ENUM_WITH_ROLLING_UPGRADE_OPTION.matcher(value);
      if (matcher.matches()) {
        StartupOption option = StartupOption.valueOf(matcher.group(1));
        option.setRollingUpgradeStartupOption(matcher.group(2));
        return option;
      } else {
        return StartupOption.valueOf(value);
      }
    }
  }

  /**
   * NameNode角色枚举，定义不同类型的NameNode节点角色
   */
  enum NamenodeRole {
    /** 主NameNode */
    NAMENODE  ("NameNode"),
    /** 备份节点 */
    BACKUP    ("Backup Node"),
    /** 检查点节点 */
    CHECKPOINT("Checkpoint Node");

    private String description = null;
    NamenodeRole(String arg) {this.description = arg;}
  
    @Override
    public String toString() {
      return description;
    }
  }

  /**
   * 块副本状态枚举，定义了副本在构建过程中的各个状态
   */
  enum ReplicaState {
    /** 副本已完成，不再被修改 */
    FINALIZED(0),
    /** 副本正在被写入 */
    RBW(1),
    /** 副本等待恢复 */
    RWR(2),
    /** 副本正在恢复中 */
    RUR(3),
    /** 临时副本，仅用于复制和数据迁移 */
    TEMPORARY(4);

    // Since ReplicaState (de)serialization depends on ordinal, either adding
    // new value should be avoided to this enum or newly appended value should
    // be handled by NameNodeLayoutVersion#Feature.

    private static final ReplicaState[] cachedValues = ReplicaState.values();

    private final int value;

    ReplicaState(int v) {
      value = v;
    }

    /**
     * 获取状态对应的数值编码
     * @return 数值编码
     */
    public int getValue() {
      return value;
    }

    /**
     * 根据数值索引获取对应的副本状态
     * @param v 输入索引值
     * @return 对应的副本状态
     * @throws IndexOutOfBoundsException 索引超出范围时抛出
     */
    public static ReplicaState getState(int v) {
      Validate.validIndex(cachedValues, v, "Index Expected range: [0, "
          + (cachedValues.length - 1) + "]. Actual value: " + v);
      return cachedValues[v];
    }

    /**
     * 从二进制输入流读取并解析副本状态
     * @param in 二进制输入流
     * @return 解析得到的副本状态
     * @throws IOException 读取IO异常时抛出
     * @throws IndexOutOfBoundsException 读取的索引无效时抛出
     */
    public static ReplicaState read(DataInput in) throws IOException {
      byte idx = in.readByte();
      Validate.validIndex(cachedValues, idx, "Index Expected range: [0, "
          + (cachedValues.length - 1) + "]. Actual value: " + idx);
      return cachedValues[idx];
    }

    /**
     * 将当前副本状态写入二进制输出流
     * @param out 二进制输出流
     * @throws IOException 写入IO异常时抛出
     */
    public void write(DataOutput out) throws IOException {
      out.writeByte(ordinal());
    }
  }

  /**
   * 正在构建的块状态枚举，定义了块在构建过程中的各个状态
   */
  enum BlockUCState {
    /**
     * 块构建完成，块已达到最小副本数要求，不再被修改
     */
    COMPLETE,
    /**
     * 块正在构建中，刚为写操作分配完成
     */
    UNDER_CONSTRUCTION,
    /**
     * 块正在恢复中，租约到期后需要对未完成块进行恢复同步
     */
    UNDER_RECOVERY,
    /**
     * 块已提交，客户端已完成写入并确认，但DataNode还未上报完成的 finalized 副本
     */
    COMMITTED
  }
  
  /** NameNode租约持有者名称 */
  String NAMENODE_LEASE_HOLDER = "HDFS_NameNode";

  /** 加密区扩展属性名称 */
  String CRYPTO_XATTR_ENCRYPTION_ZONE =
      "raw.hdfs.crypto.encryption.