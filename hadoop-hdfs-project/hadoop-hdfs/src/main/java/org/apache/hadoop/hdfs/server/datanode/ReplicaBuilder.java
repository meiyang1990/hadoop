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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;

/**
 * HDFS DataNode块副本构造器，用于根据不同副本状态和存储类型创建对应类型的ReplicaInfo对象。
 * 封装了不同类型副本的构造逻辑，支持本地存储和外部提供存储（PROVIDED）的多种副本状态。
 */
public class ReplicaBuilder {

  private ReplicaState state;
  private long blockId;
  private long genStamp;
  private long length;
  private FsVolumeSpi volume;
  private File directoryUsed;
  private long bytesToReserve;
  private Thread writer;
  private long recoveryId;
  private Block block;
  private byte[] lastPartialChunkChecksum;

  private ReplicaInfo fromReplica;

  private URI uri;
  private long offset;
  private Configuration conf;
  private FileRegion fileRegion;
  private FileSystem remoteFS;
  private PathHandle pathHandle;
  private String pathSuffix;
  private Path pathPrefix;

  /**
   * 构造指定初始副本状态的副本构造器。
   * @param state 初始副本状态
   */
  public ReplicaBuilder(ReplicaState state) {
    volume = null;
    writer = null;
    block = null;
    length = -1;
    fileRegion = null;
    conf = null;
    fromReplica = null;
    uri = null;
    this.state = state;
    pathHandle = null;
  }

  /**
   * 设置副本状态，支持链式调用。
   * @param state 目标副本状态
   * @return 当前构造器实例
   */
  public ReplicaBuilder setState(ReplicaState state) {
    this.state = state;
    return this;
  }

  /**
   * 设置块ID，支持链式调用。
   * @param blockId 目标块ID
   * @return 当前构造器实例
   */
  public ReplicaBuilder setBlockId(long blockId) {
    this.blockId = blockId;
    return this;
  }

  /**
   * 设置块生成时间戳，支持链式调用。
   * @param genStamp 生成时间戳
   * @return 当前构造器实例
   */
  public ReplicaBuilder setGenerationStamp(long genStamp) {
    this.genStamp = genStamp;
    return this;
  }

  /**
   * 设置块长度，支持链式调用。
   * @param length 块长度
   * @return 当前构造器实例
   */
  public ReplicaBuilder setLength(long length) {
    this.length = length;
    return this;
  }

  /**
   * 设置副本所在存储卷，支持链式调用。
   * @param volume 目标存储卷
   * @return 当前构造器实例
   */
  public ReplicaBuilder setFsVolume(FsVolumeSpi volume) {
    this.volume = volume;
    return this;
  }

  /**
   * 设置副本存储目录，支持链式调用。
   * @param dir 目标存储目录
   * @return 当前构造器实例
   */
  public ReplicaBuilder setDirectoryToUse(File dir) {
    this.directoryUsed = dir;
    return this;
  }

  /**
   * 设置需要预留的字节数，支持链式调用。
   * @param bytesToReserve 预留字节数
   * @return 当前构造器实例
   */
  public ReplicaBuilder setBytesToReserve(long bytesToReserve) {
    this.bytesToReserve = bytesToReserve;
    return this;
  }

  /**
   * 设置写入该副本的线程，支持链式调用。
   * @param writer 写入线程
   * @return 当前构造器实例
   */
  public ReplicaBuilder setWriterThread(Thread writer) {
    this.writer = writer;
    return this;
  }

  /**
   * 设置源副本，用于基于已有副本构造新副本，支持链式调用。
   * @param fromReplica 源副本对象
   * @return 当前构造器实例
   */
  public ReplicaBuilder from(ReplicaInfo fromReplica) {
    this.fromReplica = fromReplica;
    return this;
  }

  /**
   * 设置恢复ID，用于副本恢复场景，支持链式调用。
   * @param recoveryId 恢复ID
   * @return 当前构造器实例
   */
  public ReplicaBuilder setRecoveryId(long recoveryId) {
    this.recoveryId = recoveryId;
    return this;
  }

  /**
   * 设置块对象，支持链式调用。
   * @param block 块对象
   * @return 当前构造器实例
   */
  public ReplicaBuilder setBlock(Block block) {
    this.block = block;
    return this;
  }

  /**
   * 设置外部存储URI，用于PROVIDED类型存储，支持链式调用。
   * @param uri 外部存储资源URI
   * @return 当前构造器实例
   */
  public ReplicaBuilder setURI(URI uri) {
    this.uri = uri;
    return this;
  }

  /**
   * 设置Hadoop配置对象，支持链式调用。
   * @param conf Hadoop配置
   * @return 当前构造器实例
   */
  public ReplicaBuilder setConf(Configuration conf) {
    this.conf = conf;
    return this;
  }

  /**
   * 设置数据偏移量，用于外部存储，支持链式调用。
   * @param offset 数据起始偏移量
   * @return 当前构造器实例
   */
  public ReplicaBuilder setOffset(long offset) {
    this.offset = offset;
    return this;
  }

  /**
   * 设置文件区域信息，用于PROVIDED类型存储，支持链式调用。
   * @param fileRegion 文件区域信息
   * @return 当前构造器实例
   */
  public ReplicaBuilder setFileRegion(FileRegion fileRegion) {
    this.fileRegion = fileRegion;
    return this;
  }

  /**
   * 设置远程文件系统，用于PROVIDED类型存储，支持链式调用。
   * @param remoteFS 远程文件系统对象
   * @return 当前构造器实例
   */
  public ReplicaBuilder setRemoteFS(FileSystem remoteFS) {
    this.remoteFS = remoteFS;
    return this;
  }

  /**
   * Set the suffix of the {@link Path} associated with the replica.
   * Intended to be use only for {@link ProvidedReplica}s.
   * @param suffix the path suffix.
   * @return the builder with the path suffix set.
   */
  public ReplicaBuilder setPathSuffix(String suffix) {
    this.pathSuffix = suffix;
    return this;
  }

  /**
   * Set the prefix of the {@link Path} associated with the replica.
   * Intended to be use only for {@link ProvidedReplica}s.
   * @param prefix the path prefix.
   * @return the builder with the path prefix set.
   */
  public ReplicaBuilder setPathPrefix(Path prefix) {
    this.pathPrefix = prefix;
    return this;
  }

  /**
   * 设置路径句柄，用于PROVIDED类型存储，支持链式调用。
   * @param pathHandle 路径句柄
   * @return 当前构造器实例
   */
  public ReplicaBuilder setPathHandle(PathHandle pathHandle) {
    this.pathHandle = pathHandle;
    return this;
  }

  /**
   * 设置最后一个不完整块的校验和，支持链式调用。
   * @param checksum 校验和字节数组
   * @return 当前构造器实例
   */
  public ReplicaBuilder setLastPartialChunkChecksum(byte[] checksum) {
    this.lastPartialChunkChecksum = checksum;
    return this;
  }

  /**
   * 构建管道中本地存储的副本对象，仅支持RBW和TEMPORARY状态。
   * @return 管道中本地副本对象
   * @throws IllegalArgumentException 当状态不支持时抛出异常
   */
  public LocalReplicaInPipeline buildLocalReplicaInPipeline()
      throws IllegalArgumentException {
    LocalReplicaInPipeline info = null;
    switch(state) {
    case RBW:
      info = buildRBW();
      break;
    case TEMPORARY:
      info = buildTemporaryReplica();
      break;
    default:
      throw new IllegalArgumentException("Unknown replica state " + state);
    }
    return info;
  }

  /**
   * 构建正在被写入（RBW）状态的副本对象。
   * @return 正在被写入的副本对象
   * @throws IllegalArgumentException 参数不合法时抛出异常
   */
  private LocalReplicaInPipeline buildRBW() throws IllegalArgumentException {
    // 基于已有RBW副本克隆新对象
    if (null != fromReplica && fromReplica.getState() == ReplicaState.RBW) {
      return new ReplicaBeingWritten((ReplicaBeingWritten) fromReplica);
    } else if (null != fromReplica) {
      // 源副本状态不匹配
      throw new IllegalArgumentException("Incompatible fromReplica "
          + "state: " + fromReplica.getState());
    } else {
      // 基于已有的块对象构造
      if (null != block) {
        if (null == writer) {
          throw new IllegalArgumentException("A valid writer is "
              + "required for constructing a RBW from block "
              + block.getBlockId());
        }
        return new ReplicaBeingWritten(block, volume, directoryUsed, writer);
      } else {
        // 基于单独参数构造
        if (length != -1) {
          return new ReplicaBeingWritten(blockId, length, genStamp,
              volume, directoryUsed, writer, bytesToReserve);
        } else {
          return new ReplicaBeingWritten(blockId, genStamp, volume,
              directoryUsed, bytesToReserve);
        }
      }
    }
  }

  /**
   * 构建临时状态的副本对象。
   * @return 临时副本对象
   * @throws IllegalArgumentException 参数不合法时抛出异常
   */
  private LocalReplicaInPipeline buildTemporaryReplica()
      throws IllegalArgumentException {
    // 基于已有临时副本克隆新对象
    if (null != fromReplica &&
        fromReplica.getState() == ReplicaState.TEMPORARY) {
      return new LocalReplicaInPipeline((LocalReplicaInPipeline) fromReplica);
    } else if (null != fromReplica) {
      // 源副本状态不匹配
      throw new IllegalArgumentException("Incompatible fromReplica "
          + "state: " + fromReplica.getState());
    } else {
      // 基于已有的块对象构造
      if (null != block) {
        if (null == writer) {
          throw new IllegalArgumentException("A valid writer is "
              + "required for constructing a Replica from block "
              + block.getBlockId());
        }
        return new LocalReplicaInPipeline(block, volume, directoryUsed,
            writer);
      } else {
        // 基于单独参数构造
        if (length != -1) {
          return new LocalReplicaInPipeline(blockId, length, genStamp,
              volume, directoryUsed, writer, bytesToReserve);
        } else {
          return new LocalReplicaInPipeline(blockId, genStamp, volume,
              directoryUsed, bytesToReserve);
        }
      }
    }
  }

  /**
   * 构建已完成（FINALIZED）状态的本地副本对象。
   * @return 已完成本地副本对象
   * @throws IllegalArgumentException 参数不合法时抛出异常
   */
  private LocalReplica buildFinalizedReplica() throws IllegalArgumentException {
    // 基于已有已完成副本克隆新对象
    if (null != fromReplica &&
        fromReplica.getState() == ReplicaState.FINALIZED) {
      return new FinalizedReplica((FinalizedReplica)fromReplica);
    } else if (null != this.fromReplica) {
      // 源副本状态不匹配
      throw new IllegalArgumentException("Incompatible fromReplica "
          + "state: " + fromReplica.getState());
    } else {
      // 基于已有的块对象构造
      if (null != block) {
        return new FinalizedReplica(block, volume, directoryUsed,
            lastPartialChunkChecksum);
      } else {
        // 基于单独参数构造
        return new FinalizedReplica(blockId, length, genStamp, volume,
            directoryUsed, lastPartialChunkChecksum);
      }
    }
  }

  /**
   * 构建等待恢复（RWR）状态的本地副本对象。
   * @return 等待恢复本地副本对象
   * @throws IllegalArgumentException 参数不合法时抛出异常
   */
  private LocalReplica buildRWR() throws IllegalArgumentException {
    // 基于已有等待恢复副本克隆新对象
    if (null != fromReplica && fromReplica.getState() == ReplicaState.RWR) {
      return new ReplicaWaitingToBeRecovered(
          (ReplicaWaitingToBeRecovered) fromReplica);
    } else if (null != fromReplica){
      // 源副本状态不匹配
      throw new IllegalArgumentException("Incompatible fromReplica "
          + "state: " + fromReplica.getState());
    } else {
      // 基于已有的块对象构造
      if (null != block) {
        return new ReplicaWaitingToBeRecovered(block, volume, directoryUsed);
      } else {
        // 基于单独参数构造
        return new ReplicaWaitingToBeRecovered(blockId, length, genStamp,
            volume, directoryUsed);
      }
    }
  }

  /**
   * 构建恢复中（RUR）状态的本地副本对象。
   * @return 恢复中本地副本对象
   * @throws IllegalArgumentException 参数不合法时抛出异常
   */
  private LocalReplica buildRUR() throws IllegalArgumentException {
    // 恢复中副本必须基于已有源副本构造
    if (null == fromReplica) {
      throw new IllegalArgumentException(
          "Missing a valid replica to recover from");
    }
    // 参数合法性检查
    if (null != writer || null != block) {
      throw new IllegalArgumentException("Invalid state for "
          + "recovering from replica with blk id "
          + fromReplica.getBlockId());
    }
    // 基于已有恢复中副本克隆新对象
    if (fromReplica.getState() == ReplicaState.RUR) {
      return new ReplicaUnderRecovery((ReplicaUnderRecovery) fromReplica);
    } else {
      // 基于普通源副本创建新的恢复中副本
      return new ReplicaUnderRecovery(fromReplica, recoveryId);
    }
  }

  /**
   * 构建已完成状态的外部提供（PROVIDED）副本对象。
   * @return 已完成外部提供副本对象
   * @throws IllegalArgumentException 参数不合法时抛出异常
   */
  private ProvidedReplica buildProvidedFinalizedReplica()
      throws IllegalArgumentException {
    ProvidedReplica info = null;
    // 外部提供副本不支持从已有副本克隆
    if (fromReplica != null) {
      throw new IllegalArgumentException("Finalized PROVIDED replica " +
          "cannot be constructed from another replica");
    }
    // 必须提供足够的外部存储定位信息
    if (fileRegion == null && uri == null &&
        (pathPrefix == null || pathSuffix == null)) {
      throw new IllegalArgumentException(
          "Trying to construct a provided replica on " + volume +
          " without enough information");
    }
    // 基于URI构造
    if (fileRegion == null) {
      if (uri != null) {
        info = new FinalizedProvidedReplica(blockId, uri, offset,
            length, genStamp, pathHandle, volume, conf, remoteFS);
      } else {
        // 基于路径前缀+后缀构造
        info = new FinalizedProvidedReplica(blockId, pathPrefix, pathSuffix,
            offset, length, genStamp, pathHandle, volume, conf, remoteFS);
      }
    } else {
      // 基于文件区域构造
      info = new FinalizedProvidedReplica(fileRegion, volume, conf, remoteFS);
    }
    return info;
  }

  /**
   * 构建外部提供（PROVIDED）类型的副本对象。
   * @return 外部提供副本对象