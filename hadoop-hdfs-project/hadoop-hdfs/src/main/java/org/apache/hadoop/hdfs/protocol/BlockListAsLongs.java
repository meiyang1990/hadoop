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
package org.apache.hadoop.hdfs.protocol;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportReplica;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.thirdparty.protobuf.CodedInputStream;
import org.apache.hadoop.thirdparty.protobuf.CodedOutputStream;
import org.apache.hadoop.thirdparty.protobuf.WireFormat;

/**
 * 数据节点块报告的高效序列化抽象基类，支持新旧两种编码格式，通过增量解码减少GC开销
 * 核心功能是将块副本信息编码为变长字节序列，避免protobuf重复字段带来的装箱拆箱和内存分配开销
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class BlockListAsLongs implements Iterable<BlockReportReplica> {
  // 分块编码最大块大小，单位字节
  private final static int CHUNK_SIZE = 64*1024; // 64K
  // 空块列表占位数组
  private static long[] EMPTY_LONGS = new long[]{0, 0};

  /** 空块报告实例 */
  public static BlockListAsLongs EMPTY = new BlockListAsLongs() {
    @Override
    public int getNumberOfBlocks() {
      return 0;
    }
    @Override
    public ByteString getBlocksBuffer() {
      return ByteString.EMPTY;
    }
    @Override
    public long[] getBlockListAsLongs() {
      return EMPTY_LONGS;
    }
    @Override
    public Iterator<BlockReportReplica> iterator() {
      return Collections.emptyIterator();
    }
  };

  /**
   * 从单个ByteString缓冲区创建就地解码器，不复制数据
   * @param numBlocks 缓冲区中包含的块数量
   * @param blocksBuf 编码了块信息的ByteString缓冲区
   * @param maxDataLength protobuf消息允许的最大数据长度
   * @return 块列表实例
   */
  public static BlockListAsLongs decodeBuffer(final int numBlocks,
      final ByteString blocksBuf, final int maxDataLength) {
    return new BufferDecoder(numBlocks, blocksBuf, maxDataLength);
  }

  /**
   * 从多个ByteString缓冲区创建就地解码器（仅测试用）
   * @param numBlocks 缓冲区中包含的块数量
   * @param blocksBufs 编码了块信息的ByteString列表
   * @return 块列表实例
   */
  @VisibleForTesting
  public static BlockListAsLongs decodeBuffers(final int numBlocks,
      final List<ByteString> blocksBufs) {
    return decodeBuffers(numBlocks, blocksBufs,
        IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
  }

  /**
   * 从多个ByteString缓冲区创建就地解码器
   * @param numBlocks 缓冲区中包含的块数量
   * @param blocksBufs 编码了块信息的ByteString列表
   * @param maxDataLength protobuf消息允许的最大数据长度
   * @return 块列表实例
   */
  public static BlockListAsLongs decodeBuffers(final int numBlocks,
      final List<ByteString> blocksBufs, final int maxDataLength) {
    // 实际不复制数据，仅创建视图
    return decodeBuffer(numBlocks, ByteString.copyFrom(blocksBufs),
        maxDataLength);
  }

  /**
   * 从旧格式Long列表创建解码器，仅用于向后兼容，性能低于ByteString解码
   * @param blocksList 旧格式块信息Long列表
   * @return 块列表实例
   */
  public static BlockListAsLongs decodeLongs(List<Long> blocksList) {
    return decodeLongs(blocksList, IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
  }

  /**
   * 从旧格式Long列表创建解码器，仅用于向后兼容，性能低于ByteString解码
   * @param blocksList 旧格式块信息Long列表
   * @param maxDataLength protobuf消息允许的最大数据长度
   * @return 块列表实例
   */
  public static BlockListAsLongs decodeLongs(List<Long> blocksList,
      int maxDataLength) {
    return blocksList.isEmpty() ? EMPTY :
        new LongsDecoder(blocksList, maxDataLength);
  }

  /**
   * 将副本集合编码为高效的ByteString格式（仅测试用）
   * @param replicas 需要编码的副本集合
   * @return 编码后的块列表实例
   */
  @VisibleForTesting
  public static BlockListAsLongs encode(
      final Collection<? extends Replica> replicas) {
    BlockListAsLongs.Builder builder = builder(IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
    for (Replica replica : replicas) {
      builder.add(replica);
    }
    return builder.build();
  }

  /**
   * 从输入流读取并解码块报告
   * @param is 输入流
   * @param maxDataLength protobuf消息允许的最大数据长度
   * @return 解码后的块列表实例，读取失败返回null
   * @throws IOException 读取IO异常
   */
  public static BlockListAsLongs readFrom(InputStream is, int maxDataLength)
      throws IOException {
    CodedInputStream cis = CodedInputStream.newInstance(is);
    if (maxDataLength != IPC_MAXIMUM_DATA_LENGTH_DEFAULT) {
      cis.setSizeLimit(maxDataLength);
    }
    int numBlocks = -1;
    ByteString blocksBuf = null;
    while (!cis.isAtEnd()) {
      // 读取protobuf标签
      int tag = cis.readTag();
      // 提取字段编号
      int field = WireFormat.getTagFieldNumber(tag);
      switch(field) {
        case 0:
          break;
        case 1:
          // 读取块数量字段
          numBlocks = (int)cis.readInt32();
          break;
        case 2:
          // 读取块数据缓冲区字段
          blocksBuf = cis.readBytes();
          break;
        default:
          // 跳过未知字段
          cis.skipField(tag);
          break;
      }
    }
    if (numBlocks != -1 && blocksBuf != null) {
      return decodeBuffer(numBlocks, blocksBuf, maxDataLength);
    }
    return null;
  }

  /**
   * 将块列表编码写入输出流，符合protobuf格式
   * @param os 目标输出流
   * @throws IOException 写入IO异常
   */
  public void writeTo(OutputStream os) throws IOException {
    CodedOutputStream cos = CodedOutputStream.newInstance(os);
    cos.writeInt32(1, getNumberOfBlocks());
    cos.writeBytes(2, getBlocksBuffer());
    cos.flush();
  }

  /**
   * 创建块报告构建器（仅测试用）
   * @return 新构建器实例
   */
  @VisibleForTesting
  public static Builder builder() {
    return builder(IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
  }

  /**
   * 创建块报告构建器
   * @param maxDataLength 最大允许数据长度
   * @return 新构建器实例
   */
  public static Builder builder(int maxDataLength) {
    return new BlockListAsLongs.Builder(maxDataLength);
  }

  /**
   * 获取块列表中包含的总块数量
   * @return 块数量
   */
  abstract public int getNumberOfBlocks();

  /**
   * 获取编码后的块数据缓冲区，使用高效字节编码避免GC开销
   * 每个副本占4个long：块ID、块长度、生成时间戳、副本状态
   * @return 编码后的ByteString缓冲区
   */
  abstract public ByteString getBlocksBuffer();

  /**
   * 将块缓冲区按64K分块切割，适应protobuf消息大小限制
   * 不实际复制数据，仅返回原缓冲区的子视图
   * @return 分块后的ByteString列表
   */
  public List<ByteString> getBlocksBuffers() {
    final ByteString blocksBuf = getBlocksBuffer();
    final List<ByteString> buffers;
    final int size = blocksBuf.size();
    if (size <= CHUNK_SIZE) {
      buffers = Collections.singletonList(blocksBuf);
    } else {
      buffers = new ArrayList<ByteString>();
      for (int pos=0; pos < size; pos += CHUNK_SIZE) {
        // 不实际复制数据，仅创建视图
        buffers.add(blocksBuf.substring(pos, Math.min(pos+CHUNK_SIZE, size)));
      }
    }
    return buffers;
  }

  /**
   * 转换为旧格式long数组，仅用于兼容旧版本NameNode，性能较低
   * 数组结构：
   * 0: 已完成副本数量
   * 1: 构建中副本数量
   * 后续：已完成副本列表每个占3个long（块ID、长度、时间戳） + 分隔符三个-1 + 构建中副本每个占4个long（额外加状态）
   * @return 旧格式long数组
   */
  abstract public long[] getBlockListAsLongs();

  /**
   * 返回块报告的迭代器，迭代过程复用同一个BlockReportReplica对象，减少对象分配
   * 不要将迭代返回的对象添加到集合中
   * @return 块副本迭代器
   */
  abstract public Iterator<BlockReportReplica> iterator();

  /**
   * 块报告构建器，用于将多个副本编码为高效ByteString格式
   */
  public static class Builder {
    private final ByteString.Output out;
    private final CodedOutputStream cos;
    private int numBlocks = 0;
    private int numFinalized = 0;
    private final int maxDataLength;

    Builder(int maxDataLength) {
      out = ByteString.newOutput(64*1024);
      cos = CodedOutputStream.newInstance(out);
      this.maxDataLength = maxDataLength;
    }

    /**
     * 添加一个副本到编码缓冲区
     * @param replica 待添加的副本
     */
    public void add(Replica replica) {
      try {
        // 使用zig-zag编码压缩块ID，减小旧块ID的存储空间
        cos.writeSInt64NoTag(replica.getBlockId());
        cos.writeUInt64NoTag(replica.getBytesOnDisk());
        cos.writeUInt64NoTag(replica.getGenerationStamp());
        ReplicaState state = replica.getState();
        // 使用long变长编码存储状态，预留高位供未来扩展使用
        cos.writeUInt64NoTag(state.getValue());
        if (state == ReplicaState.FINALIZED) {
          numFinalized++;
        }
        numBlocks++;
      } catch (IOException ioe) {
        // ByteString.Output不会抛出IO异常，此处仅处理编译要求
        throw new IllegalStateException(ioe);
      }
    }

    /**
     * 获取已添加的总块数量
     * @return 总块数量
     */
    public int getNumberOfBlocks() {
      return numBlocks;
    }
    
    /**
     * 完成编码，构建块列表实例
     * @return 编码完成的块列表实例
     */
    public BlockListAsLongs build() {
      try {
        cos.flush();
      } catch (IOException ioe) {
        // ByteString.Output不会抛出IO异常，此处仅处理编译要求
        throw new IllegalStateException(ioe);
      }
      return new BufferDecoder(numBlocks, numFinalized, out.toByteString(),
          maxDataLength);
    }
  }

  /**
   * 新格式ByteString缓冲区块报告解码器，实现就地增量解码避免复制
   */
  // decode new-style ByteString buffer based block report
  private static class BufferDecoder extends BlockListAsLongs {
    // 预留高位用于未来扩展，解码时掩码过滤未使用位保证向前兼容
    // 块长度占低48位
    private static long NUM_BYTES_MASK = (-1L) >>> (64 - 48);
    // 副本状态占低4位
    private static long REPLICA_STATE_MASK = (-1L) >>> (64 - 4);

    private final ByteString buffer;
    private final int numBlocks;
    private int numFinalized;
    private final int maxDataLength;

    BufferDecoder(final int numBlocks, final ByteString buf,
        final int maxDataLength) {
      this(numBlocks, -1, buf, maxDataLength);
    }

    BufferDecoder(final int numBlocks, final int numFinalized,
        final ByteString buf, final int maxDataLength) {
      this.numBlocks = numBlocks;
      this.numFinalized = numFinalized;
      this.buffer = buf;
      this.maxDataLength = maxDataLength;
    }

    @Override
    public int getNumberOfBlocks() {
      return numBlocks;
    }

    @Override
    public ByteString getBlocksBuffer() {
      return buffer;
    }

    @Override
    public long[] getBlockListAsLongs() {
      // 转换为旧格式性能极低，仅在需要转码时使用，实际生产中极少触发
      if (numFinalized == -1) {
        int n = 0;
        for (Replica replica : this) {
          if (replica.getState() == ReplicaState.FINALIZED) {
            n++;
          }
        }
        numFinalized = n;
      }
      int numUc = numBlocks - numFinalized;
      // 计算数组总大小：2个头 + 每个完成块3个long加1个分隔符 + 每个未完成块4个long
      int size = 2 + 3*(numFinalized+1) + 4*(numUc);
      long[] longs = new long[size];
      // 填充头信息
      longs[0] = numFinalized;
      longs[1] = numUc;

      int idx = 2;
      int ucIdx = idx + 3*numFinalized;
      // 写入分隔符块（三个-1）
      longs[ucIdx++] = -1;
      longs[ucIdx++] = -1;
      longs[ucIdx++] = -1;

      for (BlockReportReplica block : this) {
        switch (block.getState()) {
          case FINALIZED: {
            // 已完成块写入已完成区
            longs[idx++] = block.getBlockId();
            longs[idx++] = block.getNumBytes();
            longs[idx++] = block.getGenerationStamp();
            break;
          }
          default: {
            // 非完成块写入未完成区，额外存储状态
            longs[ucIdx++] = block.getBlockId();
            longs[ucIdx++] = block.getNumBytes();
            longs[ucIdx++] = block.getGenerationStamp();
            longs[ucIdx++] = block.getState().getValue();
            break;
          }
        }
      }
      return longs;
    }

    @Override
    public Iterator<BlockReportReplica> iterator() {
      return new Iterator<BlockReportReplica>() {
        // 复用单个对象避免分配
        final BlockReportReplica block = new BlockReportReplica();
        // 从缓冲区获取输入流
        final CodedInputStream cis = buffer.newCodedInput();
        private int currentBlockIndex = 0;

        {
          if (maxDataLength != IPC_MAXIMUM_DATA_LENGTH_DEFAULT) {
            cis.setSizeLimit(maxDataLength);
          }
        }

        @Override
        public boolean hasNext() {
          return currentBlockIndex < numBlocks;
        }

        @Override
        public BlockReportReplica next() {
          currentBlockIndex++;
          try {
            // zig-zag解码块ID，掩码过滤预留位
            block.setBlockId(cis.readSInt64());
            block.setNumBytes(cis.readRawVarint64() & NUM_BYTES_MASK);
            block.setGenerationStamp(cis.readRawVarint64());
            long state = cis.readRawVarint64() & REPLICA_STATE_MASK;
            block.setState(ReplicaState.getState((int)state));
          } catch (IOException e) {
            throw new IllegalStateException(e