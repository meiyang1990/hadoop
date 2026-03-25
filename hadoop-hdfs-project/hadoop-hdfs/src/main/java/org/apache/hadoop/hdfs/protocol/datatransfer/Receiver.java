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
package org.apache.hadoop.hdfs.protocol.datatransfer;

import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil.fromProto;
import static org.apache.hadoop.hdfs.protocolPB.PBHelperClient.vintPrefixed;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.StripedBlockInfo;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BaseHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.CachingStrategyProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientOperationHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockGroupChecksumProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpCopyBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReadBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReplaceBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpRequestShortCircuitAccessProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpTransferBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpWriteBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ReleaseShortCircuitAccessRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ShortCircuitShmRequestProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.SlotId;
import org.apache.hadoop.tracing.SpanContext;
import org.apache.hadoop.tracing.TraceScope;
import org.apache.hadoop.tracing.Tracer;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.hadoop.thirdparty.protobuf.ByteString;

/**
 * HDFS数据传输协议接收方抽象基类
 * 负责从输入流读取不同类型的数据传输操作，解析Protobuf请求后调用对应处理方法
 * 是DataNode处理客户端/其他DataNode数据传输请求的核心入口
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class Receiver implements DataTransferProtocol {
  private final Tracer tracer;
  protected DataInputStream in;

  /**
   * 构造接收方实例，传入追踪器用于链路追踪
   * @param tracer 链路追踪器实例
   */
  protected Receiver(Tracer tracer) {
    this.tracer = tracer;
  }

  /**
   * 使用给定输入流初始化接收方，后续将从该流读取请求
   * @param in 数据输入流
   */
  protected void initialize(final DataInputStream in) {
    this.in = in;
  }

  /**
   * 读取操作码并验证协议版本一致性
   * @return 解析得到的操作类型
   * @throws IOException 版本不匹配或IO异常时抛出
   */
  protected final Op readOp() throws IOException {
    final short version = in.readShort();
    if (version != DataTransferProtocol.DATA_TRANSFER_VERSION) {
      throw new IOException( "Version Mismatch (Expected: " +
          DataTransferProtocol.DATA_TRANSFER_VERSION  +
          ", Received: " +  version + " )");
    }
    return Op.read(in);
  }

  /**
   * 从请求携带的span信息继续链路追踪
   * @param spanContextBytes Protobuf格式的span上下文字节
   * @param description 追踪描述
   * @return 新的追踪范围对象
   */
  private TraceScope continueTraceSpan(ByteString spanContextBytes,
                                       String description) {
    TraceScope scope = null;
    SpanContext spanContext =
        TraceUtils.byteStringToSpanContext(spanContextBytes);
    if (spanContext != null) {
      scope = tracer.newScope(description, spanContext);
    }
    return scope;
  }

  /**
   * 从客户端操作头中提取span上下文继续链路追踪
   * @param header 客户端操作头Proto对象
   * @param description 追踪描述
   * @return 新的追踪范围对象
   */
  private TraceScope continueTraceSpan(ClientOperationHeaderProto header,
                                             String description) {
    return continueTraceSpan(header.getBaseHeader(), description);
  }

  /**
   * 从基础头中提取span上下文继续链路追踪
   * @param header 基础头Proto对象
   * @param description 追踪描述
   * @return 新的追踪范围对象
   */
  private TraceScope continueTraceSpan(BaseHeaderProto header,
                                             String description) {
    return continueTraceSpan(header.getTraceInfo().getSpanContext(),
        description);
  }

  /**
   * 根据操作类型分发到对应处理方法
   * @param op 要处理的操作类型
   * @throws IOException 未知操作或处理异常时抛出
   */
  protected final void processOp(Op op) throws IOException {
    switch(op) {
    case READ_BLOCK:
      opReadBlock();
      break;
    case WRITE_BLOCK:
      opWriteBlock(in);
      break;
    case REPLACE_BLOCK:
      opReplaceBlock(in);
      break;
    case COPY_BLOCK:
      opCopyBlock(in);
      break;
    case BLOCK_CHECKSUM:
      opBlockChecksum(in);
      break;
    case BLOCK_GROUP_CHECKSUM:
      opStripedBlockChecksum(in);
      break;
    case TRANSFER_BLOCK:
      opTransferBlock(in);
      break;
    case REQUEST_SHORT_CIRCUIT_FDS:
      opRequestShortCircuitFds(in);
      break;
    case RELEASE_SHORT_CIRCUIT_FDS:
      opReleaseShortCircuitFds(in);
      break;
    case REQUEST_SHORT_CIRCUIT_SHM:
      opRequestShortCircuitShm(in);
      break;
    default:
      throw new IOException("Unknown op " + op + " in data stream");
    }
  }

  /**
   * 从Protobuf缓存策略转换为内部缓存策略对象
   * @param strategy Protobuf格式缓存策略
   * @return 转换后的内部缓存策略对象
   */
  static private CachingStrategy getCachingStrategy(CachingStrategyProto strategy) {
    Boolean dropBehind = strategy.hasDropBehind() ?
        strategy.getDropBehind() : null;
    Long readahead = strategy.hasReadahead() ?
        strategy.getReadahead() : null;
    return new CachingStrategy(dropBehind, readahead);
  }

  /**
   * 处理读块请求解析与分发
   * @throws IOException IO或解析异常时抛出
   */
  private void opReadBlock() throws IOException {
    OpReadBlockProto proto = OpReadBlockProto.parseFrom(vintPrefixed(in));
    TraceScope traceScope = continueTraceSpan(proto.getHeader(),
        proto.getClass().getSimpleName());
    try {
      readBlock(PBHelperClient.convert(proto.getHeader().getBaseHeader().getBlock()),
        PBHelperClient.convert(proto.getHeader().getBaseHeader().getToken()),
        proto.getHeader().getClientName(),
        proto.getOffset(),
        proto.getLen(),
        proto.getSendChecksums(),
        (proto.hasCachingStrategy() ?
            getCachingStrategy(proto.getCachingStrategy()) :
          CachingStrategy.newDefaultStrategy()));
    } finally {
      if (traceScope != null) traceScope.close();
    }
  }
  
  /**
   * 处理写块请求解析与分发
   * @param in 输入流
   * @throws IOException IO或解析异常时抛出
   */
  private void opWriteBlock(DataInputStream in) throws IOException {
    final OpWriteBlockProto proto = OpWriteBlockProto.parseFrom(vintPrefixed(in));
    final DatanodeInfo[] targets = PBHelperClient.convert(proto.getTargetsList());
    TraceScope traceScope = continueTraceSpan(proto.getHeader(),
        proto.getClass().getSimpleName());
    try {
      writeBlock(PBHelperClient.convert(proto.getHeader().getBaseHeader().getBlock()),
          PBHelperClient.convertStorageType(proto.getStorageType()),
          PBHelperClient.convert(proto.getHeader().getBaseHeader().getToken()),
          proto.getHeader().getClientName(),
          targets,
          PBHelperClient.convertStorageTypes(proto.getTargetStorageTypesList(), targets.length),
          PBHelperClient.convert(proto.getSource()),
          fromProto(proto.getStage()),
          proto.getPipelineSize(),
          proto.getMinBytesRcvd(), proto.getMaxBytesRcvd(),
          proto.getLatestGenerationStamp(),
          fromProto(proto.getRequestedChecksum()),
          (proto.hasCachingStrategy() ?
              getCachingStrategy(proto.getCachingStrategy()) :
            CachingStrategy.newDefaultStrategy()),
          (proto.hasAllowLazyPersist() ? proto.getAllowLazyPersist() : false),
          (proto.hasPinning() ? proto.getPinning(): false),
          (PBHelperClient.convertBooleanList(proto.getTargetPinningsList())),
          proto.getStorageId(),
          proto.getTargetStorageIdsList().toArray(new String[0]));
    } finally {
     if (traceScope != null) traceScope.close();
    }
  }

  /**
   * 处理块传输请求解析与分发，用于数据节点间复制块数据
   * @param in 输入流
   * @throws IOException IO或解析异常时抛出
   */
  private void opTransferBlock(DataInputStream in) throws IOException {
    final OpTransferBlockProto proto =
      OpTransferBlockProto.parseFrom(vintPrefixed(in));
    final DatanodeInfo[] targets = PBHelperClient.convert(proto.getTargetsList());
    TraceScope traceScope = continueTraceSpan(proto.getHeader(),
        proto.getClass().getSimpleName());
    try {
      final ExtendedBlock block =
          PBHelperClient.convert(proto.getHeader().getBaseHeader().getBlock());
      final StorageType[] targetStorageTypes =
          PBHelperClient.convertStorageTypes(proto.getTargetStorageTypesList(),
              targets.length);
      transferBlock(block,
          PBHelperClient.convert(proto.getHeader().getBaseHeader().getToken()),
          proto.getHeader().getClientName(),
          targets,
          targetStorageTypes,
          proto.getTargetStorageIdsList().toArray(new String[0])
      );
    } finally {
      if (traceScope != null) traceScope.close();
    }
  }

  /**
   * 处理短路读文件描述符请求解析与分发，用于客户端直接读取本地块数据
   * @param in 输入流
   * @throws IOException IO或解析异常时抛出
   */
  private void opRequestShortCircuitFds(DataInputStream in) throws IOException {
    final OpRequestShortCircuitAccessProto proto =
      OpRequestShortCircuitAccessProto.parseFrom(vintPrefixed(in));
    SlotId slotId = (proto.hasSlotId()) ? 
        PBHelperClient.convert(proto.getSlotId()) : null;
    TraceScope traceScope = continueTraceSpan(proto.getHeader(),
        proto.getClass().getSimpleName());
    try {
      requestShortCircuitFds(PBHelperClient.convert(proto.getHeader().getBlock()),
          PBHelperClient.convert(proto.getHeader().getToken()),
          slotId, proto.getMaxVersion(),
          proto.getSupportsReceiptVerification());
    } finally {
      if (traceScope != null) traceScope.close();
    }
  }

  /**
   * 处理释放短路读文件描述符请求解析与分发
   * @param in 输入流
   * @throws IOException IO或解析异常时抛出
   */
  private void opReleaseShortCircuitFds(DataInputStream in)
      throws IOException {
    final ReleaseShortCircuitAccessRequestProto proto =
      ReleaseShortCircuitAccessRequestProto.parseFrom(vintPrefixed(in));
    TraceScope traceScope = continueTraceSpan(
        proto.getTraceInfo().getSpanContext(),
        proto.getClass().getSimpleName());
    try {
      releaseShortCircuitFds(PBHelperClient.convert(proto.getSlotId()));
    } finally {
      if (traceScope != null) traceScope.close();
    }
  }

  /**
   * 处理申请短路读共享内存请求解析与分发
   * @param in 输入流
   * @throws IOException IO或解析异常时抛出
   */
  private void opRequestShortCircuitShm(DataInputStream in) throws IOException {
    final ShortCircuitShmRequestProto proto =
        ShortCircuitShmRequestProto.parseFrom(vintPrefixed(in));
    TraceScope traceScope = continueTraceSpan(
        proto.getTraceInfo().getSpanContext(),
        proto.getClass().getSimpleName());
    try {
      requestShortCircuitShm(proto.getClientName());
    } finally {
      if (traceScope != null) traceScope.close();
    }
  }

  /**
   * 处理替换块请求解析与分发，用于数据平衡等场景迁移块
   * @param in 输入流
   * @throws IOException IO或解析异常时抛出
   */
  private void opReplaceBlock(DataInputStream in) throws IOException {
    OpReplaceBlockProto proto = OpReplaceBlockProto.parseFrom(vintPrefixed(in));
    TraceScope traceScope = continueTraceSpan(proto.getHeader(),
        proto.getClass().getSimpleName());
    try {
      replaceBlock(PBHelperClient.convert(proto.getHeader().getBlock()),
          PBHelperClient.convertStorageType(proto.getStorageType()),
          PBHelperClient.convert(proto.getHeader().getToken()),
          proto.getDelHint(),
          PBHelperClient.convert(proto.getSource()),
          proto.getStorageId());
    } finally {
      if (traceScope != null) traceScope.close();
    }
  }

  /**
   * 处理复制块请求解析与分发
   * @param in 输入流
   * @throws IOException IO或解析异常时抛出
   */
  private void opCopyBlock(DataInputStream in) throws IOException {
    OpCopyBlockProto proto = OpCopyBlockProto.parseFrom(vintPrefixed(in));
    TraceScope traceScope = continueTraceSpan(proto.getHeader(),
        proto.getClass().getSimpleName());
    try {
      copyBlock(PBHelperClient.convert(proto.getHeader().getBlock()),
          PBHelperClient.convert(proto.getHeader().getToken()));
    } finally {
      if (traceScope != null) traceScope.close();
    }
  }

  /**
   * 处理获取块校验和请求解析与分发
   * @param in 输入流
   * @throws IOException IO或解析异常时抛出
   */
  private void opBlockChecksum(DataInputStream in) throws IOException {
    OpBlockChecksumProto proto = OpBlockChecksumProto.parseFrom(vintPrefixed(in));
    TraceScope traceScope = continueTraceSpan(proto.getHeader(),
        proto.getClass().getSimpleName());
    try {
      blockChecksum(PBHelperClient.convert(proto.getHeader().getBlock()),
          PBHelperClient.convert(proto.getHeader().getToken()),
          PBHelperClient.convert(proto.getBlockChecksumOptions()));
    } finally {
      if (traceScope != null) traceScope.close();
    }
  }

  /**
   * 处理获取纠删码块组校验和请求解析与分发
   * @param dis 输入流
   * @throws IOException IO或解析异常时抛出
   */
  private void opStripedBlockChecksum(DataInputStream dis) throws IOException {
    OpBlockGroupChecksumProto proto =
        OpBlockGroupChecksumProto.parseFrom(vintPrefixed(dis));
    TraceScope traceScope = continueTraceSpan(proto.getHeader(),
        proto.getClass().getSimpleName());
    StripedBlockInfo stripedBlockInfo = new StripedBlockInfo(
        PBHelperClient.convert(proto.getHeader().getBlock()),
        PBHelperClient.convert(proto.getDatanodes()),
        PBHelperClient.convertTokens(proto.getBlockTokensList()),
        PBHelperClient.convertBlockIndices(proto.getBlockIndicesList()),
        PBHelperClient.convertErasureCodingPolicy(proto.getEcPolicy())
    );

    try {
      blockGroupChecksum(stripedBlockInfo,
          PBHelperClient.convert(proto.getHeader().getToken()),
          proto.getRequestedNumBytes(),
          PBHelperClient.convert(proto.getBlockChecksumOptions()));
    } finally {
      if (traceScope != null) {
        traceScope.close();
      }
    }
  }
}