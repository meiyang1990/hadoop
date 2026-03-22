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

package org.apache.hadoop.hdfs.server.common.sps;

import static org.apache.hadoop.hdfs.protocolPB.PBHelperClient.vintPrefixed;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockPinningException;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.protocol.BlockStorageMovementCommand.BlockMovingInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件说明：HDFS存储策略满足器中负责分发块迁移任务的组件，驱动数据节点之间完成块副本迁移
 * 
 * Dispatching block replica moves between datanodes to satisfy the storage
 * policy.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockDispatcher {
  private static final Logger LOG = LoggerFactory
      .getLogger(BlockDispatcher.class);

  private final boolean connectToDnViaHostname;
  private final int socketTimeout;
  private final int ioFileBufferSize;

  /**
   * 构造块分发器，初始化连接和IO相关配置
   *
   * @param sockTimeout
   *          socket读取超时时间
   * @param ioFileBuffSize
   *          IO缓冲区大小
   * @param connectToDatanodeViaHostname
   *          是否通过主机名连接数据节点，false表示使用IP地址
   */
  public BlockDispatcher(int sockTimeout, int ioFileBuffSize,
      boolean connectToDatanodeViaHostname) {
    this.socketTimeout = sockTimeout;
    this.ioFileBufferSize = ioFileBuffSize;
    this.connectToDnViaHostname = connectToDatanodeViaHostname;
  }

  /**
   * 将指定块副本从源数据节点迁移到目标数据节点，阻塞等待迁移完成返回结果
   * 用于满足存储策略要求，将块移动到指定存储类型的节点上
   *
   * @param blkMovingInfo
   *          待迁移块的信息，包含源、目标节点和存储类型要求
   * @param saslClient
   *          SASL数据传输客户端，用于安全认证连接
   * @param eb
   *          扩展块信息
   * @param sock
   *          用于连接目标节点的socket对象
   * @param km
   *          数据加密密钥工厂
   * @param accessToken
   *          块访问权限令牌
   * @return 块迁移状态结果
   * @throws IOException 连接或迁移过程中发生IO异常
   */
  public BlockMovementStatus moveBlock(BlockMovingInfo blkMovingInfo,
      SaslDataTransferClient saslClient, ExtendedBlock eb, Socket sock,
      DataEncryptionKeyFactory km, Token<BlockTokenIdentifier> accessToken) throws IOException {
    LOG.info("Start moving block:{} from src:{} to destin:{} to satisfy "
        + "storageType, sourceStoragetype:{} and destinStoragetype:{}",
        blkMovingInfo.getBlock(), blkMovingInfo.getSource(),
        blkMovingInfo.getTarget(), blkMovingInfo.getSourceStorageType(),
        blkMovingInfo.getTargetStorageType());
    DataOutputStream out = null;
    DataInputStream in = null;
    try {
      // 建立到目标数据节点数据传输端口的连接
      NetUtils.connect(sock,
          NetUtils.createSocketAddr(
              blkMovingInfo.getTarget().getXferAddr(connectToDnViaHostname)),
          socketTimeout);
      // 设置读超时：数据节点默认每30秒发送一次IN_PROGRESS响应，此处设置总超时为socket超时的5倍
      // 避免无响应节点导致连接永久挂起
      sock.setSoTimeout(socketTimeout * 5);
      sock.setKeepAlive(true);
      // 获取原始socket流
      OutputStream unbufOut = sock.getOutputStream();
      InputStream unbufIn = sock.getInputStream();
      LOG.debug("Connecting to datanode {}", blkMovingInfo.getTarget());

      // 通过SASL完成安全握手，获取加密后的流
      IOStreamPair saslStreams = saslClient.socketSend(sock, unbufOut,
          unbufIn, km, accessToken, blkMovingInfo.getTarget());
      unbufOut = saslStreams.out;
      unbufIn = saslStreams.in;
      // 包装为带缓冲的数据流
      out = new DataOutputStream(
          new BufferedOutputStream(unbufOut, ioFileBufferSize));
      in = new DataInputStream(
          new BufferedInputStream(unbufIn, ioFileBufferSize));
      // 发送块迁移请求
      sendRequest(out, eb, accessToken, blkMovingInfo.getSource(),
          blkMovingInfo.getTargetStorageType());
      // 接收并处理迁移响应
      receiveResponse(in);

      LOG.info(
          "Successfully moved block:{} from src:{} to destin:{} for"
              + " satisfying storageType:{}",
          blkMovingInfo.getBlock(), blkMovingInfo.getSource(),
          blkMovingInfo.getTarget(), blkMovingInfo.getTargetStorageType());
      return BlockMovementStatus.DN_BLK_STORAGE_MOVEMENT_SUCCESS;
    } catch (BlockPinningException e) {
      // 固定块不允许迁移，无需重试，直接标记成功跳过本次迁移
      LOG.debug("Pinned block can't be moved, so skipping block:{}",
          blkMovingInfo.getBlock(), e);
      return BlockMovementStatus.DN_BLK_STORAGE_MOVEMENT_SUCCESS;
    } finally {
      // 关闭所有流和socket，释放资源
      IOUtils.closeStream(out);
      IOUtils.closeStream(in);
      IOUtils.closeSocket(sock);
    }
  }

  /** Send a reportedBlock replace request to the output stream. */
  private static void sendRequest(DataOutputStream out, ExtendedBlock eb,
      Token<BlockTokenIdentifier> accessToken, DatanodeInfo source,
      StorageType targetStorageType) throws IOException {
    new Sender(out).replaceBlock(eb, targetStorageType, accessToken,
        source.getDatanodeUuid(), source, null);
  }

  /** Receive a reportedBlock copy response from the input stream. */
  private static void receiveResponse(DataInputStream in) throws IOException {
    BlockOpResponseProto response = BlockOpResponseProto
        .parseFrom(vintPrefixed(in));
    // 跳过所有中间进行中的响应，等待最终结果
    while (response.getStatus() == Status.IN_PROGRESS) {
      // read intermediate responses
      response = BlockOpResponseProto.parseFrom(vintPrefixed(in));
    }
    String logInfo = "reportedBlock move is failed";
    // 检查响应状态，非成功状态抛出对应异常
    DataTransferProtoUtil.checkBlockOpStatus(response, logInfo);
  }
}