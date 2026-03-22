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

package org.apache.hadoop.hdfs.protocolPB;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.HeartbeatRequestProto;
import org.apache.hadoop.hdfs.server.protocol.DatanodeLifelineProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hadoop.thirdparty.protobuf.RpcController;

import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.ipc;

/**
 * 数据节点生命线协议客户端侧PB转换器，将DatanodeLifelineProtocol接口的请求转换为Protobuf格式RPC调用，
 * 转发给实现了DatanodeLifelineProtocolPB接口的NameNode服务端。
 * 生命线协议用于数据节点在常规心跳超时情况下，发送紧急存活状态报告给NameNode。
 */
@InterfaceAudience.Private
public class DatanodeLifelineProtocolClientSideTranslatorPB implements
    ProtocolMetaInterface, DatanodeLifelineProtocol, Closeable {

  /** RpcController is not used and hence is set to null. */
  private static final RpcController NULL_CONTROLLER = null;

  private final DatanodeLifelineProtocolPB rpcProxy;

  /**
   * 构造客户端侧PB转换器，初始化RPC代理连接到NameNode
   * @param nameNodeAddr NameNode服务地址
   * @param conf Hadoop配置对象
   * @throws IOException 创建RPC连接失败时抛出异常
   */
  public DatanodeLifelineProtocolClientSideTranslatorPB(
      InetSocketAddress nameNodeAddr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, DatanodeLifelineProtocolPB.class,
        ProtobufRpcEngine2.class);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    rpcProxy = createNamenode(nameNodeAddr, conf, ugi);
  }

  /**
   * 创建生命线协议的NameNode RPC代理对象
   * @param nameNodeAddr NameNode服务地址
   * @param conf Hadoop配置对象
   * @param ugi 当前用户身份信息
   * @return 生命线协议PB接口的RPC代理
   * @throws IOException 创建代理失败时抛出异常
   */
  private static DatanodeLifelineProtocolPB createNamenode(
      InetSocketAddress nameNodeAddr, Configuration conf,
      UserGroupInformation ugi) throws IOException {
    return RPC.getProxy(DatanodeLifelineProtocolPB.class,
        RPC.getProtocolVersion(DatanodeLifelineProtocolPB.class), nameNodeAddr,
        ugi, conf,
        NetUtils.getSocketFactory(conf, DatanodeLifelineProtocolPB.class));
  }

  @Override
  public void close() throws IOException {
    // 关闭RPC代理，释放连接资源
    RPC.stopProxy(rpcProxy);
  }

  /**
   * 发送数据节点生命线报告给NameNode，上报数据节点当前状态和存活信息
   * @param registration 数据节点注册信息
   * @param reports 各存储块报告数组
   * @param cacheCapacity 缓存总容量
   * @param cacheUsed 已使用缓存大小
   * @param xmitsInProgress 进行中的块传输数量
   * @param xceiverCount 数据节点服务线程数
   * @param failedVolumes 失败卷数量
   * @param volumeFailureSummary 卷失败汇总信息
   * @throws IOException RPC调用失败时抛出异常
   */
  @Override
  public void sendLifeline(DatanodeRegistration registration,
      StorageReport[] reports, long cacheCapacity, long cacheUsed,
      int xmitsInProgress, int xceiverCount, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary) throws IOException {
    // 构建Protobuf格式的心跳请求对象（生命线请求复用心跳请求结构）
    HeartbeatRequestProto.Builder builder = HeartbeatRequestProto.newBuilder()
        .setRegistration(PBHelper.convert(registration))
        .setXmitsInProgress(xmitsInProgress).setXceiverCount(xceiverCount)
        .setFailedVolumes(failedVolumes);
    // 转换并添加存储报告
    builder.addAllReports(PBHelperClient.convertStorageReports(reports));
    // 非0时添加缓存容量字段
    if (cacheCapacity != 0) {
      builder.setCacheCapacity(cacheCapacity);
    }
    // 非0时添加已使用缓存字段
    if (cacheUsed != 0) {
      builder.setCacheUsed(cacheUsed);
    }
    // 不为空时添加卷失败汇总信息
    if (volumeFailureSummary != null) {
      builder.setVolumeFailureSummary(PBHelper.convertVolumeFailureSummary(
          volumeFailureSummary));
    }
    // 执行RPC调用发送生命线请求
    ipc(() -> rpcProxy.sendLifeline(NULL_CONTROLLER, builder.build()));
  }

  @Override // ProtocolMetaInterface
  public boolean isMethodSupported(String methodName)
      throws IOException {
    // 检查指定方法是否在远程RPC服务端被支持
    return RpcClientUtil.isMethodSupported(rpcProxy,
        DatanodeLifelineProtocolPB.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(DatanodeLifelineProtocolPB.class), methodName);
  }
}