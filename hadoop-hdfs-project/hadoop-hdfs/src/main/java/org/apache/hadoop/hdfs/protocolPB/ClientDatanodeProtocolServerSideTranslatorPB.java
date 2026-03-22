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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeVolumeInfo;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.DeleteBlockPoolRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.DeleteBlockPoolResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.EvictWritersRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.EvictWritersResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetBalancerBandwidthRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetBalancerBandwidthResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetBlockLocalPathInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetBlockLocalPathInfoResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetDatanodeInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetDatanodeInfoResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.GetReconfigurationStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.GetReconfigurationStatusResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetReplicaVisibleLengthRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetReplicaVisibleLengthResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetVolumeReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetVolumeReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.GetVolumeReportResponseProto.Builder;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.ListReconfigurablePropertiesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.ListReconfigurablePropertiesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.RefreshNamenodesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.RefreshNamenodesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.ShutdownDatanodeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.ShutdownDatanodeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.StartReconfigurationRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.StartReconfigurationResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.TriggerBlockReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.TriggerBlockReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeVolumeInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.SubmitDiskBalancerPlanRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.SubmitDiskBalancerPlanResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.CancelPlanRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.CancelPlanResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.QueryPlanStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.QueryPlanStatusResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.DiskBalancerSettingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos.DiskBalancerSettingResponseProto;
import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.apache.hadoop.hdfs.server.datanode.DiskBalancerWorkStatus;
import org.apache.hadoop.net.NetUtils;

/**
 * 文件：ClientDatanodeProtocolServerSideTranslatorPB.java
 * 所属模块：HDFS 协议PB转换层
 * 核心职责：将Protobuf格式的ClientDatanodeProtocol RPC请求转换为原始Java接口调用，
 *           并将返回结果封装回Protobuf格式响应，实现RPC协议的序列化/反序列化转换，
 *           负责转发请求到DataNode本地的ClientDatanodeProtocol实现。
 */
/**
 * Implementation for protobuf service that forwards requests
 * received on {@link ClientDatanodeProtocolPB} to the
 * {@link ClientDatanodeProtocol} server implementation.
 */
@InterfaceAudience.Private
public class ClientDatanodeProtocolServerSideTranslatorPB implements
    ClientDatanodeProtocolPB {
  // 预构建的空响应对象：刷新NameNode列表响应
  private final static RefreshNamenodesResponseProto REFRESH_NAMENODE_RESP =
      RefreshNamenodesResponseProto.newBuilder().build();
  // 预构建的空响应对象：删除块池响应
  private final static DeleteBlockPoolResponseProto DELETE_BLOCKPOOL_RESP =
      DeleteBlockPoolResponseProto.newBuilder().build();
  // 预构建的空响应对象：关闭DataNode响应
  private final static ShutdownDatanodeResponseProto SHUTDOWN_DATANODE_RESP =
      ShutdownDatanodeResponseProto.newBuilder().build();
  // 预构建的空响应对象：启动配置重新加载响应
  private final static StartReconfigurationResponseProto START_RECONFIG_RESP =
      StartReconfigurationResponseProto.newBuilder().build();
  // 预构建的空响应对象：触发块报告响应
  private final static TriggerBlockReportResponseProto TRIGGER_BLOCK_REPORT_RESP =
      TriggerBlockReportResponseProto.newBuilder().build();
  // 预构建的空响应对象：驱逐写入者响应
  private final static EvictWritersResponseProto EVICT_WRITERS_RESP =
      EvictWritersResponseProto.newBuilder().build();
  
  // 原始ClientDatanodeProtocol服务实现实例
  private final ClientDatanodeProtocol impl;

  /**
   * 构造函数，注入原始ClientDatanodeProtocol服务实现
   * @param impl 原始ClientDatanodeProtocol服务实现
   */
  public ClientDatanodeProtocolServerSideTranslatorPB(
      ClientDatanodeProtocol impl) {
    this.impl = impl;
  }

  /**
   * 获取副本可见长度RPC请求处理
   * @param unused RPC控制器
   * @param request Protobuf格式请求
   * @return Protobuf格式响应
   * @throws ServiceException 服务异常
   */
  @Override
  public GetReplicaVisibleLengthResponseProto getReplicaVisibleLength(
      RpcController unused, GetReplicaVisibleLengthRequestProto request)
      throws ServiceException {
    long len;
    try {
      // 将Protobuf格式块信息转换为原始对象，调用服务实现
      len = impl.getReplicaVisibleLength(PBHelperClient.convert(request.getBlock()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    // 封装结果为Protobuf格式响应返回
    return GetReplicaVisibleLengthResponseProto.newBuilder().setLength(len)
        .build();
  }

  /**
   * 刷新NameNode列表RPC请求处理
   * @param unused RPC控制器
   * @param request Protobuf格式请求
   * @return Protobuf格式响应
   * @throws ServiceException 服务异常
   */
  @Override
  public RefreshNamenodesResponseProto refreshNamenodes(
      RpcController unused, RefreshNamenodesRequestProto request)
      throws ServiceException {
    try {
      impl.refreshNamenodes();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return REFRESH_NAMENODE_RESP;
  }

  /**
   * 删除块池RPC请求处理
   * @param unused RPC控制器
   * @param request Protobuf格式请求
   * @return Protobuf格式响应
   * @throws ServiceException 服务异常
   */
  @Override
  public DeleteBlockPoolResponseProto deleteBlockPool(RpcController unused,
      DeleteBlockPoolRequestProto request) throws ServiceException {
    try {
      // 从请求中提取参数，调用服务实现
      impl.deleteBlockPool(request.getBlockPool(), request.getForce());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return DELETE_BLOCKPOOL_RESP;
  }

  /**
   * 获取块本地路径信息RPC请求处理
   * @param unused RPC控制器
   * @param request Protobuf格式请求
   * @return Protobuf格式响应
   * @throws ServiceException 服务异常
   */
  @Override
  public GetBlockLocalPathInfoResponseProto getBlockLocalPathInfo(
      RpcController unused, GetBlockLocalPathInfoRequestProto request)
      throws ServiceException {
    BlockLocalPathInfo resp;
    try {
      // 将Protobuf格式的块和访问令牌转换为原始对象，调用服务实现
      resp = impl.getBlockLocalPathInfo(
                 PBHelperClient.convert(request.getBlock()),
                 PBHelperClient.convert(request.getToken()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    // 将原始结果转换为Protobuf格式返回
    return GetBlockLocalPathInfoResponseProto.newBuilder()
        .setBlock(PBHelperClient.convert(resp.getBlock()))
        .setLocalPath(resp.getBlockPath()).setLocalMetaPath(resp.getMetaPath())
        .build();
  }

  /**
   * 关闭DataNodeRPC请求处理
   * @param unused RPC控制器
   * @param request Protobuf格式请求
   * @return Protobuf格式响应
   * @throws ServiceException 服务异常
   */
  @Override
  public ShutdownDatanodeResponseProto shutdownDatanode(
      RpcController unused, ShutdownDatanodeRequestProto request)
      throws ServiceException {
    try {
      impl.shutdownDatanode(request.getForUpgrade());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return SHUTDOWN_DATANODE_RESP;
  }

  /**
   * 驱逐所有写入者RPC请求处理
   * @param unused RPC控制器
   * @param request Protobuf格式请求
   * @return Protobuf格式响应
   * @throws ServiceException 服务异常
   */
  @Override
  public EvictWritersResponseProto evictWriters(RpcController unused,
      EvictWritersRequestProto request) throws ServiceException {
    try {
      impl.evictWriters();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return EVICT_WRITERS_RESP;
  }

  /**
   * 获取DataNode信息RPC请求处理
   * @param unused RPC控制器
   * @param request Protobuf格式请求
   * @return Protobuf格式响应
   * @throws ServiceException 服务异常
   */
  public GetDatanodeInfoResponseProto getDatanodeInfo(RpcController unused,
      GetDatanodeInfoRequestProto request) throws ServiceException {
    GetDatanodeInfoResponseProto res;
    try {
      // 调用服务实现获取结果，转换为Protobuf格式
      res = GetDatanodeInfoResponseProto.newBuilder()
          .setLocalInfo(PBHelperClient.convert(impl.getDatanodeInfo())).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return res;
  }

  /**
   * 启动配置重新加载RPC请求处理
   * @param unused RPC控制器
   * @param request Protobuf格式请求
   * @return Protobuf格式响应
   * @throws ServiceException 服务异常
   */
  @Override
  public StartReconfigurationResponseProto startReconfiguration(
      RpcController unused, StartReconfigurationRequestProto request)
      throws ServiceException {
    try {
      impl.startReconfiguration();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return START_RECONFIG_RESP;
  }

  /**
   * 列出可重新配置属性RPC请求处理
   * @param controller RPC控制器
   * @param request Protobuf格式请求
   * @return Protobuf格式响应
   * @throws ServiceException 服务异常
   */
  @Override
  public ListReconfigurablePropertiesResponseProto listReconfigurableProperties(
      RpcController controller,
      ListReconfigurablePropertiesRequestProto request)
      throws ServiceException {
    try {
      // 调用公共工具类完成转换和响应封装
      return ReconfigurationProtocolServerSideUtils
          .listReconfigurableProperties(impl.listReconfigurableProperties());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * 获取配置重新加载状态RPC请求处理
   * @param unused RPC控制器
   * @param request Protobuf格式请求
   * @return Protobuf格式响应
   * @throws ServiceException 服务异常
   */
  @Override
  public GetReconfigurationStatusResponseProto getReconfigurationStatus(
      RpcController unused, GetReconfigurationStatusRequestProto request)
      throws ServiceException {
    try {
      // 调用公共工具类完成转换和响应封装
      return ReconfigurationProtocolServerSideUtils
          .getReconfigurationStatus(impl.getReconfigurationStatus());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * 触发块报告RPC请求处理
   * @param unused RPC控制器
   * @param request Protobuf格式请求
   * @return Protobuf格式响应
   * @throws ServiceException 服务异常
   */
  @Override
  public TriggerBlockReportResponseProto triggerBlockReport(
      RpcController unused, TriggerBlockReportRequestProto request)
          throws ServiceException {
    try {
      // 根据请求参数构建块报告选项
      BlockReportOptions.Factory factory = new BlockReportOptions.Factory().
          setIncremental(request.getIncremental());
      if (request.hasNnAddress()) {
        factory.setNamenodeAddr(NetUtils.createSocketAddr(request.getNnAddress()));
      }
      impl.triggerBlockReport(factory.build());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return TRIGGER_BLOCK_REPORT_RESP;
  }

  /**
   * 获取Balancer带宽RPC请求处理
   * @param controller RPC控制器
   * @param request Protobuf格式请求
   * @return Protobuf格式响应
   * @throws ServiceException 服务异常
   */
  @Override
  public GetBalancerBandwidthResponseProto getBalancerBandwidth(
      RpcController controller, GetBalancerBandwidthRequestProto request)
      throws ServiceException {
    long bandwidth;
    try {
      bandwidth = impl.getBalancerBandwidth();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return GetBalancerBandwidthResponseProto.newBuilder()
        .setBandwidth(bandwidth).build();
  }

  /**
   * Submit a disk balancer plan for execution.
   * @param controller  - RpcController
   * @param request   - Request
   * @return   Response
   * @throws ServiceException
   */
  /**
   * 提交磁盘均衡计划执行RPC请求处理
   * @param controller RPC控制器
   * @param request Protobuf格式请求
   * @return Protobuf格式响应
   * @throws ServiceException 服务异常
   */
  @Override
  public SubmitDiskBalancerPlanResponseProto submitDiskBalancerPlan(
      RpcController controller, SubmitDiskBalancerPlanRequestProto request)
      throws ServiceException {
    try {
      // 提取请求参数，处理缺省值，调用服务实现
      impl.submitDiskBalancerPlan(request.getPlanID(),
          request.hasPlanVersion() ? request.getPlanVersion() : 1,
          request.hasPlanFile() ? request.getPlanFile() : "",
          request.getPlan(),
          request.hasIgnoreDateCheck() ? request.getIgnoreDateCheck() : false);
      SubmitDiskBalancerPlanResponseProto response =
          SubmitDiskBalancerPlanResponseProto.newBuilder()
              .build();
      return response;
    } catch(Exception e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Cancel an executing plan.
   * @param controller - RpcController
   * @param request  - Request
   * @return Response.
   * @throws ServiceException
   */
  /**
   * 取消正在执行的磁盘均衡计划RPC请求处理
   * @param controller RPC控制器
   * @param request Protobuf格式请求
   * @return Protobuf格式响应
   * @throws ServiceException 服务异常
   */