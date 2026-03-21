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

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.yarn.api.records.ReservationAllocationState;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ResourceAllocationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationDefinitionPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationDefinitionProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceAllocationRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 预约系统工具类，提供不同数据格式间的转换方法，主要用于测试场景
 */
public final class ReservationSystemUtil {

  private ReservationSystemUtil() {
    // 工具类不允许实例化
  }

  /**
   * 根据预约请求计算总资源量
   * @param request 预约请求
   * @return 总资源量
   */
  public static Resource toResource(ReservationRequest request) {
    Resource resource = Resources.multiply(request.getCapability(),
        (float) request.getNumContainers());
    return resource;
  }

  /**
   * 将按时间区间的预约请求转换为按时间区间的总资源
   * @param allocations 按时间区间的预约请求映射
   * @return 按时间区间的总资源映射
   */
  public static Map<ReservationInterval, Resource> toResources(
      Map<ReservationInterval, ReservationRequest> allocations) {
    Map<ReservationInterval, Resource> resources =
        new HashMap<ReservationInterval, Resource>();
    for (Map.Entry<ReservationInterval, ReservationRequest> entry :
        allocations.entrySet()) {
      resources.put(entry.getKey(),
          toResource(entry.getValue()));
    }
    return resources;
  }

  /**
   * 将预约分配对象转换为ProtoBuf格式
   * @param allocation 预约分配对象
   * @return ProtoBuf格式的预约分配状态
   */
  public static ReservationAllocationStateProto buildStateProto(
      ReservationAllocation allocation) {
    ReservationAllocationStateProto.Builder builder =
        ReservationAllocationStateProto.newBuilder();

    // 设置预约接受时间
    builder.setAcceptanceTime(allocation.getAcceptanceTime());
    // 设置是否包含任务组
    builder.setContainsGangs(allocation.containsGangs());
    // 设置开始时间
    builder.setStartTime(allocation.getStartTime());
    // 设置结束时间
    builder.setEndTime(allocation.getEndTime());
    // 设置提交用户
    builder.setUser(allocation.getUser());
    // 转换预约定义为Proto格式
    ReservationDefinitionProto definitionProto = convertToProtoFormat(
        allocation.getReservationDefinition());
    builder.setReservationDefinition(definitionProto);

    // 遍历所有时间区间分配，逐个转换为Proto格式
    for (Map.Entry<ReservationInterval, Resource> entry :
        allocation.getAllocationRequests().entrySet()) {
      ResourceAllocationRequestProto p =
          ResourceAllocationRequestProto.newBuilder()
          .setStartTime(entry.getKey().getStartTime())
          .setEndTime(entry.getKey().getEndTime())
          .setResource(convertToProtoFormat(entry.getValue()))
          .build();
      builder.addAllocationRequests(p);
    }

    ReservationAllocationStateProto allocationProto = builder.build();
    return allocationProto;
  }

  /**
   * 将预约定义转换为ProtoBuf格式
   * @param reservationDefinition 预约定义对象
   * @return ProtoBuf格式的预约定义
   */
  private static ReservationDefinitionProto convertToProtoFormat(
      ReservationDefinition reservationDefinition) {
    return ((ReservationDefinitionPBImpl)reservationDefinition).getProto();
  }

  /**
   * 将资源对象转换为ProtoBuf格式
   * @param e 资源对象
   * @return ProtoBuf格式的资源
   */
  public static ResourceProto convertToProtoFormat(Resource e) {
    return YarnProtos.ResourceProto.newBuilder()
        .setMemory(e.getMemorySize())
        .setVirtualCores(e.getVirtualCores())
        .build();
  }

  /**
   * 将ProtoBuf格式的分配请求列表转换为内存时间区间-资源映射
   * @param allocationRequestsList ProtoBuf格式的分配请求列表
   * @return 时间区间-资源映射
   */
  public static Map<ReservationInterval, Resource> toAllocations(
      List<ResourceAllocationRequestProto> allocationRequestsList) {
    Map<ReservationInterval, Resource> allocations = new HashMap<>();
    for (ResourceAllocationRequestProto proto : allocationRequestsList) {
      allocations.put(
          new ReservationInterval(proto.getStartTime(), proto.getEndTime()),
          convertFromProtoFormat(proto.getResource()));
    }
    return allocations;
  }

  /**
   * 从ProtoBuf格式转换为资源对象
   * @param resource ProtoBuf格式的资源
   * @return 资源对象
   */
  private static ResourcePBImpl convertFromProtoFormat(ResourceProto resource) {
    return new ResourcePBImpl(resource);
  }

  /**
   * 从ProtoBuf格式转换为预约定义对象
   * @param r ProtoBuf格式的预约定义
   * @return 预约定义对象
   */
  public static ReservationDefinitionPBImpl convertFromProtoFormat(
      ReservationDefinitionProto r) {
    return new ReservationDefinitionPBImpl(r);
  }

  /**
   * 从ProtoBuf格式转换为预约ID对象
   * @param r ProtoBuf格式的预约ID
   * @return 预约ID对象
   */
  public static ReservationIdPBImpl convertFromProtoFormat(
      ReservationIdProto r) {
    return new ReservationIdPBImpl(r);
  }

  /**
   * 从ProtoBuf格式转换为预约ID对象
   * @param reservationId ProtoBuf格式的预约ID
   * @return 预约ID对象
   */
  public static ReservationId toReservationId(
      ReservationIdProto reservationId) {
    return new ReservationIdPBImpl(reservationId);
  }

  /**
   * 将ProtoBuf格式的预约分配转换为内存预约分配对象
   * @param planName 计划名称
   * @param reservationId 预约ID
   * @param allocationState ProtoBuf格式的预约分配状态
   * @param minAlloc 最小分配单元
   * @param planResourceCalculator 资源计算器
   * @return 内存预约分配对象
   */
  public static InMemoryReservationAllocation toInMemoryAllocation(
          String planName, ReservationId reservationId,
          ReservationAllocationStateProto allocationState, Resource minAlloc,
          ResourceCalculator planResourceCalculator) {
    // 转换预约定义
    ReservationDefinition definition =
        convertFromProtoFormat(
            allocationState.getReservationDefinition());
    // 转换资源分配映射
    Map<ReservationInterval, Resource> allocations = toAllocations(
            allocationState.getAllocationRequestsList());
    // 构造内存预约分配对象
    InMemoryReservationAllocation allocation =
        new InMemoryReservationAllocation(reservationId, definition,
        allocationState.getUser(), planName, allocationState.getStartTime(),
        allocationState.getEndTime(), allocations, planResourceCalculator,
        minAlloc, allocationState.getContainsGangs());
    return allocation;
  }

  /**
   * 将预约分配集合转换为预约分配状态列表
   * @param res 预约分配集合
   * @param includeResourceAllocations 是否包含资源分配信息
   * @return 预约分配状态列表
   */
  public static List<ReservationAllocationState>
        convertAllocationsToReservationInfo(Set<ReservationAllocation> res,
                        boolean includeResourceAllocations) {
    List<ReservationAllocationState> reservationInfo = new ArrayList<>();

    Map<ReservationInterval, Resource> requests;
    for (ReservationAllocation allocation : res) {
      List<ResourceAllocationRequest> allocations = new ArrayList<>();
      if (includeResourceAllocations) {
        requests = allocation.getAllocationRequests();

        // 遍历所有分配，构造资源分配请求
        for (Map.Entry<ReservationInterval, Resource> request :
                requests.entrySet()) {
          ReservationInterval interval = request.getKey();
          allocations.add(ResourceAllocationRequest.newInstance(
                  interval.getStartTime(), interval.getEndTime(),
                  request.getValue()));
        }
      }

      // 构造并添加预约分配状态
      reservationInfo.add(ReservationAllocationState.newInstance(
              allocation.getAcceptanceTime(), allocation.getUser(),
              allocations, allocation.getReservationId(),
              allocation.getReservationDefinition()));
    }
    return reservationInfo;
  }
}