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

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import java.util.ArrayList;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateRequestPBImpl;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.DistributedSchedulingAllocateRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.DistributedSchedulingAllocateRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateRequestProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateRequest;

import java.util.Iterator;
import java.util.List;

/**
 * 文件说明：分布式调度分配请求PB序列化实现，基于Protobuf实现分布式调度AllocateRequest协议对象的序列化与反序列化
 * 实现了 {@link DistributedSchedulingAllocateRequest} 接口。
 */
public class DistributedSchedulingAllocateRequestPBImpl
    extends DistributedSchedulingAllocateRequest {
  // Protobuf构建器，用于构造PB对象
  private DistributedSchedulingAllocateRequestProto.Builder builder = null;
  // 标记当前对象是否通过Proto构造
  private boolean viaProto = false;

  // 存储解析后的PB对象
  private DistributedSchedulingAllocateRequestProto proto;
  // 缓存标准分配请求对象
  private AllocateRequest allocateRequest;
  // 缓存已分配容器列表
  private List<Container> containers;

  /**
   * 构造函数，初始化空的PB构建器
   */
  public DistributedSchedulingAllocateRequestPBImpl() {
    builder = DistributedSchedulingAllocateRequestProto.newBuilder();
  }

  /**
   * 基于已有PB对象构造请求实现
   * @param proto 已有的分布式调度分配请求PB对象
   */
  public DistributedSchedulingAllocateRequestPBImpl(
      DistributedSchedulingAllocateRequestProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  @Override
  public AllocateRequest getAllocateRequest() {
    // 根据是否通过Proto构造选择对应对象
    DistributedSchedulingAllocateRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    // 如果已经缓存则直接返回
    if (this.allocateRequest != null) {
      return this.allocateRequest;
    }
    // PB中不存在该字段则返回null
    if (!p.hasAllocateRequest()) {
      return null;
    }
    // 从PB格式转换为API对象并缓存
    this.allocateRequest = convertFromProtoFormat(p.getAllocateRequest());
    return this.allocateRequest;
  }

  @Override
  public void setAllocateRequest(AllocateRequest pAllocateRequest) {
    // 确保构建器已初始化
    maybeInitBuilder();
    // 清空原有分配请求字段
    if (allocateRequest == null) {
      builder.clearAllocateRequest();
    }
    // 缓存新的分配请求对象
    this.allocateRequest = pAllocateRequest;
  }

  @Override
  public List<Container> getAllocatedContainers() {
    // 如果已经缓存则直接返回
    if (this.containers != null) {
      return this.containers;
    }
    // 从PB中解析容器列表
    initAllocatedContainers();
    return containers;
  }

  // 从PB中解析已分配容器列表并缓存
  private void initAllocatedContainers() {
    DistributedSchedulingAllocateRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    List<ContainerProto> list = p.getAllocatedContainersList();
    this.containers = new ArrayList<Container>();
    // 逐个转换PB容器对象为API容器对象
    for (ContainerProto c : list) {
      this.containers.add(convertFromProtoFormat(c));
    }
  }

  @Override
  public void setAllocatedContainers(List<Container> pContainers) {
    maybeInitBuilder();
    // 清空列表处理
    if (pContainers == null || pContainers.isEmpty()) {
      if (this.containers != null) {
        this.containers.clear();
      }
      builder.clearAllocatedContainers();
      return;
    }
    // 缓存新的容器列表
    this.containers = new ArrayList<>();
    this.containers.addAll(pContainers);
  }

  /**
   * 获取当前请求对应的PB对象，合并本地修改到PB后返回
   * @return 序列化后的分布式调度分配请求PB对象
   */
  public DistributedSchedulingAllocateRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 延迟初始化构建器，如果从PB构造则基于原有PB创建构建器
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = DistributedSchedulingAllocateRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // 将本地修改合并到PB对象
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 将本地缓存的API对象合并到PB构建器中
  private void mergeLocalToBuilder() {
    // 合并容器列表
    if (this.containers != null) {
      builder.clearAllocatedContainers();
      Iterable<ContainerProto> iterable =
          getContainerProtoIterable(this.containers);
      builder.addAllAllocatedContainers(iterable);
    }
    // 合并分配请求
    if (this.allocateRequest != null) {
      builder.setAllocateRequest(
          ((AllocateRequestPBImpl)this.allocateRequest).getProto());
    }
  }

  // 获取将容器列表转换为PB容器迭代器的可迭代对象
  private Iterable<ContainerProto> getContainerProtoIterable(
      final List<Container> newContainersList) {
    maybeInitBuilder();
    return new Iterable<ContainerProto>() {
      @Override
      public synchronized Iterator<ContainerProto> iterator() {
        return new Iterator<ContainerProto>() {
          Iterator<Container> iter = newContainersList.iterator();

          @Override
          public synchronized boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public synchronized ContainerProto next() {
            // 将API容器对象转换为PB格式
            return ProtoUtils.convertToProtoFormat(iter.next());
          }

          @Override
          public synchronized void remove() {
            throw new UnsupportedOperationException();

          }
        };
      }
    };
  }

  // 将PB容器对象转换为API容器对象
  private ContainerPBImpl convertFromProtoFormat(ContainerProto p) {
    return new ContainerPBImpl(p);
  }

  // 将PB分配请求对象转换为API分配请求对象
  private AllocateRequestPBImpl convertFromProtoFormat(AllocateRequestProto p) {
    return new AllocateRequestPBImpl(p);
  }
}