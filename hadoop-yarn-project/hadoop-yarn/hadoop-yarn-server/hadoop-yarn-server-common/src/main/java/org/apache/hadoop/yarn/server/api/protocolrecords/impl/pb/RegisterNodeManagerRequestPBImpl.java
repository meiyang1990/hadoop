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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeLabelPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeAttributePBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeLabelProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeAttributeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.LogAggregationReportProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NMContainerStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeLabelsProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeLabelsProto.Builder;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeAttributesProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.impl.pb.NodeStatusPBImpl;

/**
 * NodeManager向ResourceManager注册请求的Protobuf实现类
 * 实现了基于Protobuf的序列化与反序列化，用于YARN RPC通信
 */
public class RegisterNodeManagerRequestPBImpl extends RegisterNodeManagerRequest {
  RegisterNodeManagerRequestProto proto = RegisterNodeManagerRequestProto.getDefaultInstance();
  RegisterNodeManagerRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private Resource resource = null;
  private NodeId nodeId = null;
  private List<NMContainerStatus> containerStatuses = null;
  private List<ApplicationId> runningApplications = null;
  private Set<NodeLabel> labels = null;
  private Set<NodeAttribute> attributes = null;

  private List<LogAggregationReport> logAggregationReportsForApps = null;

  /** Physical resources in the node. */
  private Resource physicalResource = null;
  private NodeStatus nodeStatus;

  public RegisterNodeManagerRequestPBImpl() {
    builder = RegisterNodeManagerRequestProto.newBuilder();
  }

  public RegisterNodeManagerRequestPBImpl(RegisterNodeManagerRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  /** 获取序列化后的Protobuf对象 */
  public synchronized RegisterNodeManagerRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /** 将本地缓存的对象合并到Protobuf Builder中 */
  private synchronized void mergeLocalToBuilder() {
    if (this.containerStatuses != null) {
      addNMContainerStatusesToProto();
    }
    if (this.runningApplications != null) {
      addRunningApplicationsToProto();
    }
    if (this.resource != null) {
      builder.setResource(convertToProtoFormat(this.resource));
    }
    if (this.nodeId != null) {
      builder.setNodeId(convertToProtoFormat(this.nodeId));
    }
    if (this.labels != null) {
      builder.clearNodeLabels();
      Builder newBuilder = NodeLabelsProto.newBuilder();
      for (NodeLabel label : labels) {
        newBuilder.addNodeLabels(convertToProtoFormat(label));
      }
      builder.setNodeLabels(newBuilder.build());
    }
    if (this.attributes != null) {
      builder.clearNodeAttributes();
      NodeAttributesProto.Builder attributesBuilder =
          NodeAttributesProto.newBuilder();
      for (NodeAttribute attribute : attributes) {
        attributesBuilder.addNodeAttributes(convertToProtoFormat(attribute));
      }
      builder.setNodeAttributes(attributesBuilder.build());
    }
    if (this.physicalResource != null) {
      builder.setPhysicalResource(convertToProtoFormat(this.physicalResource));
    }
    if (this.logAggregationReportsForApps != null) {
      addLogAggregationStatusForAppsToProto();
    }
    if (this.nodeStatus != null) {
      builder.setNodeStatus(convertToProtoFormat(this.nodeStatus));
    }
  }

  /** 将日志聚合报告转换写入Protobuf Builder */
  private void addLogAggregationStatusForAppsToProto() {
    maybeInitBuilder();
    builder.clearLogAggregationReportsForApps();
    if (this.logAggregationReportsForApps == null) {
      return;
    }
    Iterable<LogAggregationReportProto> it =
        new Iterable<LogAggregationReportProto>() {
          @Override
          public Iterator<LogAggregationReportProto> iterator() {
            return new Iterator<LogAggregationReportProto>() {
              private Iterator<LogAggregationReport> iter =
                  logAggregationReportsForApps.iterator();

              @Override
              public boolean hasNext() {
                return iter.hasNext();
              }

              @Override
              public LogAggregationReportProto next() {
                return convertToProtoFormat(iter.next());
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }
            };
          }
        };
    builder.addAllLogAggregationReportsForApps(it);
  }

  private LogAggregationReportProto convertToProtoFormat(
      LogAggregationReport value) {
    return ((LogAggregationReportPBImpl) value).getProto();
  }

  /** 将容器状态列表转换写入Protobuf Builder */
  private synchronized void addNMContainerStatusesToProto() {
    maybeInitBuilder();
    builder.clearContainerStatuses();
    List<NMContainerStatusProto> list =
        new ArrayList<NMContainerStatusProto>();
    for (NMContainerStatus status : this.containerStatuses) {
      list.add(convertToProtoFormat(status));
    }
    builder.addAllContainerStatuses(list);
  }

    
  /** 将本地缓存合并生成最终Protobuf对象 */
  private synchronized void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /** 初始化Builder，确保可以修改Proto */
  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RegisterNodeManagerRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public synchronized Resource getResource() {
    RegisterNodeManagerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.resource != null) {
      return this.resource;
    }
    if (!p.hasResource()) {
      return null;
    }
    this.resource = convertFromProtoFormat(p.getResource());
    return this.resource;
  }

  @Override
  public synchronized void setResource(Resource resource) {
    maybeInitBuilder();
    if (resource == null) 
      builder.clearResource();
    this.resource = resource;
  }

  @Override
  public synchronized NodeId getNodeId() {
    RegisterNodeManagerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.nodeId != null) {
      return this.nodeId;
    }
    if (!p.hasNodeId()) {
      return null;
    }
    this.nodeId = convertFromProtoFormat(p.getNodeId());
    return this.nodeId;
  }

  @Override
  public synchronized void setNodeId(NodeId nodeId) {
    maybeInitBuilder();
    if (nodeId == null) {
      builder.clearNodeId();
    }
    this.nodeId = nodeId;
  }

  @Override
  public synchronized int getHttpPort() {
    RegisterNodeManagerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasHttpPort()) {
      return 0;
    }
    return (p.getHttpPort());
  }

  @Override
  public synchronized void setHttpPort(int httpPort) {
    maybeInitBuilder();
    builder.setHttpPort(httpPort);
  }
  
  @Override
  public synchronized List<ApplicationId> getRunningApplications() {
    initRunningApplications();
    return runningApplications;
  }
  
  /** 从Protobuf中反序列化初始化运行中应用列表 */
  private synchronized void initRunningApplications() {
    if (this.runningApplications != null) {
      return;
    }
    RegisterNodeManagerRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<ApplicationIdProto> list = p.getRunningApplicationsList();
    this.runningApplications = new ArrayList<ApplicationId>();
    for (ApplicationIdProto c : list) {
      this.runningApplications.add(convertFromProtoFormat(c));
    }
  }

  @Override
  public synchronized void setRunningApplications(List<ApplicationId> apps) {
    if (apps == null) {
      return;
    }
    initRunningApplications();
    this.runningApplications.addAll(apps);
  }
  
  /** 将运行中应用列表转换写入Protobuf Builder */
  private synchronized void addRunningApplicationsToProto() {
    maybeInitBuilder();
    builder.clearRunningApplications();
    if (runningApplications == null) {
      return;
    }
    Iterable<ApplicationIdProto> it = new Iterable<ApplicationIdProto>() {
      
      @Override
      public Iterator<ApplicationIdProto> iterator() {
        return new Iterator<ApplicationIdProto>() {
          Iterator<ApplicationId> iter = runningApplications.iterator();
          
          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }
          
          @Override
          public ApplicationIdProto next() {
            return convertToProtoFormat(iter.next());  
          }
          
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
    builder.addAllRunningApplications(it);
  }

  @Override
  public synchronized List<NMContainerStatus> getNMContainerStatuses() {
    initContainerRecoveryReports();
    return containerStatuses;
  }
  
  /** 从Protobuf中反序列化初始化容器状态列表 */
  private synchronized void initContainerRecoveryReports() {
    if (this.containerStatuses != null) {
      return;
    }
    RegisterNodeManagerRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<NMContainerStatusProto> list = p.getContainerStatusesList();
    this.containerStatuses = new ArrayList<NMContainerStatus>();
    for (NMContainerStatusProto c : list) {
      this.containerStatuses.add(convertFromProtoFormat(c));
    }
  }

  @Override
  public synchronized void setContainerStatuses(
      List<NMContainerStatus> containerReports) {
    if (containerReports == null) {
      return;
    }
    initContainerRecoveryReports();
    this.containerStatuses.addAll(containerReports);
  }

  @Override
  public synchronized Resource getPhysicalResource() {
    RegisterNodeManagerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.physicalResource != null) {
      return this.physicalResource;
    }
    if (!p.hasPhysicalResource()) {
      return null;
    }
    this.physicalResource = convertFromProtoFormat(p.getPhysicalResource());
    return this.physicalResource;
  }

  @Override
  public synchronized void setPhysicalResource(Resource pPhysicalResource) {
    maybeInitBuilder();
    if (pPhysicalResource == null) {
      builder.clearPhysicalResource();
    }
    this.physicalResource = pPhysicalResource;
  }

  @Override
  public synchronized NodeStatus getNodeStatus() {
    RegisterNodeManagerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.nodeStatus != null) {
      return this.nodeStatus;
    }
    if (!p.hasNodeStatus()) {
      return null;
    }
    this.nodeStatus = convertFromProtoFormat(p.getNodeStatus());
    return this.nodeStatus;
  }

  @Override
  public synchronized void setNodeStatus(NodeStatus pNodeStatus) {
    maybeInitBuilder();
    if (pNodeStatus == null) {
      builder.clearNodeStatus();
    }
    this.nodeStatus = pNodeStatus;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }
  
  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }
  
  @Override
  public synchronized String getNMVersion() {
    RegisterNodeManagerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNmVersion()) {
      return "";
    }
    return (p.getNmVersion());
  }

  @Override
  public synchronized void setNMVersion(String version) {
    maybeInitBuilder();
    builder.setNmVersion(version);
  }
  
  @Override
  public synchronized Set<NodeLabel> getNodeLabels() {
    initNodeLabels();
    return this.labels;
  }

  @Override
  public synchronized void setNodeLabels(Set<NodeLabel> nodeLabels) {
    maybeInitBuilder();
    builder.clearNodeLabels();
    this.labels = nodeLabels;
  }
  
  /** 从Protobuf中反序列化初始化节点标签集合 */
  private synchronized void initNodeLabels() {
    if (this.labels != null) {
      return;
    }
    RegisterNodeManagerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNodeLabels()) {
      labels=null;
      return;
    }
    NodeLabelsProto nodeLabels = p.getNodeLabels();
    labels = new HashSet<NodeLabel>();
    for(NodeLabelProto nlp : nodeLabels.getNodeLabelsList()) {
      labels.add(convertFromProtoFormat(nlp));
    }
  }

  @Override
  public synchronized Set<NodeAttribute> getNodeAttributes() {
    initNodeAttributes();
    return this.attributes;
  }

  @Override
  public synchronized void setNodeAttributes(
      Set<NodeAttribute> nodeAttributes) {
    maybeInitBuilder();
    builder.clearNodeAttributes();
    this.attributes = nodeAttributes;
  }

  /** 从Protobuf中反序列化初始化节点属性集合 */
  private synchronized void initNodeAttributes() {
    if (this.attributes != null) {
      return;
    }
    RegisterNodeManagerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNodeAttributes()) {
      attributes=null;
      return;
    }
    NodeAttributesProto nodeAttributes = p.getNodeAttributes();
    attributes = new HashSet<>();
    for(NodeAttributeProto nap : nodeAttributes.getNodeAttributesList()) {
      attributes.add(convertFromProtoFormat(nap));
    }
  }

  private static NodeLabelPBImpl convertFromProtoFormat(NodeLabelProto p) {
    return new NodeLabelPBImpl(p);
  }

  private static NodeLabelProto convertToProtoFormat(NodeLabel t) {
    return ((NodeLabelPBImpl)t).getProto();
  }

  private static NodeAttributePBImpl convertFromProtoFormat(
      NodeAttributeProto p) {
    return new NodeAttributePBImpl(p);
  }

  private static NodeAttributeProto convertToProtoFormat(NodeAttribute t) {
    return ((NodeAttributePBImpl)t).getProto();
  }

  private static ApplicationIdPBImpl convertFromProtoFormat(
      ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private static ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl)t).getProto();