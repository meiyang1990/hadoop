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

package org.apache.hadoop.yarn.server.resourcemanager.nodelabels;

import org.apache.hadoop.classification.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeKey;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.nodelabels.AttributeValue;
import org.apache.hadoop.yarn.nodelabels.NodeAttributeStore;
import org.apache.hadoop.yarn.nodelabels.NodeAttributesManager;
import org.apache.hadoop.yarn.nodelabels.NodeLabelUtil;
import org.apache.hadoop.yarn.nodelabels.RMNodeAttribute;
import org.apache.hadoop.yarn.nodelabels.StringAttributeValue;
import org.apache.hadoop.yarn.server.api.protocolrecords.AttributeMappingOperationType;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeToAttributes;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAttributesUpdateSchedulerEvent;


/**
 * YARN RM 节点属性管理器实现类，负责管理集群节点属性的存储、更新与调度通知.
 * 支持集中式节点属性的增删改操作，并持久化变更到存储，同时通知调度器属性变更事件.
 */
public class NodeAttributesManagerImpl extends NodeAttributesManager {
  protected static final Logger LOG =
      LoggerFactory.getLogger(NodeAttributesManagerImpl.class);
  /**
   * 如果用户未指定属性值，默认使用空字符串作为属性值.
   */
  public static final String EMPTY_ATTRIBUTE_VALUE = "";

  // 事件分发器，处理节点属性存储变更事件
  Dispatcher dispatcher;
  // 节点属性存储实现，负责持久化存储节点属性映射
  NodeAttributeStore store;

  // TODO 全局集群属性映射：属性键 -> RM端属性信息
  private ConcurrentHashMap<NodeAttributeKey, RMNodeAttribute> clusterAttributes
      = new ConcurrentHashMap<>();

  // 按主机聚合节点属性：主机名 -> (属性 -> 属性值映射
  private ConcurrentMap<String, Host> nodeCollections =
      new ConcurrentHashMap<>();

  private final ReadLock readLock;
  private final WriteLock writeLock;
  // RM上下文引用，用于通知调度器属性变更
  private RMContext rmContext = null;

  public NodeAttributesManagerImpl() {
    super("NodeAttributesManagerImpl");
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  /**
   * 初始化事件分发器.
   * @param conf 配置信息
   */
  protected void initDispatcher(Configuration conf) {
    // 创建异步事件处理器
    dispatcher = new AsyncDispatcher("AttributeNodeLabelsManager dispatcher");
    AsyncDispatcher asyncDispatcher = (AsyncDispatcher) dispatcher;
    asyncDispatcher.init(conf);
    asyncDispatcher.setDrainEventsOnStop();
  }

  /**
   * 启动事件分发器.
   */
  protected void startDispatcher() {
    // 启动分发器
    AsyncDispatcher asyncDispatcher = (AsyncDispatcher) dispatcher;
    asyncDispatcher.start();
  }

  @Override
  protected void serviceStart() throws Exception {
    initNodeAttributeStore(getConfig());
    // 仅在服务启动时初始化分发器，因为恢复过程在服务初始化阶段完成，恢复时不需要触发事件处理
    initDispatcher(getConfig());

    if (null != dispatcher) {
      dispatcher.register(NodeAttributesStoreEventType.class,
          new ForwardingEventHandler());
    }

    startDispatcher();
    super.serviceStart();
  }

  protected void initNodeAttributeStore(Configuration conf) throws Exception {
    this.store = getAttributeStoreClass(conf);
    this.store.init(conf, this);
    // 从存储中恢复已有节点属性
    this.store.recover();
  }

  private NodeAttributeStore getAttributeStoreClass(Configuration conf) {
    try {
      // 根据配置反射实例化节点属性存储实现类
      return ReflectionUtils.newInstance(
          conf.getClass(YarnConfiguration.FS_NODE_ATTRIBUTE_STORE_IMPL_CLASS,
              FileSystemNodeAttributeStore.class, NodeAttributeStore.class),
          conf);
    } catch (Exception e) {
      throw new YarnRuntimeException(
          "Could not instantiate Node Attribute Store ", e);
    }
  }

  @VisibleForTesting
  protected void internalUpdateAttributesOnNodes(
      Map<String, Map<NodeAttribute, AttributeValue>> nodeAttributeMapping,
      AttributeMappingOperationType op,
      Map<NodeAttributeKey, RMNodeAttribute> newAttributesToBeAdded,
      String attributePrefix) {
    writeLock.lock();
    try {
      // 构建操作日志信息
      StringBuilder logMsg = new StringBuilder(op.name());
      logMsg.append(" attributes on nodes:");
      // 遍历每个节点更新属性映射
      for (Entry<String, Map<NodeAttribute, AttributeValue>> entry :
          nodeAttributeMapping.entrySet()) {
        String nodeHost = entry.getKey();
        Map<NodeAttribute, AttributeValue> attributes = entry.getValue();

        Host node = nodeCollections.get(nodeHost);
        if (node == null) {
          node = new Host(nodeHost);
          nodeCollections.put(nodeHost, node);
        }
        // 根据操作类型执行对应更新
        switch (op) {
        case REMOVE:
          removeNodeFromAttributes(nodeHost, attributes.keySet());
          node.removeAttributes(attributes);
          break;
        case ADD:
          clusterAttributes.putAll(newAttributesToBeAdded);
          addNodeToAttribute(nodeHost, attributes);
          node.addAttributes(attributes);
          break;
        case REPLACE:
          clusterAttributes.putAll(newAttributesToBeAdded);
          replaceNodeToAttribute(nodeHost, attributePrefix,
              node.getAttributes(), attributes);
          node.replaceAttributes(attributes, attributePrefix);
          break;
        default:
          break;
        }
        // 记录当前节点操作信息到日志
        logMsg.append(" NM = ")
            .append(entry.getKey())
            .append(", attributes=[ ")
            .append(StringUtils.join(entry.getValue().keySet(), ","))
            .append("] ,");
      }
      LOG.debug("{}", logMsg);

      // 如果是集中式属性，分发存储变更事件
      if (null != dispatcher && NodeAttribute.PREFIX_CENTRALIZED
          .equals(attributePrefix)) {
        dispatcher.getEventHandler()
            .handle(new NodeAttributesStoreEvent(nodeAttributeMapping, op));
      }

      // 构建通知RM调度器的节点属性映射
      Map<String, Set<NodeAttribute>> newNodeToAttributesMap =
          new HashMap<String, Set<NodeAttribute>>();
      nodeAttributeMapping.forEach((k, v) -> {
        Host node = nodeCollections.get(k);
        newNodeToAttributesMap.put(k, node.attributes.keySet());
      });

      // 通知RM调度器节点属性已更新
      if (rmContext != null && rmContext.getDispatcher() != null) {
        LOG.info("Updated NodeAttribute event to RM:"
            + newNodeToAttributesMap);
        rmContext.getDispatcher().getEventHandler().handle(
            new NodeAttributesUpdateSchedulerEvent(newNodeToAttributesMap));
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void removeNodeFromAttributes(String nodeHost,
      Set<NodeAttribute> attributeMappings) {
    // 从每个属性关联节点集合中移除当前节点
    for (NodeAttribute rmAttribute : attributeMappings) {
      RMNodeAttribute host =
          clusterAttributes.get(rmAttribute.getAttributeKey());
      if (host != null) {
        host.removeNode(nodeHost);
        // 如果没有任何节点关联此属性，则从全局映射中删除该属性
        if (host.getAssociatedNodeIds().isEmpty()) {
          clusterAttributes.remove(rmAttribute.getAttributeKey());
        }
      }
    }
  }

  private void addNodeToAttribute(String nodeHost,
      Map<NodeAttribute, AttributeValue> attributeMappings) {
    // 将当前节点添加到每个属性的关联节点集合中
    for (Entry<NodeAttribute, AttributeValue> attributeEntry : attributeMappings
        .entrySet()) {

      RMNodeAttribute rmNodeAttribute =
          clusterAttributes.get(attributeEntry.getKey().getAttributeKey());
      if (rmNodeAttribute != null) {
        rmNodeAttribute.addNode(nodeHost, attributeEntry.getValue());
      } else {
        clusterAttributes.put(attributeEntry.getKey().getAttributeKey(),
            new RMNodeAttribute(attributeEntry.getKey()));
      }
    }
  }

  private void replaceNodeToAttribute(String nodeHost, String prefix,
      Map<NodeAttribute, AttributeValue> oldAttributeMappings,
      Map<NodeAttribute, AttributeValue> newAttributeMappings) {
    // 先移除指定前缀的旧属性，再添加新属性
    if (oldAttributeMappings != null) {
      Set<NodeAttribute> toRemoveAttributes =
          NodeLabelUtil.filterAttributesByPrefix(
              oldAttributeMappings.keySet(), prefix);
      removeNodeFromAttributes(nodeHost, toRemoveAttributes);
    }
    addNodeToAttribute(nodeHost, newAttributeMappings);
  }

  /**
   * 验证节点属性映射的合法性，预处理属性名称、前缀、类型和值，并转换为标准格式.
   * @param nodeAttributeMapping 输入节点到属性的映射
   * @param newAttributesToBeAdded 待新增到全局的新属性集合
   * @param isRemoveOperation 是否为删除操作
   * @return 验证后转换完成的合法节点属性映射
   * @throws IOException 验证失败时抛出异常
   */
  protected Map<String, Map<NodeAttribute, AttributeValue>> validate(
      Map<String, Set<NodeAttribute>> nodeAttributeMapping,
      Map<NodeAttributeKey, RMNodeAttribute> newAttributesToBeAdded,
      boolean isRemoveOperation) throws IOException {
    Map<String, Map<NodeAttribute, AttributeValue>> nodeToAttributesMap =
        new TreeMap<>();
    Map<NodeAttribute, AttributeValue> attributesValues;
    Set<Entry<String, Set<NodeAttribute>>> entrySet =
        nodeAttributeMapping.entrySet();
    // 遍历每个节点的属性映射
    for (Entry<String, Set<NodeAttribute>> nodeToAttrMappingEntry : entrySet) {
      attributesValues = new HashMap<>();
      String node = nodeToAttrMappingEntry.getKey().trim();
      if (nodeToAttrMappingEntry.getValue().isEmpty()) {
        // 删除操作常遇到空属性集合，直接跳过
        continue;
      }

      // 逐个验证属性合法性
      for (NodeAttribute attribute : nodeToAttrMappingEntry.getValue()) {
        NodeAttributeKey attributeKey = attribute.getAttributeKey();
        String attributeName = attributeKey.getAttributeName().trim();
        // 验证属性名称格式
        NodeLabelUtil.checkAndThrowAttributeName(attributeName);
        // 验证属性前缀格式
        NodeLabelUtil
            .checkAndThrowAttributePrefix(attributeKey.getAttributePrefix());
        // 验证属性值格式
        NodeLabelUtil
            .checkAndThrowAttributeValue(attribute.getAttributeValue());

        // 对属性名称和前缀去除首尾空格
        attributeKey.setAttributeName(attributeName);
        attributeKey
            .setAttributePrefix(attributeKey.getAttributePrefix().trim());

        // 验证属性类型是否匹配已有属性，不匹配则添加到待新增集合
        if (validateForAttributeTypeMismatch(isRemoveOperation, attribute,
            newAttributesToBeAdded)) {
          newAttributesToBeAdded.put(attribute.getAttributeKey(),
              new RMNodeAttribute(attribute));
        }
        // TODO 需要通过工厂根据类型创建对应值对象
        StringAttributeValue value = new StringAttributeValue();
        value.validateAndInitializeValue(
            normalizeAttributeValue(attribute.getAttributeValue()));
        attributesValues.put(attribute, value);
      }
      nodeToAttributesMap.put(node, attributesValues);
    }
    return nodeToAttributesMap;
  }

  /**
   * 验证新增/删除操作中属性类型是否与已有类型匹配.
   * @param isRemoveOperation 是否为删除操作
   * @param attribute 待验证的属性
   * @param newAttributes 当前操作中的新属性集合
   * @return 是否需要将该属性新增到全局集合
   * @throws IOException 类型不匹配时抛出异常
   */
  private boolean validateForAttributeTypeMismatch(boolean isRemoveOperation,
      NodeAttribute attribute,
      Map<NodeAttributeKey, RMNodeAttribute> newAttributes)
      throws IOException {
    NodeAttributeKey attributeKey = attribute.getAttributeKey();
    if (isRemoveOperation
        && !clusterAttributes.containsKey(attributeKey)) {
      // 删除操作且属性不存在，无需验证，不需要新增
      return false;
    } else {
      // 获取已存在的属性定义（全局集合或当前操作新增集合）
      NodeAttribute existingAttribute =
          (clusterAttributes.containsKey(attributeKey)
              ? clusterAttributes.get(attributeKey).getAttribute()
              : (newAttributes.containsKey(attributeKey)
                  ? newAttributes.get(attributeKey).getAttribute()
                  : null));
      if (existingAttribute == null) {
        // 不存在，需要新增
        return true;
      } else if (existingAttribute.getAttributeType() != attribute
          .getAttributeType()) {
        // 类型不匹配，抛出异常
        throw new IOException("Attribute name - type is not matching with "
            + "already configured mapping for the attribute "
            + attributeKey + " existing : "
            + existingAttribute.getAttributeType() + ", new :"
            + attribute.getAttributeType());
      }
      // 类型匹配，不需要新增
      return false;
    }
  }

  protected String normalizeAttributeValue(String value) {
    // 去除属性值首尾空格，null则返回默认空字符串
    if (value != null) {
      return value.trim();
    }
    return EMPTY_ATTRIBUTE_VALUE;
  }

  @Override
  public Set<NodeAttribute> getClusterNodeAttributes(
      Set<String> prefix) {
    Set<NodeAttribute> attributes = new HashSet<>();
    Set<Entry<NodeAttributeKey, RMNodeAttribute>> allAttributes =
        clusterAttributes.entrySet();
    // 如果未指定前缀，返回所有属性
    boolean forAllPrefix = prefix == null || prefix.isEmpty();
    // 根据前缀筛选集群节点属性
    Iterator<Entry<NodeAttributeKey, RMNodeAttribute>> iterator =
        allAttributes.iterator();
    while (iterator.hasNext()) {
      Entry<NodeAttributeKey, RMNodeAttribute> current = iterator.next();
      NodeAttributeKey attrID = current.getKey();
      RMNodeAttribute rmAttr = current.getValue();
      if (forAllPrefix || prefix.contains(attrID.getAttributePrefix())) {
        attributes.add(rmAttr.getAttribute());
      }
    }
    return attributes;
  }

  @Override
  public Map<NodeAttributeKey,
      Map<String, AttributeValue>> getAttributesToNodes(
      Set<NodeAttributeKey> attributes) {
    readLock.lock();
    try {
      // 如果未指定属性，获取所有属性对应节点集合
      boolean fetchAllAttributes = (attributes == null || attributes.isEmpty());
      Map<NodeAttributeKey, Map<String, AttributeValue>> attributesToNodes =
          new HashMap<>();
      // 遍历全局属性，筛选指定属性，返回对应关联节点和值
      for (Entry<NodeAttributeKey, RMNodeAttribute> attributeEntry :
          clusterAttributes.entrySet()) {
        if (fetchAllAttributes
            || attributes.contains(attributeEntry.getKey())) {
          attributesToNodes.put(attributeEntry.getKey(),
              attributeEntry.getValue().getAssociatedNodeIds());
        }
      }
      return attributesToNodes;
    } finally {
      readLock.unlock();
    }
  }

  public Resource getResourceByAttribute(NodeAttribute attribute) {
    readLock.lock();
    try {
      // 根据属性获取聚合后的总资源
      return clusterAttributes.containsKey(attribute.getAttributeKey())
          ? clusterAttributes.get(attribute.getAttributeKey()).getResource()
          : Resource.newInstance(0, 0);