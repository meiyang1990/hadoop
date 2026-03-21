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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.nodelabels.NodeAttributeStore;
import org.apache.hadoop.yarn.nodelabels.NodeAttributesManager;
import org.apache.hadoop.yarn.nodelabels.store.AbstractFSNodeStore;
import org.apache.hadoop.yarn.nodelabels.store.FSStoreOpHandler;
import org.apache.hadoop.yarn.nodelabels.store.op.AddNodeToAttributeLogOp;
import org.apache.hadoop.yarn.nodelabels.store.op.RemoveNodeToAttributeLogOp;
import org.apache.hadoop.yarn.nodelabels.store.op.ReplaceNodeToAttributeLogOp;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeToAttributes;

import java.io.IOException;
import java.util.List;

/**
 * 基于文件系统实现的节点属性存储，将节点属性变更持久化到文件系统
 */
public class FileSystemNodeAttributeStore
    extends AbstractFSNodeStore<NodeAttributesManager>
    implements NodeAttributeStore {

  protected static final Logger LOG =
      LoggerFactory.getLogger(FileSystemNodeAttributeStore.class);

  // 默认存储目录名称
  protected static final String DEFAULT_DIR_NAME = "node-attribute";
  // 镜像文件名称（全量快照）
  protected static final String MIRROR_FILENAME = "nodeattribute.mirror";
  // 编辑日志文件名称（增量变更记录）
  protected static final String EDITLOG_FILENAME = "nodeattribute.editlog";

  /**
   * 构造函数，指定存储类型为节点属性
   */
  public FileSystemNodeAttributeStore() {
    super(FSStoreOpHandler.StoreType.NODE_ATTRIBUTE);
  }

  /**
   * 获取默认的文件系统节点属性存储根目录
   * @return 默认根目录路径字符串
   * @throws IOException 获取当前用户信息失败时抛出
   */
  private String getDefaultFSNodeAttributeRootDir() throws IOException {
    // default is in local: /tmp/hadoop-yarn-${user}/node-attribute/
    return "file:///tmp/hadoop-yarn-" + UserGroupInformation.getCurrentUser()
        .getShortUserName() + "/" + DEFAULT_DIR_NAME;
  }

  @Override
  public void init(Configuration conf, NodeAttributesManager mgr)
      throws Exception {
    // 构建存储schema，指定编辑日志和镜像文件名称
    StoreSchema schema = new StoreSchema(EDITLOG_FILENAME, MIRROR_FILENAME);
    // 从配置获取根目录，使用默认值兜底，初始化存储
    initStore(conf, new Path(
        conf.get(YarnConfiguration.FS_NODE_ATTRIBUTE_STORE_ROOT_DIR,
            getDefaultFSNodeAttributeRootDir())), schema, mgr);
  }

  @Override
  public void replaceNodeAttributes(List<NodeToAttributes> nodeToAttribute)
      throws IOException {
    // 创建替换节点属性操作
    ReplaceNodeToAttributeLogOp op = new ReplaceNodeToAttributeLogOp();
    // 将变更写入日志
    writeToLog(op.setAttributes(nodeToAttribute));
  }

  @Override
  public void addNodeAttributes(List<NodeToAttributes> nodeAttributeMapping)
      throws IOException {
    // 创建添加节点属性操作
    AddNodeToAttributeLogOp op = new AddNodeToAttributeLogOp();
    // 将变更写入日志
    writeToLog(op.setAttributes(nodeAttributeMapping));
  }

  @Override
  public void removeNodeAttributes(List<NodeToAttributes> nodeAttributeMapping)
      throws IOException {
    // 创建移除节点属性操作
    RemoveNodeToAttributeLogOp op = new RemoveNodeToAttributeLogOp();
    // 将变更写入日志
    writeToLog(op.setAttributes(nodeAttributeMapping));
  }

  @Override
  public void recover() throws IOException, YarnException {
    // 从文件系统存储恢复节点属性数据
    super.recoverFromStore();
  }

  @Override
  public void close() throws IOException {
    // 关闭文件系统存储资源
    super.closeFSStore();
  }
}