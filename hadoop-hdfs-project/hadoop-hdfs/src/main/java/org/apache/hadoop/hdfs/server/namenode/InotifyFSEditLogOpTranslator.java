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

package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.util.Lists;

import java.util.List;

/**
 * 文件描述：HDFS名称节点编辑日志操作到Inotify事件的转换器
 * 核心职责：将编辑日志中的各种文件系统操作转换为可供外部订阅消费的Inotify事件，
 *          支持第三方系统监听HDFS文件系统变更，实现数据同步、缓存更新等业务场景
 */
@InterfaceAudience.Private
public class InotifyFSEditLogOpTranslator {

  /**
   * 计算AddCloseOp操作对应的文件总大小，累加所有数据块的字节数
   * @param acOp AddClose操作对象
   * @return 文件总大小，单位字节
   */
  private static long getSize(FSEditLogOp.AddCloseOp acOp) {
    long size = 0;
    for (Block b : acOp.getBlocks()) {
      size += b.getNumBytes();
    }
    return size;
  }

  /**
   * 将编辑日志操作转换为Inotify事件批次
   * @param op 编辑日志操作对象
   * @return 转换后的Inotify事件批次，不支持的操作类型返回null
   */
  public static EventBatch translate(FSEditLogOp op) {
    // 根据操作码分支处理不同类型的编辑日志操作
    switch(op.opCode) {
    case OP_ADD:
      // 强转为添加文件操作对象
      FSEditLogOp.AddOp addOp = (FSEditLogOp.AddOp) op;
      if (addOp.blocks.length == 0) { // 空块代表新建文件操作
        // 构造新建文件CreateEvent并返回对应事件批次
        return new EventBatch(op.txid,
            new Event[] { new Event.CreateEvent.Builder().path(addOp.path)
            .ctime(addOp.atime)
            .replication(addOp.replication)
            .ownerName(addOp.permissions.getUserName())
            .groupName(addOp.permissions.getGroupName())
            .perms(addOp.permissions.getPermission())
            .overwrite(addOp.overwrite)
            .defaultBlockSize(addOp.blockSize)
            .erasureCoded(addOp.erasureCodingPolicyId
                    != ErasureCodeConstants.REPLICATION_POLICY_ID)
            .iNodeType(Event.CreateEvent.INodeType.FILE).build() });
      } else { // 非空块代表追加文件操作
        // 构造追加AppendEvent并返回对应事件批次
        return new EventBatch(op.txid,
            new Event[]{new Event.AppendEvent.Builder()
                .path(addOp.path)
                .build()});
      }
    case OP_CLOSE:
      // 强转为关闭文件操作对象
      FSEditLogOp.CloseOp cOp = (FSEditLogOp.CloseOp) op;
      // 构造关闭CloseEvent并返回对应事件批次
      return new EventBatch(op.txid, new Event[] {
          new Event.CloseEvent(cOp.path, getSize(cOp), cOp.mtime) });
    case OP_APPEND:
      // 强转为追加操作对象
      FSEditLogOp.AppendOp appendOp = (FSEditLogOp.AppendOp) op;
      // 构造追加AppendEvent并返回对应事件批次
      return new EventBatch(op.txid, new Event[] {new Event.AppendEvent
          .Builder().path(appendOp.path).newBlock(appendOp.newBlock).build()});
    case OP_SET_REPLICATION:
      // 强转为设置副本数操作对象
      FSEditLogOp.SetReplicationOp setRepOp = (FSEditLogOp.SetReplicationOp) op;
      // 构造副本数元数据更新事件并返回对应事件批次
      return new EventBatch(op.txid,
        new Event[] { new Event.MetadataUpdateEvent.Builder()
          .metadataType(Event.MetadataUpdateEvent.MetadataType.REPLICATION)
          .path(setRepOp.path)
          .replication(setRepOp.replication).build() });
    case OP_CONCAT_DELETE:
      // 强转为拼接删除操作对象
      FSEditLogOp.ConcatDeleteOp cdOp = (FSEditLogOp.ConcatDeleteOp) op;
      // 初始化事件列表收集多个操作对应的事件
      List<Event> events = Lists.newArrayList();
      // 添加目标文件的追加事件
      events.add(new Event.AppendEvent.Builder()
          .path(cdOp.trg)
          .build());
      // 遍历所有源文件，每个源文件添加取消链接事件
      for (String src : cdOp.srcs) {
        events.add(new Event.UnlinkEvent.Builder()
          .path(src)
          .timestamp(cdOp.timestamp)
          .build());
      }
      // 添加目标文件的关闭事件
      events.add(new Event.CloseEvent(cdOp.trg, -1, cdOp.timestamp));
      // 返回包含多个事件的事件批次
      return new EventBatch(op.txid, events.toArray(new Event[0]));
    case OP_RENAME_OLD:
      // 强转为旧版重命名操作对象
      FSEditLogOp.RenameOldOp rnOpOld = (FSEditLogOp.RenameOldOp) op;
      // 构造重命名事件并返回对应事件批次
      return new EventBatch(op.txid, new Event[] {
          new Event.RenameEvent.Builder()
              .srcPath(rnOpOld.src)
              .dstPath(rnOpOld.dst)
              .timestamp(rnOpOld.timestamp)
              .build() });
    case OP_RENAME:
      // 强转为新版重命名操作对象
      FSEditLogOp.RenameOp rnOp = (FSEditLogOp.RenameOp) op;
      // 构造重命名事件并返回对应事件批次
      return new EventBatch(op.txid, new Event[] {
          new Event.RenameEvent.Builder()
            .srcPath(rnOp.src)
            .dstPath(rnOp.dst)
            .timestamp(rnOp.timestamp)
            .build() });
    case OP_DELETE:
      // 强转为删除操作对象
      FSEditLogOp.DeleteOp delOp = (FSEditLogOp.DeleteOp) op;
      // 构造取消链接事件并返回对应事件批次
      return new EventBatch(op.txid, new Event[] {
          new Event.UnlinkEvent.Builder()
            .path(delOp.path)
            .timestamp(delOp.timestamp)
            .build() });
    case OP_MKDIR:
      // 强转为创建目录操作对象
      FSEditLogOp.MkdirOp mkOp = (FSEditLogOp.MkdirOp) op;
      // 构造创建目录CreateEvent并返回对应事件批次
      return new EventBatch(op.txid,
        new Event[] { new Event.CreateEvent.Builder().path(mkOp.path)
          .ctime(mkOp.timestamp)
          .ownerName(mkOp.permissions.getUserName())
          .groupName(mkOp.permissions.getGroupName())
          .perms(mkOp.permissions.getPermission())
          .iNodeType(Event.CreateEvent.INodeType.DIRECTORY).build() });
    case OP_SET_PERMISSIONS:
      // 强转为设置权限操作对象
      FSEditLogOp.SetPermissionsOp permOp = (FSEditLogOp.SetPermissionsOp) op;
      // 构造权限元数据更新事件并返回对应事件批次
      return new EventBatch(op.txid,
        new Event[] { new Event.MetadataUpdateEvent.Builder()
          .metadataType(Event.MetadataUpdateEvent.MetadataType.PERMS)
          .path(permOp.src)
          .perms(permOp.permissions).build() });
    case OP_SET_OWNER:
      // 强转为设置所有者操作对象
      FSEditLogOp.SetOwnerOp ownOp = (FSEditLogOp.SetOwnerOp) op;
      // 构造所有者元数据更新事件并返回对应事件批次
      return new EventBatch(op.txid,
        new Event[] { new Event.MetadataUpdateEvent.Builder()
          .metadataType(Event.MetadataUpdateEvent.MetadataType.OWNER)
          .path(ownOp.src)
          .ownerName(ownOp.username).groupName(ownOp.groupname).build() });
    case OP_TIMES:
      // 强转为修改时间戳操作对象
      FSEditLogOp.TimesOp timesOp = (FSEditLogOp.TimesOp) op;
      // 构造时间戳元数据更新事件并返回对应事件批次
      return new EventBatch(op.txid,
        new Event[] { new Event.MetadataUpdateEvent.Builder()
          .metadataType(Event.MetadataUpdateEvent.MetadataType.TIMES)
          .path(timesOp.path)
          .atime(timesOp.atime).mtime(timesOp.mtime).build() });
    case OP_SYMLINK:
      // 强转为创建符号链接操作对象
      FSEditLogOp.SymlinkOp symOp = (FSEditLogOp.SymlinkOp) op;
      // 构造符号链接CreateEvent并返回对应事件批次
      return new EventBatch(op.txid,
        new Event[] { new Event.CreateEvent.Builder().path(symOp.path)
          .ctime(symOp.atime)
          .ownerName(symOp.permissionStatus.getUserName())
          .groupName(symOp.permissionStatus.getGroupName())
          .perms(symOp.permissionStatus.getPermission())
          .symlinkTarget(symOp.value)
          .iNodeType(Event.CreateEvent.INodeType.SYMLINK).build() });
    case OP_REMOVE_XATTR:
      // 强转为移除扩展属性操作对象
      FSEditLogOp.RemoveXAttrOp rxOp = (FSEditLogOp.RemoveXAttrOp) op;
      // 构造扩展属性元数据更新事件并返回对应事件批次
      return new EventBatch(op.txid,
        new Event[] { new Event.MetadataUpdateEvent.Builder()
          .metadataType(Event.MetadataUpdateEvent.MetadataType.XATTRS)
          .path(rxOp.src)
          .xAttrs(rxOp.xAttrs)
          .xAttrsRemoved(true).build() });
    case OP_SET_XATTR:
      // 强转为设置扩展属性操作对象
      FSEditLogOp.SetXAttrOp sxOp = (FSEditLogOp.SetXAttrOp) op;
      // 构造扩展属性元数据更新事件并返回对应事件批次
      return new EventBatch(op.txid,
        new Event[] { new Event.MetadataUpdateEvent.Builder()
          .metadataType(Event.MetadataUpdateEvent.MetadataType.XATTRS)
          .path(sxOp.src)
          .xAttrs(sxOp.xAttrs)
          .xAttrsRemoved(false).build() });
    case OP_SET_ACL:
      // 强转为设置ACL操作对象
      FSEditLogOp.SetAclOp saOp = (FSEditLogOp.SetAclOp) op;
      // 构造ACL元数据更新事件并返回对应事件批次
      return new EventBatch(op.txid,
        new Event[] { new Event.MetadataUpdateEvent.Builder()
          .metadataType(Event.MetadataUpdateEvent.MetadataType.ACLS)
          .path(saOp.src)
          .acls(saOp.aclEntries).build() });
    case OP_TRUNCATE:
      // 强转为截断文件操作对象
      FSEditLogOp.TruncateOp tOp = (FSEditLogOp.TruncateOp) op;
      // 构造截断事件并返回对应事件批次
      return new EventBatch(op.txid, new Event[] {
          new Event.TruncateEvent(tOp.src, tOp.newLength, tOp.timestamp) });
    default:
      // 不支持的操作类型返回null
      return null;
    }
  }
}