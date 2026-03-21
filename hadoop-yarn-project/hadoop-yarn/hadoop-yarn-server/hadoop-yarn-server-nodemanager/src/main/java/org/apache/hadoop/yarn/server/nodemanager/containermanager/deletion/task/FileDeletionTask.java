// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;

import java.io.IOException;
import java.util.List;

/**
 * 文件删除任务，负责处理容器退出后本地文件/目录的清理工作，继承自DeletionTask基类。
 */
public class FileDeletionTask extends DeletionTask implements Runnable {

  private final Path subDir;
  private final List<Path> baseDirs;
  private static final FileContext lfs = getLfs();

  /**
   * 静态初始化获取本地文件系统上下文实例。
   * @return 本地文件系统上下文
   */
  private static FileContext getLfs() {
    try {
      return FileContext.getLocalFSFileContext();
    } catch (UnsupportedFileSystemException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * 构造文件删除任务，使用默认无效任务ID。
   *
   * @param deletionService     删除服务实例
   * @param user                执行删除操作对应用户
   * @param subDir              需要删除的子目录
   * @param baseDirs            包含子目录的基目录列表
   */
  public FileDeletionTask(DeletionService deletionService, String user,
      Path subDir, List<Path> baseDirs) {
    this(INVALID_TASK_ID, deletionService, user, subDir, baseDirs);
  }

  /**
   * 构造文件删除任务，指定任务ID用于恢复场景。
   *
   * @param taskId              任务ID，NM重启恢复时使用
   * @param deletionService     删除服务实例
   * @param user                执行删除操作对应用户
   * @param subDir              需要删除的子目录
   * @param baseDirs            包含子目录的基目录列表
   */
  public FileDeletionTask(int taskId, DeletionService deletionService,
      String user, Path subDir, List<Path> baseDirs) {
    super(taskId, deletionService, user, DeletionTaskType.FILE);
    this.subDir = subDir;
    this.baseDirs = baseDirs;
  }

  /**
   * 获取需要删除的子目录。
   *
   * @return 待删除子目录
   */
  public Path getSubDir() {
    return this.subDir;
  }

  /**
   * 获取包含待删除子目录的基目录列表。
   *
   * @return 基目录列表
   */
  public List<Path> getBaseDirs() {
    return this.baseDirs;
  }

  /**
   * 执行文件删除任务，区分NM本身删除和容器对应用户权限删除。
   */
  @Override
  public void run() {
    LOG.debug("Running DeletionTask : {}", this);
    boolean error = false;
    // 无指定用户，NM进程直接删除
    if (null == getUser()) {
      // 无基目录，直接删除绝对路径
      if (baseDirs == null || baseDirs.size() == 0) {
        LOG.debug("NM deleting absolute path : {}", subDir);
        try {
          lfs.delete(subDir, true);
        } catch (IOException e) {
          error = true;
          LOG.warn("Failed to delete " + subDir);
        }
      } else {
        // 遍历每个基目录，拼接完整路径后删除
        for (Path baseDir : baseDirs) {
          Path del = subDir == null? baseDir : new Path(baseDir, subDir);
          LOG.debug("NM deleting path : {}", del);
          try {
            lfs.delete(del, true);
          } catch (IOException e) {
            error = true;
            LOG.warn("Failed to delete " + subDir);
          }
        }
      }
    } else {
      // 有指定用户，委托容器执行器以对应用户身份删除
      try {
        LOG.debug("Deleting path: [{}] as user [{}]", subDir, getUser());
        if (baseDirs == null || baseDirs.size() == 0) {
          getDeletionService().getContainerExecutor().deleteAsUser(
              new DeletionAsUserContext.Builder()
              .setUser(getUser())
              .setSubDir(subDir)
              .build());
        } else {
          getDeletionService().getContainerExecutor().deleteAsUser(
              new DeletionAsUserContext.Builder()
              .setUser(getUser())
              .setSubDir(subDir)
              .setBasedirs(baseDirs.toArray(new Path[0]))
              .build());
        }
      } catch (IOException|InterruptedException e) {
        error = true;
        LOG.warn("Failed to delete as user " + getUser(), e);
      }
    }
    if (error) {
      setSuccess(!error);
    }
    // 通知删除服务任务完成
    deletionTaskFinished();
  }

  /**
   * 转换为字符串方便日志调试。
   *
   * @return 当前任务的字符串描述
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("FileDeletionTask :");
    sb.append("  id : ").append(getTaskId());
    sb.append("  user : ").append(getUser());
    sb.append("  subDir : ").append(
        subDir == null ? "null" : subDir.toString());
    sb.append("  baseDir : ");
    if (baseDirs == null || baseDirs.size() == 0) {
      sb.append("null");
    } else {
      for (Path baseDir : baseDirs) {
        sb.append(baseDir.toString()).append(',');
      }
    }
    return sb.toString().trim();
  }

  /**
   * 将当前删除任务转换为Protobuf格式，用于NM状态存储和重启恢复。
   *
   * @return 任务的Protobuf表示
   */
  public DeletionServiceDeleteTaskProto convertDeletionTaskToProto() {
    DeletionServiceDeleteTaskProto.Builder builder =
        getBaseDeletionTaskProtoBuilder();
    builder.setTaskType(DeletionTaskType.FILE.name());
    if (getSubDir() != null) {
      builder.setSubdir(getSubDir().toString());
    }
    if (getBaseDirs() != null) {
      for (Path dir : getBaseDirs()) {
        builder.addBasedirs(dir.toString());
      }
    }
    return builder.build();
  }
}