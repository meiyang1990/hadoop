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

package org.apache.hadoop.mapreduce;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @file org/apache/hadoop/mapreduce/JobSubmissionFiles.java
 * @brief MapReduce作业提交文件路径管理工具类，提供各类作业提交相关文件的路径生成、 staging目录初始化权限检查功能
 *
 * 该类属于MapReduce客户端核心模块，负责统一管理作业提交阶段所有临时文件的路径规范，
 * 并完成作业提交根目录(staging目录)的初始化、权限校验与纠正，保障作业提交的安全性。
 */
@InterfaceAudience.Private
public class JobSubmissionFiles {

  private final static Logger LOG =
      LoggerFactory.getLogger(JobSubmissionFiles.class);

  // 作业提交目录权限：仅所有者可读写执行，禁止其他用户访问
  final public static FsPermission JOB_DIR_PERMISSION =
      FsPermission.createImmutable((short) 0700); // rwx------
  // 作业提交文件权限：所有者可读写，其他用户只读，保障共享场景下的文件访问安全
  final public static FsPermission JOB_FILE_PERMISSION = 
      FsPermission.createImmutable((short) 0644); // rw-r--r--
  
  /**
   * 获取作业输入分片文件路径
   * @param jobSubmissionDir 作业提交根目录
   * @return 作业分片文件完整路径
   */
  public static Path getJobSplitFile(Path jobSubmissionDir) {
    return new Path(jobSubmissionDir, "job.split");
  }

  /**
   * 获取作业输入分片元信息文件路径
   * @param jobSubmissionDir 作业提交根目录
   * @return 作业分片元信息文件完整路径
   */
  public static Path getJobSplitMetaFile(Path jobSubmissionDir) {
    return new Path(jobSubmissionDir, "job.splitmetainfo");
  }
  
  /**
   * 获取作业配置文件路径
   * @param jobSubmitDir 作业提交根目录
   * @return 作业配置文件完整路径
   */
  public static Path getJobConfPath(Path jobSubmitDir) {
    return new Path(jobSubmitDir, "job.xml");
  }
    
  /**
   * 获取作业Jar包路径
   * @param jobSubmitDir 作业提交根目录
   * @return 作业Jar包完整路径
   */
  public static Path getJobJar(Path jobSubmitDir) {
    return new Path(jobSubmitDir, "job.jar");
  }
  
  /**
   * 获取分布式缓存普通文件目录路径
   * @param jobSubmitDir 作业提交根目录
   * @return 分布式缓存普通文件目录完整路径
   */
  public static Path getJobDistCacheFiles(Path jobSubmitDir) {
    return new Path(jobSubmitDir, "files");
  }
  
  /**
   * 获取作业自定义log4j配置文件路径
   * @param jobSubmitDir 作业提交根目录
   * @return log4j配置文件完整路径
   */
  public static Path getJobLog4jFile(Path jobSubmitDir) {
    return new Path(jobSubmitDir, "log4j");
  }

  /**
   * 获取分布式缓存归档文件目录路径
   * @param jobSubmitDir 作业提交根目录
   * @return 分布式缓存归档文件目录完整路径
   */
  public static Path getJobDistCacheArchives(Path jobSubmitDir) {
    return new Path(jobSubmitDir, "archives");
  }

  /**
   * 获取分布式缓存第三方依赖Jar包目录路径
   * @param jobSubmitDir 作业提交根目录
   * @return 分布式缓存依赖Jar包目录完整路径
   */
  public static Path getJobDistCacheLibjars(Path jobSubmitDir) {
    return new Path(jobSubmitDir, "libjars");
  }

  /**
   * 初始化作业提交根目录(staging目录)，校验目录所有权与权限配置
   * @param cluster MapReduce集群信息对象
   * @param conf 作业配置对象
   * @return 初始化完成的staging目录路径
   * @throws IOException IO异常、权限校验失败时抛出
   * @throws InterruptedException 中断异常
   */
  public static Path getStagingDir(Cluster cluster, Configuration conf)
      throws IOException, InterruptedException {
    UserGroupInformation user = UserGroupInformation.getLoginUser();
    return getStagingDir(cluster, conf, user);
  }

  /**
   * 初始化作业提交根目录(staging目录)，校验目录所有权与权限配置，公开用于单元测试
   *
   * @param cluster  MapReduce集群信息对象
   * @param conf     作业配置对象
   * @param realUser 提交作业的登录用户信息
   * @return 初始化完成的staging目录路径对象
   * @throws IOException          当staging目录所有者不匹配当前提交用户时抛出
   * @throws InterruptedException 获取staging目录路径时中断抛出
   */
  @VisibleForTesting
  public static Path getStagingDir(Cluster cluster, Configuration conf,
      UserGroupInformation realUser) throws IOException, InterruptedException {
    // 从集群获取staging根目录路径
    Path stagingArea = cluster.getStagingAreaDir();
    // 获取对应文件系统实例
    FileSystem fs = stagingArea.getFileSystem(conf);
    // 获取当前实际操作用户
    UserGroupInformation currentUser = realUser.getCurrentUser();
    try {
      // 获取staging目录状态信息
      FileStatus fsStatus = fs.getFileStatus(stagingArea);
      String fileOwner = fsStatus.getOwner();
      // 校验目录所有者是否匹配提交用户（兼容短用户名和全用户名多种情况）
      if (!(fileOwner.equals(currentUser.getShortUserName()) || fileOwner
          .equalsIgnoreCase(currentUser.getUserName()) || fileOwner
          .equals(realUser.getShortUserName()) || fileOwner
          .equalsIgnoreCase(realUser.getUserName()))) {
        String errorMessage = "The ownership on the staging directory " +
            stagingArea + " is not as expected. " +
            "It is owned by " + fileOwner + ". The directory must " +
            "be owned by the submitter " + currentUser.getShortUserName()
            + " or " + currentUser.getUserName();
        // 代理提交场景下额外添加原用户校验提示
        if (!realUser.getUserName().equals(currentUser.getUserName())) {
          throw new IOException(
              errorMessage + " or " + realUser.getShortUserName() + " or "
                  + realUser.getUserName());
        } else {
          throw new IOException(errorMessage);
        }
      }
      // 校验目录权限，如果不对自动纠正
      if (!fsStatus.getPermission().equals(JOB_DIR_PERMISSION)) {
        LOG.info("Permissions on staging directory " + stagingArea + " are " +
            "incorrect: " + fsStatus.getPermission() + ". Fixing permissions " +
            "to correct value " + JOB_DIR_PERMISSION);
        fs.setPermission(stagingArea, JOB_DIR_PERMISSION);
      }
    } catch (FileNotFoundException e) {
      // staging目录不存在，创建目录并设置正确权限
      FileSystem.mkdirs(fs, stagingArea, new FsPermission(JOB_DIR_PERMISSION));
    }
    return stagingArea;
  }
}