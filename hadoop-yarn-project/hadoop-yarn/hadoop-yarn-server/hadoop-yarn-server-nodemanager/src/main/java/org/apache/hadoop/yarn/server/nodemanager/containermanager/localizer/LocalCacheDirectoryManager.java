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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 本地缓存目录管理器，用于管理分层目录结构的本地化缓存
 * 限制每个目录存放的文件数量不超过配置值，每个目录最多包含36个子目录（命名为0-9和a-z）
 * 根目录用空字符串表示，内部维护未满目录队列，目录满后不再新增文件直到有文件被删除
 * 仅在请求路径且无可用未满目录时才会创建新子目录
 * 
 * 注意：此类仅返回相对本地化路径，不会实际在磁盘上创建目录
 */
public class LocalCacheDirectoryManager {

  private final int perDirectoryFileLimit;
  // 每一层最多36个子目录 = 26个字母 + 10个数字
  public static final int DIRECTORIES_PER_LEVEL = 36;

  private Queue<Directory> nonFullDirectories;
  private HashMap<String, Directory> knownDirectories;
  private int totalSubDirectories;

  /**
   * 构造函数，根据配置初始化缓存目录管理器
   * @param conf YARN配置对象
   */
  public LocalCacheDirectoryManager(Configuration conf) {
    totalSubDirectories = 0;
    Directory rootDir = new Directory(totalSubDirectories);
    nonFullDirectories = new LinkedList<Directory>();
    knownDirectories = new HashMap<String, Directory>();
    knownDirectories.put("", rootDir);
    nonFullDirectories.add(rootDir);
    this.perDirectoryFileLimit =
        conf.getInt(YarnConfiguration.NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY,
          YarnConfiguration.DEFAULT_NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY) - 36;
  }

  /**
   * 获取用于资源本地化的相对路径，从第一个可用的未满目录中分配
   * 
   * @return 用于本地化的相对路径字符串
   */
  public synchronized String getRelativePathForLocalization() {
    // 如果没有可用未满目录，创建新目录
    if (nonFullDirectories.isEmpty()) {
      totalSubDirectories++;
      Directory newDir = new Directory(totalSubDirectories);
      nonFullDirectories.add(newDir);
      knownDirectories.put(newDir.getRelativePath(), newDir);
    }
    // 获取队首未满目录
    Directory subDir = nonFullDirectories.peek();
    // 增加计数后如果达到限制，将该目录从队列移除
    if (subDir.incrementAndGetCount() >= perDirectoryFileLimit) {
      nonFullDirectories.remove();
    }
    return subDir.getRelativePath();
  }

  /**
   * 减少指定路径目录的文件计数，目录满后如果有空余会重新加入可用队列
   * @param relPath 目标目录相对路径，根目录为空字符串
   */
  public synchronized void decrementFileCountForPath(String relPath) {
    relPath = relPath == null ? "" : relPath.trim();
    Directory subDir = knownDirectories.get(relPath);
    int oldCount = subDir.getCount();
    // 减少计数后低于限制且之前已经满了，将目录重新加入未满队列
    if (subDir.decrementAndGetCount() < perDirectoryFileLimit
        && oldCount >= perDirectoryFileLimit) {
      nonFullDirectories.add(subDir);
    }
  }

  /**
   * 增加指定相对目录的文件计数，目录达到限制后从可用队列移除
   * @param relPath 目标目录相对路径
   */
  public synchronized void incrementFileCountForPath(String relPath) {
    relPath = relPath == null ? "" : relPath.trim();
    Directory subDir = knownDirectories.get(relPath);
    // 如果目录不存在，初始化新增目录
    if (subDir == null) {
      int dirnum = Directory.getDirectoryNumber(relPath);
      totalSubDirectories = Math.max(dirnum, totalSubDirectories);
      subDir = new Directory(dirnum);
      nonFullDirectories.add(subDir);
      knownDirectories.put(subDir.getRelativePath(), subDir);
    }
    // 增加计数后如果达到限制，从队列移除该目录
    if (subDir.incrementAndGetCount() >= perDirectoryFileLimit) {
      nonFullDirectories.remove(subDir);
    }
  }

  /**
   * 从缓存树中的任意目录向上找到缓存根目录
   * @param path 缓存树中的目录路径
   * @return 本地缓存根目录，找不到返回null
   */
  public static Path getCacheDirectoryRoot(Path path) {
    // 向上遍历直到找到非36进制子目录节点
    while (path != null) {
      String name = path.getName();
      // 名称长度不为1，说明不是分层子目录，返回当前作为根
      if (name.length() != 1) {
        return path;
      }
      int dirnum = DIRECTORIES_PER_LEVEL;
      try {
        // 尝试按36进制解析目录名
        dirnum = Integer.parseInt(name, DIRECTORIES_PER_LEVEL);
      } catch (NumberFormatException e) {
      }
      // 解析结果超出范围，说明不是分层子目录，返回当前作为根
      if (dirnum >= DIRECTORIES_PER_LEVEL) {
        return path;
      }
      // 继续向上遍历父目录
      path = path.getParent();
    }
    return path;
  }

  @VisibleForTesting
  synchronized Directory getDirectory(String relPath) {
    return knownDirectories.get(relPath);
  }

  /**
   * 目录信息类，记录目录相对路径和当前文件计数，限制目录内文件数量
   */
  static class Directory {

    private final String relativePath;
    private int fileCount;

    /**
     * 根据目录编号生成相对路径，使用36进制编码分层目录结构
     * @param directoryNo 目录全局编号
     * @return 分层结构的相对路径字符串
     */
    static String getRelativePath(int directoryNo) {
      String relativePath = "";
      if (directoryNo > 0) {
        // 将目录编号减1转为36进制字符串
        String tPath = Integer.toString(directoryNo - 1, DIRECTORIES_PER_LEVEL);
        StringBuilder sb = new StringBuilder();
        if (tPath.length() == 1) {
          sb.append(tPath.charAt(0));
        } else {
          // 调整第一位编码，确保0号子目录可以被复用
          sb.append(Integer.toString(
            Integer.parseInt(tPath.substring(0, 1), DIRECTORIES_PER_LEVEL) - 1,
            DIRECTORIES_PER_LEVEL));
        }
        // 分层目录使用/分隔不同层级
        for (int i = 1; i < tPath.length(); i++) {
          sb.append(Path.SEPARATOR).append(tPath.charAt(i));
        }
        relativePath = sb.toString();
      }
      return relativePath;
    }

    /**
     * 从相对路径解析出全局目录编号，抵消getRelativePath中的调整逻辑
     * @param relativePath 相对路径字符串
     * @return 全局目录编号
     */
    static int getDirectoryNumber(String relativePath) {
      // 移除分隔符得到纯数字符串
      String numStr = relativePath.replace("/", "");
      // 根目录编号为0
      if (relativePath.isEmpty()) {
        return 0;
      }
      if (numStr.length() > 1) {
        // 还原getRelativePath中对第一位的调整
        String firstChar = Integer.toString(
            Integer.parseInt(numStr.substring(0, 1),
                DIRECTORIES_PER_LEVEL) + 1, DIRECTORIES_PER_LEVEL);
        numStr = firstChar + numStr.substring(1);
      }
      // 36进制解析得到编号后加1得到全局编号
      return Integer.parseInt(numStr, DIRECTORIES_PER_LEVEL) + 1;
    }

    public Directory(int directoryNo) {
      fileCount = 0;
      relativePath = getRelativePath(directoryNo);
    }

    public int incrementAndGetCount() {
      return ++fileCount;
    }

    public int decrementAndGetCount() {
      return --fileCount;
    }

    public String getRelativePath() {
      return relativePath;
    }

    public int getCount() {
      return fileCount;
    }
  }
}