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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;

/**
 * MapReduce任务临时文件异步清理队列，使用后台守护线程异步删除不再需要的文件/目录。
 * 采用单例模式，整个JVM中仅启动一个清理线程，通过异步方式避免阻塞任务主线程，提升任务执行效率。
 */
class CleanupQueue {

  public static final Logger LOG =
      LoggerFactory.getLogger(CleanupQueue.class);

  private static PathCleanupThread cleanupThread;

  /**
   * 构造单例模式的清理队列，仅初始化一次后台清理线程。
   * 后台线程以守护线程方式运行，JVM退出时自动终止。
   */
  public CleanupQueue() {
    synchronized (PathCleanupThread.class) {
      if (cleanupThread == null) {
        cleanupThread = new PathCleanupThread();
      }
    }
  }
  
  /**
   * 存储待删除路径的上下文信息，包含文件系统和路径信息，子类可扩展清理逻辑。
   */
  static class PathDeletionContext {
    String fullPath;// full path of file or dir
    FileSystem fs;

    public PathDeletionContext(FileSystem fs, String fullPath) {
      this.fs = fs;
      this.fullPath = fullPath;
    }
    
    protected String getPathForCleanup() {
      return fullPath;
    }

    /**
     * 准备待删除路径，子类可重写该方法修改路径权限/状态，以便完成删除。
     * @throws IOException 准备过程中IO异常
     */
    protected void enablePathForCleanup() throws IOException {
      // Do nothing by default.
      // Subclasses can override to provide enabling for deletion.
    }
  }

  /**
   * 将待删除路径添加到清理队列，由后台线程异步执行删除。
   * @param contexts 待删除路径上下文数组
   */
  void addToQueue(PathDeletionContext... contexts) {
    cleanupThread.addToQueue(contexts);
  }

  /**
   * 执行单个路径的删除操作，先准备路径再递归删除。
   * @param context 待删除路径上下文
   * @return 删除成功返回true，否则返回false；路径不存在也视为成功
   * @throws IOException 删除过程中IO异常
   */
  protected static boolean deletePath(PathDeletionContext context)
            throws IOException {
    context.enablePathForCleanup();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Trying to delete " + context.fullPath);
    }
    if (context.fs.exists(new Path(context.fullPath))) {
      return context.fs.delete(new Path(context.fullPath), true);
    }
    return true;
  }

  // currently used by tests only
  /**
   * 检查清理队列是否为空，仅用于单元测试。
   * @return 队列为空返回true，否则返回false
   */
  protected boolean isQueueEmpty() {
    return (cleanupThread.queue.size() == 0);
  }

  /**
   * 后台路径清理线程，持续从队列取出待删除路径执行删除，处理异常保证线程不退出。
   * 继承SubjectInheritingThread继承访问主体信息，支持安全上下文传递。
   */
  private static class PathCleanupThread extends SubjectInheritingThread {

    // 阻塞队列存储待删除路径上下文
    private LinkedBlockingQueue<PathDeletionContext> queue =
      new LinkedBlockingQueue<PathDeletionContext>();

    /**
     * 初始化清理线程，设置名称为守护线程并启动。
     */
    public PathCleanupThread() {
      setName("Directory/File cleanup thread");
      setDaemon(true);
      start();
    }

    /**
     * 将一批待删除路径添加入阻塞队列。
     * @param contexts 待删除路径上下文数组
     */
    void addToQueue(PathDeletionContext[] contexts) {
      for (PathDeletionContext context : contexts) {
        try {
          queue.put(context);
        } catch(InterruptedException ie) {}
      }
    }

    /**
     * 线程主工作循环，持续从队列取出任务执行删除，处理异常不退出循环。
     */
    public void work() {
      if (LOG.isDebugEnabled()) {
        LOG.debug(getName() + " started.");
      }
      PathDeletionContext context = null;
      while (true) {
        try {
          // 从队列阻塞获取待删除任务
          context = queue.take();
          // 执行删除，删除失败打印警告日志
          if (!deletePath(context)) {
            LOG.warn("CleanupThread:Unable to delete path " + context.fullPath);
          }
          else if (LOG.isDebugEnabled()) {
            LOG.debug("DELETED " + context.fullPath);
          }
        } catch (InterruptedException t) {
          // 处理中断异常，退出线程
          if (context == null) {
            LOG.warn("Interrupted deletion of an invalid path: Path deletion "
                + "context is null.");
          } else {
            LOG.warn("Interrupted deletion of " + context.fullPath);
          }
          return;
        } catch (Exception e) {
          // 捕获其他异常，打印日志后继续循环，不退出线程
          LOG.warn("Error deleting path " + context.fullPath + ": " + e);
        } 
      }
    }
  }
}