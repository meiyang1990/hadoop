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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.UUID;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.util.FSDownload;

import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @file LocalDistributedCacheManager.java
 * @brief 为LocalJobRunner提供分布式缓存本地管理的辅助工具类
 * 
 * 该类负责在本地模式运行MapReduce作业时，本地化处理分布式缓存资源，
 * 创建符号链接、构建类加载器，并在作业完成后清理资源，模拟YARN环境的分布式缓存功能
 */
@SuppressWarnings("deprecation")
class LocalDistributedCacheManager {
  /** 日志记录器 */
  public static final Logger LOG =
      LoggerFactory.getLogger(LocalDistributedCacheManager.class);
  
  /** 本地化后的归档文件路径列表 */
  private List<String> localArchives = new ArrayList<String>();
  /** 本地化后的普通文件路径列表 */
  private List<String> localFiles = new ArrayList<String>();
  /** 需要添加到类路径的本地化资源路径列表 */
  private List<String> localClasspaths = new ArrayList<String>();
  
  /** 已创建的符号链接文件列表，用于后续清理 */
  private List<File> symlinksCreated = new ArrayList<File>();
  /** 为缓存资源创建的类加载器 */
  private URLClassLoader classLoaderCreated = null;
  
  /** 标记setup方法是否已被调用，保证方法调用顺序正确性 */
  private boolean setupCalled = false;
  
  /**
   * 本地化分布式缓存资源，并更新配置中本地化资源的引用信息
   * @param conf 作业配置对象
   * @param jobId 作业ID
   * @throws IOException 本地化过程中IO异常
   */
  public synchronized void setup(JobConf conf, JobID jobId) throws IOException {
    // 获取当前工作目录作为作业工作目录
    File workDir = new File(System.getProperty("user.dir"));
    
    // 根据分布式缓存配置生成YARN格式的本地资源列表
    Map<String, LocalResource> localResources = 
      new LinkedHashMap<String, LocalResource>();
    MRApps.setupDistributedCache(conf, localResources);

    // 提取需要添加到本地类路径的资源
    Map<String, Path> classpaths = new HashMap<String, Path>();
    Path[] archiveClassPaths = JobContextImpl.getArchiveClassPaths(conf);
    if (archiveClassPaths != null) {
      for (Path p : archiveClassPaths) {
        classpaths.put(p.toUri().getPath().toString(), p);
      }
    }
    Path[] fileClassPaths = JobContextImpl.getFileClassPaths(conf);
    if (fileClassPaths != null) {
      for (Path p : fileClassPaths) {
        classpaths.put(p.toUri().getPath().toString(), p);
      }
    }
    
    // 初始化本地化所需基础组件
    LocalDirAllocator localDirAllocator =
      new LocalDirAllocator(MRConfig.LOCAL_DIR);
    FileContext localFSFileContext = FileContext.getLocalFSFileContext();
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    
    ExecutorService exec = null;
    try {
      // 创建下载线程池，并行下载缓存资源
      ThreadFactory tf = new ThreadFactoryBuilder()
      .setNameFormat("LocalDistributedCacheManager Downloader #%d")
      .build();
      exec = HadoopExecutors.newCachedThreadPool(tf);
      // 获取本地写入目录
      Path destPath = localDirAllocator.getLocalPathForWrite(".", conf);
      Map<LocalResource, Future<Path>> resourcesToPaths = Maps.newHashMap();
      // 提交所有资源下载任务
      for (LocalResource resource : localResources.values()) {
        // 为每个资源生成唯一下载路径，避免冲突
        Path destPathForDownload = new Path(destPath,
            jobId.toString() + "_" + UUID.randomUUID().toString());
        Callable<Path> download =
            new FSDownload(localFSFileContext, ugi, conf, destPathForDownload,
                resource);
        Future<Path> future = exec.submit(download);
        resourcesToPaths.put(resource, future);
      }
      // 等待所有下载完成，处理结果
      for (Entry<String, LocalResource> entry : localResources.entrySet()) {
        LocalResource resource = entry.getValue();
        Path path;
        try {
          path = resourcesToPaths.get(resource).get();
        } catch (InterruptedException e) {
          throw new IOException(e);
        } catch (ExecutionException e) {
          throw new IOException(e);
        }
        String pathString = path.toUri().toString();
        String link = entry.getKey();
        String target = new File(path.toUri()).getPath();
        // 在工作目录创建符号链接指向本地化资源
        symlink(workDir, target, link);
        
        // 按资源类型分类保存本地化路径
        if (resource.getType() == LocalResourceType.ARCHIVE) {
          localArchives.add(pathString);
        } else if (resource.getType() == LocalResourceType.FILE) {
          localFiles.add(pathString);
        } else if (resource.getType() == LocalResourceType.PATTERN) {
          // 本地模式暂不支持PATTERN类型资源
          throw new IllegalArgumentException("Resource type PATTERN is not " +
          		"implemented yet. " + resource.getResource());
        }
        Path resourcePath;
        try {
          resourcePath = resource.getResource().toPath();
        } catch (URISyntaxException e) {
          throw new IOException(e);
        }
        LOG.info(String.format("Localized %s as %s", resourcePath, path));
        // 如果该资源需要加入类路径，记录下来
        String cp = resourcePath.toUri().getPath();
        if (classpaths.keySet().contains(cp)) {
          localClasspaths.add(path.toUri().getPath().toString());
        }
      }
    } finally {
      // 关闭线程池
      if (exec != null) {
        exec.shutdown();
      }
    }    
    // 将本地化后的资源路径更新到配置中，供后续任务使用
    if (!localArchives.isEmpty()) {
      conf.set(MRJobConfig.CACHE_LOCALARCHIVES, StringUtils
          .arrayToString(localArchives.toArray(new String[localArchives
              .size()])));
    }
    if (!localFiles.isEmpty()) {
      conf.set(MRJobConfig.CACHE_LOCALFILES, StringUtils
          .arrayToString(localFiles.toArray(new String[localArchives
              .size()])));
    }
    setupCalled = true;
  }
  
  /**
   * 在工作目录创建指向本地化资源的符号链接
   * @param workDir 作业工作目录
   * @param target 本地化后的资源目标路径
   * @param link 符号链接名称
   * @throws IOException 创建符号链接失败抛出异常
   */
  private void symlink(File workDir, String target, String link)
      throws IOException {
    if (link != null) {
      link = workDir.toString() + Path.SEPARATOR + link;
      File flink = new File(link);
      if (!flink.exists()) {
        LOG.info(String.format("Creating symlink: %s <- %s", target, link));
        if (0 != FileUtil.symLink(target, link)) {
          LOG.warn(String.format("Failed to create symlink: %s <- %s", target,
              link));
        } else {
          symlinksCreated.add(new File(link));
        }
      }
    }
  }
  
  /**
   * 检查是否有需要添加到类路径的本地化资源
   * 必须在setup()调用后使用
   * @return true表示存在需要添加到类路径的资源，false则没有
   */
  public synchronized boolean hasLocalClasspaths() {
    if (!setupCalled) {
      throw new IllegalStateException(
          "hasLocalClasspaths() should be called after setup()");
    }
    return !localClasspaths.isEmpty();
  }
  
  /**
   * 创建包含所有本地化缓存资源的类加载器，用于加载作业依赖类
   * @param parent 父类加载器
   * @return 包含缓存资源类路径的类加载器
   * @throws MalformedURLException 类路径格式错误抛出异常
   */
  public synchronized ClassLoader makeClassLoader(final ClassLoader parent)
      throws MalformedURLException {
    if (classLoaderCreated != null) {
      throw new IllegalStateException("A classloader was already created");
    }
    final URL[] urls = new URL[localClasspaths.size()];
    // 将本地类路径转换为URL数组
    for (int i = 0; i < localClasspaths.size(); ++i) {
      urls[i] = new File(localClasspaths.get(i)).toURI().toURL();
      LOG.info(urls[i].toString());
    }
    // 特权操作创建类加载器，保持权限上下文正确
    return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
      @Override
      public ClassLoader run() {
        classLoaderCreated = new URLClassLoader(urls, parent);
        return classLoaderCreated;
      }
    });
  }

  /**
   * 清理所有本地化缓存资源，关闭类加载器、删除符号链接和下载的资源
   * @throws IOException 清理过程中IO异常
   */
  public synchronized void close() throws IOException {
    if(classLoaderCreated != null) {
      AccessController.doPrivileged(new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          try {
            classLoaderCreated.close();
            classLoaderCreated = null;
          } catch (IOException e) {
            LOG.warn("Failed to close classloader created " +
                "by LocalDistributedCacheManager");
          }
          return null;
        }
      });
    }

    // 删除所有创建的符号链接
    for (File symlink : symlinksCreated) {
      if (!symlink.delete()) {
        LOG.warn("Failed to delete symlink created by the local job runner: " +
            symlink);
      }
    }
    // 删除下载的本地化资源文件
    FileContext localFSFileContext = FileContext.getLocalFSFileContext();
    for (String archive : localArchives) {
      localFSFileContext.delete(new Path(archive), true);
    }
    for (String file : localFiles) {
      localFSFileContext.delete(new Path(file), true);
    }
  }
}