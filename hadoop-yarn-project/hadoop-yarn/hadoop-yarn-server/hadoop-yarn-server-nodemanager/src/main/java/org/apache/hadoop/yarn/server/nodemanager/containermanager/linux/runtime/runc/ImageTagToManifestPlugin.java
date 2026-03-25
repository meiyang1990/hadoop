// 这个文件已经全部加上中文注释
/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_RUNC_CACHE_REFRESH_INTERVAL;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_RUNC_IMAGE_TOPLEVEL_DIR;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NUM_MANIFESTS_TO_CACHE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_HDFS_RUNC_IMAGE_TAG_TO_HASH_FILE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_LOCAL_RUNC_IMAGE_TAG_TO_HASH_FILE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_CACHE_REFRESH_INTERVAL;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_IMAGE_TOPLEVEL_DIR;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_NUM_MANIFESTS_TO_CACHE;

/**
 * 为RuncContainerRuntime提供镜像标签到runC镜像清单转换的插件，支持本地和HDFS两层标签映射缓存
 * 实现从用户友好的镜像标签获取对应SHA256哈希对应的镜像清单
 */
@InterfaceStability.Unstable
public class ImageTagToManifestPlugin extends AbstractService
    implements RuncImageTagToManifestPlugin {

  // 镜像清单缓存，key为镜像SHA256哈希
  private Map<String, ImageManifest> manifestCache;
  // JSON对象映射器，用于解析镜像清单JSON
  private ObjectMapper objMapper;
  // 本地文件系统中镜像标签到哈希的缓存，原子引用保证线程安全
  private AtomicReference<Map<String, String>> localImageToHashCache =
      new AtomicReference<>(new HashMap<>());
  // HDFS中镜像标签到哈希的缓存，原子引用保证线程安全
  private AtomicReference<Map<String, String>> hdfsImageToHashCache =
      new AtomicReference<>(new HashMap<>());
  // YARN配置对象
  private Configuration conf;
  // 定时刷新缓存的线程池
  private ScheduledExecutorService exec;
  // HDFS映射文件上次修改时间，用于判断是否需要重新加载
  private long hdfsModTime;
  // 本地映射文件上次修改时间，用于判断是否需要重新加载
  private long localModTime;
  // HDFS上标签哈希映射文件路径
  private String hdfsImageToHashFile;
  // 镜像清单根目录
  private String manifestDir;
  // 本地标签哈希映射文件路径
  private String localImageTagToHashFile;

  private static final Logger LOG = LoggerFactory.getLogger(ImageTagToManifestPlugin.class);

  // SHA256哈希固定长度为64字符
  private static final int SHA256_HASH_LENGTH = 64;
  // 哈希仅允许字母数字字符
  private static final String ALPHA_NUMERIC = "[a-zA-Z0-9]+";

  public ImageTagToManifestPlugin() {
    super("ImageTagToManifestPluginService");
  }

  @Override
  /**
   * 根据镜像标签获取对应runC镜像清单
   * @param imageTag 镜像标签
   * @return 镜像清单对象
   * @throws IOException 读取清单文件失败时抛出
   */
  public ImageManifest getManifestFromImageTag(String imageTag)
      throws IOException {
    // 先从标签映射获取对应哈希
    String hash = getHashFromImageTag(imageTag);
    // 查询本地缓存
    ImageManifest manifest = manifestCache.get(hash);
    if (manifest != null) {
      return manifest;
    }

    // 缓存未命中，从存储读取清单文件
    Path manifestPath = new Path(manifestDir + hash);
    FileSystem fs = manifestPath.getFileSystem(conf);
    FSDataInputStream input;
    try {
      input = fs.open(manifestPath);
    } catch (IllegalArgumentException iae) {
      throw new IOException("Manifest file is not a valid HDFS file: "
          + manifestPath.toString(), iae);
    }

    // 解析JSON为清单对象
    byte[] bytes = IOUtils.toByteArray(input);
    manifest = objMapper.readValue(bytes, ImageManifest.class);

    // 放入缓存
    manifestCache.put(hash, manifest);
    return manifest;
  }

  @Override
  /**
   * 从镜像标签解析对应SHA256哈希，优先本地缓存、其次HDFS缓存，最后假设标签本身就是哈希
   * @param imageTag 输入镜像标签
   * @return 对应的镜像哈希
   */
  public String getHashFromImageTag(String imageTag) {
    String hash;
    Map<String, String> localImageToHashCacheMap = localImageToHashCache.get();
    Map<String, String> hdfsImageToHashCacheMap = hdfsImageToHashCache.get();

    // 优先级: 1本地文件 -> 2HDFS文件 -> 3直接使用标签作为哈希
    hash = localImageToHashCacheMap.get(imageTag);
    if (hash == null) {
      hash = hdfsImageToHashCacheMap.get(imageTag);
      if (hash == null) {
        hash = imageTag;
      }
    }
    return hash;
  }

  /**
   * 获取本地标签哈希映射文件的读取器，仅文件修改后才返回非null
   * @return  BufferedReader 读取器，无需重新加载或文件不存在则返回null
   * @throws IOException 打开文件失败时抛出
   */
  protected BufferedReader getLocalImageToHashReader() throws IOException {
    if (localImageTagToHashFile == null) {
      LOG.debug("Did not load local image to hash file, " +
          "file is null");
      return null;
    }

    File imageTagToHashFile = new File(localImageTagToHashFile);
    if(!imageTagToHashFile.exists()) {
      LOG.debug("Did not load local image to hash file, " +
          "file doesn't exist");
      return null;
    }

    // 检查文件是否修改，未修改则无需重新加载
    long newLocalModTime = imageTagToHashFile.lastModified();
    if (newLocalModTime == localModTime) {
      LOG.debug("Did not load local image to hash file, " +
          "file is unmodified");
      return null;
    }
    localModTime = newLocalModTime;

    return new BufferedReader(new InputStreamReader(
        new FileInputStream(imageTagToHashFile), StandardCharsets.UTF_8));
  }

  /**
   * 获取HDFS标签哈希映射文件的读取器，仅文件修改后才返回非null
   * @return  BufferedReader 读取器，无需重新加载或文件不存在则返回null
   * @throws IOException 打开文件失败时抛出
   */
  protected BufferedReader getHdfsImageToHashReader() throws IOException {
    if (hdfsImageToHashFile == null) {
      LOG.debug("Did not load hdfs image to hash file, " +
          "file is null");
      return null;
    }

    Path imageToHash = new Path(hdfsImageToHashFile);
    FileSystem fs = imageToHash.getFileSystem(conf);
    if (!fs.exists(imageToHash)) {
      LOG.debug("Did not load hdfs image to hash file, " +
          "file doesn't exist");
      return null;
    }

    // 检查文件是否修改，未修改则无需重新加载
    long newHdfsModTime = fs.getFileStatus(imageToHash).getModificationTime();
    if (newHdfsModTime == hdfsModTime) {
      LOG.debug("Did not load hdfs image to hash file, " +
          "file is unmodified");
      return null;
    }
    hdfsModTime = newHdfsModTime;

    return new BufferedReader(new InputStreamReader(fs.open(imageToHash),
        StandardCharsets.UTF_8));
  }

  /** You may specify multiple tags per hash all on the same line.
   * Comments are allowed using #. Anything after this character will not
   * be read
   * Example file:
   * foo/bar:current,fizz/gig:latest:123456789
   * #this/line:wont,be:parsed:2378590895

   * This will map both foo/bar:current and fizz/gig:latest to 123456789
   */
  /**
   * 从BufferedReader读取标签哈希映射关系，解析文件格式并构建映射表
   * @param br 读取器
   * @return 解析后的标签哈希映射表，读取器为null返回null
   * @throws IOException 读取文件失败时抛出
   */
  protected static Map<String, String> readImageToHashFile(
      BufferedReader br) throws IOException {
    if (br == null) {
      return null;
    }

    String line;
    Map<String, String> imageToHashCache = new HashMap<>();
    while ((line = br.readLine()) != null) {
      int index;
      // 处理注释，#开头整行跳过，中间包含则截断后面部分
      index = line.indexOf("#");
      if (index == 0) {
        continue;
      } else if (index != -1) {
        line = line.substring(0, index);
      }

      // 最后一个冒号分隔标签部分和哈希部分
      index = line.lastIndexOf(":");
      if (index == -1) {
        LOG.warn("Malformed imageTagToManifest entry: " + line);
        continue;
      }
      String imageTags = line.substring(0, index);
      // 多个标签用逗号分隔
      String[] imageTagArray = imageTags.split(",");
      String hash = line.substring(index + 1);
      // 校验哈希格式必须是64位字母数字的SHA256
      if (!hash.matches(ALPHA_NUMERIC) || hash.length() != SHA256_HASH_LENGTH) {
        LOG.warn("Malformed image hash: " + hash);
        continue;
      }

      // 将多个标签都映射到同一个哈希
      for (String imageTag : imageTagArray) {
        imageToHashCache.put(imageTag, hash);
      }
    }
    return imageToHashCache;
  }

  /**
   * 加载本地和HDFS的标签哈希映射文件，更新缓存
   * @return 是否有缓存更新
   * @throws IOException 加载文件失败时抛出
   */
  public boolean loadImageToHashFiles() throws IOException {
    boolean ret = false;
    try (
        BufferedReader localBr = getLocalImageToHashReader();
        BufferedReader hdfsBr = getHdfsImageToHashReader()
    ) {
      // 分别解析本地和HDFS文件
      Map<String, String> localImageToHash = readImageToHashFile(localBr);
      Map<String, String> hdfsImageToHash = readImageToHashFile(hdfsBr);

      Map<String, String> tmpLocalImageToHash = localImageToHashCache.get();
      Map<String, String> tmpHdfsImageToHash = hdfsImageToHashCache.get();

      // 如果本地缓存变化，更新原子引用
      if (localImageToHash != null &&
          !localImageToHash.equals(tmpLocalImageToHash)) {
        localImageToHashCache.set(localImageToHash);
        LOG.info("Reloaded local image tag to hash cache");
        ret = true;
      }
      // 如果HDFS缓存变化，更新原子引用
      if (hdfsImageToHash != null &&
          !hdfsImageToHash.equals(tmpHdfsImageToHash)) {
        hdfsImageToHashCache.set(hdfsImageToHash);
        LOG.info("Reloaded hdfs image tag to hash cache");
        ret = true;
      }
    }
    return ret;
  }

  @Override
  /**
   * 服务初始化方法，读取配置，初始化缓存和线程池
   */
  protected void serviceInit(Configuration configuration) throws Exception {
    super.serviceInit(configuration);
    this.conf = configuration;
    // 读取本地映射文件路径配置
    localImageTagToHashFile = conf.get(NM_LOCAL_RUNC_IMAGE_TAG_TO_HASH_FILE);
    if (localImageTagToHashFile == null) {
      LOG.debug("Failed to load local runC image to hash file. " +
          "Config not set");
    }
    // 读取HDFS映射文件路径配置
    hdfsImageToHashFile = conf.get(NM_HDFS_RUNC_IMAGE_TAG_TO_HASH_FILE);
    if (hdfsImageToHashFile == null) {
      LOG.debug("Failed to load HDFS runC image to hash file. Config not set");
    }
    // 两个配置都未设置打印警告
    if(hdfsImageToHashFile == null && localImageTagToHashFile == null) {
      LOG.warn("No valid image-tag-to-hash files");
    }
    // 读取镜像清单根目录配置
    manifestDir = conf.get(NM_RUNC_IMAGE_TOPLEVEL_DIR,
        DEFAULT_NM_RUNC_IMAGE_TOPLEVEL_DIR) + "/manifests/";
    // 读取缓存最大条目数配置
    int numManifestsToCache = conf.getInt(NM_RUNC_NUM_MANIFESTS_TO_CACHE,
        DEFAULT_NUM_MANIFESTS_TO_CACHE);
    this.objMapper = new ObjectMapper();
    // 初始化LRU缓存，线程安全
    this.manifestCache = Collections.synchronizedMap(
        new LRUCache(numManifestsToCache, 0.75f));

    exec = HadoopExecutors.newScheduledThreadPool(1);
  }

  @Override
  /**
   * 服务启动方法，初始加载缓存，启动定时刷新任务
   */
  protected void serviceStart() throws Exception {
    super.serviceStart();
    // 初始加载一次缓存
    if(!loadImageToHashFiles()) {
      LOG.warn("Couldn't load any image-tag-to-hash-files");
    }
    // 读取刷新间隔配置
    int runcCacheRefreshInterval = conf.getInt(NM_RUNC_CACHE_REFRESH_INTERVAL,
        DEFAULT_NM_RUNC_CACHE_REFRESH_INTERVAL);
    exec = HadoopExecutors.newScheduledThreadPool(1);
    // 启动定时刷新任务，按固定间隔执行
    exec.scheduleWithFixedDelay(
        new Runnable() {
          @Override
          public void run() {
            try {
              loadImageToHashFiles();
            } catch (Exception e) {
              LOG.warn("runC cache refresh thread caught an exception: ", e);
            }
          }
        }, runcCacheRefreshInterval, runcCacheRefreshInterval, TimeUnit.SECONDS);
  }

  @Override
  /**
   * 服务停止方法，关闭定时线程池
   */
  protected void serviceStop() throws Exception {
    super.serviceStop();
    exec.shutdownNow();
  }

  /**
   * 基于LinkedHashMap实现的LRU缓存，容量满后自动移除最早访问的条目
   */
  private static class LRUCache extends LinkedHashMap<String, ImageManifest> {
    private int cacheSize;

    LRUCache(int initialCapacity, float loadFactor) {
      super(initialCapacity, loadFactor, true);
      this.cacheSize = initialCapacity;
    }

    @Override
    protected boolean removeEldestEntry(
        Map.Entry<String, ImageManifest> eldest) {
      return this.size() > cacheSize;
    }
  }
}