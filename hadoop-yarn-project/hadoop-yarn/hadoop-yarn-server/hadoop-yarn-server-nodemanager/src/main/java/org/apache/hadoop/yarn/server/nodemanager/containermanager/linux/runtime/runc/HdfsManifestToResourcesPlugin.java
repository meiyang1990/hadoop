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

import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_RUNC_IMAGE_TOPLEVEL_DIR;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_RUNC_STAT_CACHE_TIMEOUT;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_RUNC_STAT_CACHE_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_IMAGE_TOPLEVEL_DIR;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_STAT_CACHE_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_STAT_CACHE_TIMEOUT;

/**
 * 文件级注释：HDFS存储的runC容器镜像清单转YARN本地资源插件，为RuncContainerRuntime提供
 * HDFS上镜像配置层和网络层的资源定位转换能力
 *
 * This class is a plugin for the
 * {@link org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.RuncContainerRuntime}
 * that maps runC image manifests into their associated config and
 * layers that are located in HDFS.
 */
@InterfaceStability.Unstable
public class HdfsManifestToResourcesPlugin extends AbstractService implements
    RuncManifestToResourcesPlugin {
  private Configuration conf;
  private String layersDir;
  private String configDir;
  private FileSystem fs;
  private LoadingCache<Path, FileStatus> statCache;


  private static final String CONFIG_MEDIA_TYPE =
      "application/vnd.docker.container.image.v1+json";

  private static final String LAYER_TAR_GZIP_MEDIA_TYPE =
      "application/vnd.docker.image.rootfs.diff.tar.gzip";

  private static final String SHA_256 = "sha256";

  private static final String CONFIG_HASH_ALGORITHM =
      SHA_256;

  private static final String LAYER_HASH_ALGORITHM =
      SHA_256;

  private static final int SHA256_HASH_LENGTH = 64;

  private static final String ALPHA_NUMERIC = "[a-zA-Z0-9]+";

  /**
   * 构造函数，初始化服务名称
   */
  public HdfsManifestToResourcesPlugin() {
    super(HdfsManifestToResourcesPlugin.class.getName());
  }

  /**
   * 服务初始化方法，加载配置并创建文件状态缓存
   * @param configuration YARN配置对象
   */
  @Override
  public void serviceInit(Configuration configuration) {
    this.conf = configuration;
    // 读取HDFS镜像根目录配置
    String toplevelDir = conf.get(NM_RUNC_IMAGE_TOPLEVEL_DIR,
        DEFAULT_NM_RUNC_IMAGE_TOPLEVEL_DIR);
    // 拼接层文件目录路径
    this.layersDir = toplevelDir + "/layers/";
    // 拼接配置文件目录路径
    this.configDir = toplevelDir + "/config/";
    // 创建缓存加载器，从HDFS获取文件状态
    CacheLoader<Path, FileStatus> cacheLoader =
        new CacheLoader<Path, FileStatus>() {
        @Override
        public FileStatus load(@Nonnull Path path) throws Exception {
          return statBlob(path);
        }
    };
    // 读取缓存最大容量配置
    int statCacheSize = conf.getInt(NM_RUNC_STAT_CACHE_SIZE,
        DEFAULT_RUNC_STAT_CACHE_SIZE);
    // 读取缓存超时配置
    int statCacheTimeout = conf.getInt(NM_RUNC_STAT_CACHE_TIMEOUT,
        DEFAULT_NM_RUNC_STAT_CACHE_TIMEOUT);
    // 构建Guava加载缓存，设置容量和刷新间隔
    this.statCache = CacheBuilder.newBuilder().maximumSize(statCacheSize)
        .refreshAfterWrite(statCacheTimeout, TimeUnit.SECONDS)
        .build(cacheLoader);
  }

  /**
   * 服务启动方法，获取HDFS文件系统实例
   * @throws IOException 获取文件系统失败时抛出
   */
  @Override
  public void serviceStart() throws IOException {
    Path path = new Path(layersDir);
    this.fs = path.getFileSystem(conf);
  }

  /**
   * 从镜像清单转换所有层为YARN本地资源列表
   * @param manifest runC镜像清单对象
   * @return 层资源列表
   * @throws IOException 校验或资源获取失败时抛出
   */
  @Override
  public List<LocalResource> getLayerResources(ImageManifest manifest)
      throws IOException  {
    List<LocalResource> localRsrcs = new ArrayList<>();

    // 遍历所有层blob，逐个转换为本地资源
    for(ImageManifest.Blob blob : manifest.getLayers()) {
      LocalResource rsrc = getResource(blob, layersDir,
          LAYER_TAR_GZIP_MEDIA_TYPE, LAYER_HASH_ALGORITHM, ".sqsh");
      localRsrcs.add(rsrc);
    }
    return localRsrcs;
  }

  /**
   * 从镜像清单转换配置blob为YARN本地资源
   * @param manifest runC镜像清单对象
   * @return 配置本地资源
   * @throws IOException 校验或资源获取失败时抛出
   */
  public LocalResource getConfigResource(ImageManifest manifest)
      throws IOException {
    ImageManifest.Blob config = manifest.getConfig();
    return getResource(config, configDir, CONFIG_MEDIA_TYPE,
        CONFIG_HASH_ALGORITHM, "");
  }

  /**
   * 根据blob信息在HDFS定位资源，转换为YARN LocalResource对象
   * @param blob 镜像清单中的blob描述
   * @param dir HDFS中对应blob类型的存储目录
   * @param expectedMediaType 期望的媒体类型
   * @param expectedHashAlgorithm 期望的哈希算法
   * @param resourceSuffix 资源文件后缀
   * @return 转换完成的YARN本地资源对象
   * @throws IOException 校验失败或获取文件状态失败时抛出
   */
  public LocalResource getResource(ImageManifest.Blob blob,
      String dir, String expectedMediaType,
      String expectedHashAlgorithm, String resourceSuffix) throws IOException {
    String mediaType = blob.getMediaType();
    // 校验媒体类型是否匹配
    if (!mediaType.equals(expectedMediaType)) {
      throw new IOException("Invalid blob mediaType: " + mediaType);
    }

    // 拆分摘要为算法和哈希两部分
    String[] blobDigest = blob.getDigest().split(":", 2);

    String algorithm = blobDigest[0];
    // 校验哈希算法是否匹配
    if (!algorithm.equals(expectedHashAlgorithm)) {
      throw new IOException("Invalid blob digest algorithm: " + algorithm);
    }

    String hash = blobDigest[1];
    // 校验哈希格式是否合法
    if (!hash.matches(ALPHA_NUMERIC) || hash.length() != SHA256_HASH_LENGTH) {
      throw new IOException("Malformed blob digest: " + hash);
    }

    long size = blob.getSize();
    // 拼接HDFS资源路径
    Path path = new Path(dir, hash + resourceSuffix);
    LocalResource rsrc;

    try {
      // 从缓存获取文件状态，不存在则自动加载
      FileStatus stat = statCache.get(path);
      long timestamp = stat.getModificationTime();
      // 转换路径为YARN URL
      URL url = URL.fromPath(path);

      // 创建公开可见的文件类型本地资源
      rsrc = LocalResource.newInstance(url,
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC,
        size, timestamp);
    } catch (ExecutionException e) {
      throw new IOException(e);
    }

    return rsrc;
  }

  /**
   * 从HDFS获取指定路径的文件状态
   * @param path HDFS文件路径
   * @return 文件状态对象
   * @throws IOException 获取失败时抛出
   */
  protected FileStatus statBlob(Path path) throws IOException {
    return fs.getFileStatus(path);
  }
}