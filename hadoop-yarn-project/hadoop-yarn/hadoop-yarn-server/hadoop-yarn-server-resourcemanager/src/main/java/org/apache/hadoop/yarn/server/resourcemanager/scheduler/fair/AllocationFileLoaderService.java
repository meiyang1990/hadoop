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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.XMLUtils;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.security.Permission;
import org.apache.hadoop.yarn.security.PrivilegedEntity;
import org.apache.hadoop.yarn.security.PrivilegedEntity.EntityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocation.AllocationFileParser;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocation.AllocationFileQueueParser;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocation.QueueProperties;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocation.AllocationFileQueueParser.EVERYBODY_ACL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocation.AllocationFileQueueParser.ROOT;

/**
 * 公平调度器分配配置文件加载服务，负责定期检查和热加载队列分配XML配置文件，
 * 实现公平调度器队列配置的动态更新。
 */
@Public
@Unstable
public class AllocationFileLoaderService extends AbstractService {

  public static final Logger LOG = LoggerFactory.getLogger(
      AllocationFileLoaderService.class.getName());

  /** 分配配置文件检查间隔，单位毫秒 */
  public static final long ALLOC_RELOAD_INTERVAL_MS = 10 * 1000;

  /**
   * 分配配置文件修改后等待重载的时间，避免加载未写入完成的文件，单位毫秒
   */
  public static final long ALLOC_RELOAD_WAIT_MS = 5 * 1000;

  /** 重载线程退出等待超时时间，单位毫秒 */
  public static final long THREAD_JOIN_TIMEOUT_MS = 1000;

  // 允许加载分配配置文件的文件系统，不区分大小写
  private static final String SUPPORTED_FS_REGEX =
      "(?i)(hdfs)|(file)|(s3a)|(viewfs)";

  private final Clock clock;
  private final FairScheduler scheduler;

  // 上次成功重载队列配置的时间
  private volatile long lastSuccessfulReload;
  private volatile boolean lastReloadAttemptFailed = false;

  // 分配配置XML文件路径
  private Path allocFile;
  private FileSystem fs;

  private Listener reloadListener;

  @VisibleForTesting
  long reloadIntervalMs = ALLOC_RELOAD_INTERVAL_MS;

  private Thread reloadThread;
  private volatile boolean running = true;

  public AllocationFileLoaderService(FairScheduler scheduler) {
    this(SystemClock.getInstance(), scheduler);
  }

  private List<Permission> defaultPermissions;

  AllocationFileLoaderService(Clock clock, FairScheduler scheduler) {
    super(AllocationFileLoaderService.class.getName());
    this.scheduler = scheduler;
    this.clock = clock;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    this.allocFile = getAllocationFile(conf);
    if (this.allocFile != null) {
      this.fs = allocFile.getFileSystem(conf);
      // 创建后台线程定期检查分配文件是否变更
      reloadThread = new SubjectInheritingThread(() -> {
        while (running) {
          try {
            synchronized (this) {
              reloadListener.onCheck();
            }
            long time = clock.getTime();
            // 获取分配文件最后修改时间
            long lastModified =
                fs.getFileStatus(allocFile).getModificationTime();
            // 文件已修改且等待时间已过，触发重载
            if (lastModified > lastSuccessfulReload &&
                time > lastModified + ALLOC_RELOAD_WAIT_MS) {
              try {
                reloadAllocations();
              } catch (Exception ex) {
                if (!lastReloadAttemptFailed) {
                  LOG.error("Failed to reload fair scheduler config file - " +
                      "will use existing allocations.", ex);
                }
                lastReloadAttemptFailed = true;
              }
            } else if (lastModified == 0l) {
              // 文件获取不到修改时间，输出警告
              if (!lastReloadAttemptFailed) {
                LOG.warn("Failed to reload fair scheduler config file because" +
                    " last modified returned 0. File exists: "
                    + fs.exists(allocFile));
              }
              lastReloadAttemptFailed = true;
            }
          } catch (IOException e) {
            LOG.error("Exception while loading allocation file: " + e);
          }
          try {
            // 等待下一次检查
            Thread.sleep(reloadIntervalMs);
          } catch (InterruptedException ex) {
            LOG.info(
                "Interrupted while waiting to reload alloc configuration");
          }
        }
      });
      reloadThread.setName("AllocationFileReloader");
      reloadThread.setDaemon(true);
    }
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    if (reloadThread != null) {
      reloadThread.start();
    }
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    running = false;
    if (reloadThread != null) {
      reloadThread.interrupt();
      try {
        reloadThread.join(THREAD_JOIN_TIMEOUT_MS);
      } catch (InterruptedException e) {
        LOG.warn("reloadThread fails to join.");
      }
    }
    super.serviceStop();
  }

  /**
   * 从配置中解析获取分配配置文件路径，处理相对路径和类路径查找
   *
   * @param conf 配置对象
   * @return 分配文件路径，找不到返回null
   * @throws UnsupportedFileSystemException 不支持的文件系统抛出异常
   */
  @VisibleForTesting
  public Path getAllocationFile(Configuration conf)
      throws UnsupportedFileSystemException {
    String allocFilePath = conf.get(FairSchedulerConfiguration.ALLOCATION_FILE,
        FairSchedulerConfiguration.DEFAULT_ALLOCATION_FILE);
    Path allocPath = new Path(allocFilePath);
    String allocPathScheme = allocPath.toUri().getScheme();
    // 检查文件系统是否在支持列表中
    if(allocPathScheme != null && !allocPathScheme.matches(SUPPORTED_FS_REGEX)){
      throw new UnsupportedFileSystemException("Allocation file "
          + allocFilePath + " uses an unsupported filesystem");
    } else if (!allocPath.isAbsolute()) {
      // 相对路径从类路径查找
      URL url = Thread.currentThread().getContextClassLoader()
          .getResource(allocFilePath);
      if (url == null) {
        LOG.warn(allocFilePath + " not found on the classpath.");
        allocPath = null;
      } else if (!url.getProtocol().equalsIgnoreCase("file")) {
        throw new RuntimeException("Allocation file " + url
            + " found on the classpath is not on the local filesystem.");
      } else {
        allocPath = new Path(url.getProtocol(), null, url.getPath());
      }
    } else if (allocPath.isAbsoluteAndSchemeAuthorityNull()){
      // 绝对路径无scheme默认添加file scheme
      allocPath = new Path("file", null, allocFilePath);
    }
    return allocPath;
  }

  public synchronized void setReloadListener(Listener reloadListener) {
    this.reloadListener = reloadListener;
  }

  /**
   * 从分配配置XML文件重新加载队列分配信息，解析后通知监听器更新配置
   *
   * @throws IOException 无法读取配置文件抛出
   * @throws AllocationConfigurationException 分配配置非法抛出
   * @throws ParserConfigurationException XML parser配置错误抛出
   * @throws SAXException 配置文件格式错误抛出
   */
  public synchronized void reloadAllocations()
      throws IOException, ParserConfigurationException, SAXException,
      AllocationConfigurationException {
    if (allocFile == null) {
      reloadListener.onReload(null);
      return;
    }
    LOG.info("Loading allocation file " + allocFile);

    // 初始化安全XML parser，读取并解析分配文件
    DocumentBuilderFactory docBuilderFactory = XMLUtils.newSecureDocumentBuilderFactory();
    docBuilderFactory.setIgnoringComments(true);
    DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
    Document doc = builder.parse(fs.open(allocFile));
    Element root = doc.getDocumentElement();
    // 检查根节点是否为allocations
    if (!"allocations".equals(root.getTagName())) {
      throw new AllocationConfigurationException("Bad fair scheduler config "
          + "file: top-level element not <allocations>");
    }
    NodeList elements = root.getChildNodes();

    // 解析分配文件整体结构
    AllocationFileParser allocationFileParser =
        new AllocationFileParser(elements);
    allocationFileParser.parse();

    // 解析队列配置信息
    AllocationFileQueueParser queueParser =
        new AllocationFileQueueParser(allocationFileParser.getQueueElements());
    QueueProperties queueProperties = queueParser.parse();

    // 加载队列放置策略
    getQueuePlacementPolicy(allocationFileParser);
    // 设置根队列默认属性
    setupRootQueueProperties(allocationFileParser, queueProperties);

    // 创建预留队列全局配置
    ReservationQueueConfiguration globalReservationQueueConfig =
        createReservationQueueConfig(allocationFileParser);

    // 构建完整分配配置对象
    AllocationConfiguration info = new AllocationConfiguration(queueProperties,
        allocationFileParser, globalReservationQueueConfig);

    // 更新重载时间和状态
    lastSuccessfulReload = clock.getTime();
    lastReloadAttemptFailed = false;

    // 通知监别完成配置重载
    reloadListener.onReload(info);
  }

  /**
   * 从分配文件或调度器配置加载队列放置策略
   */
  private void getQueuePlacementPolicy(
      AllocationFileParser allocationFileParser)
      throws AllocationConfigurationException {
    if (allocationFileParser.getQueuePlacementPolicy().isPresent()) {
      QueuePlacementPolicy.fromXml(
          allocationFileParser.getQueuePlacementPolicy().get(),
          scheduler);
    } else {
      QueuePlacementPolicy.fromConfiguration(scheduler);
    }
  }

  /**
   * 为根队列设置默认抢占超时和阈值，若分配文件未指定则使用全局默认值
   */
  private void setupRootQueueProperties(
      AllocationFileParser allocationFileParser,
      QueueProperties queueProperties) {
    // 设置根队列最小资源抢占超时
    if (!queueProperties.getMinSharePreemptionTimeouts()
        .containsKey(QueueManager.ROOT_QUEUE)) {
      queueProperties.getMinSharePreemptionTimeouts().put(
          QueueManager.ROOT_QUEUE,
          allocationFileParser.getDefaultMinSharePreemptionTimeout());
    }
    // 设置根队列公平份额抢占超时
    if (!queueProperties.getFairSharePreemptionTimeouts()
        .containsKey(QueueManager.ROOT_QUEUE)) {
      queueProperties.getFairSharePreemptionTimeouts().put(
          QueueManager.ROOT_QUEUE,
          allocationFileParser.getDefaultFairSharePreemptionTimeout());
    }

    // 设置根队列公平份额抢占阈值
    if (!queueProperties.getFairSharePreemptionThresholds()
        .containsKey(QueueManager.ROOT_QUEUE)) {
      queueProperties.getFairSharePreemptionThresholds().put(
          QueueManager.ROOT_QUEUE,
          allocationFileParser.getDefaultFairSharePreemptionThreshold());
    }
  }

  /**
   * 从分配文件解析创建全局预留队列配置
   */
  private ReservationQueueConfiguration createReservationQueueConfig(
      AllocationFileParser allocationFileParser) {
    ReservationQueueConfiguration globalReservationQueueConfig =
        new ReservationQueueConfiguration();
    if (allocationFileParser.getReservationPlanner().isPresent()) {
      globalReservationQueueConfig
          .setPlanner(allocationFileParser.getReservationPlanner().get());
    }
    if (allocationFileParser.getReservationAdmissionPolicy().isPresent()) {
      globalReservationQueueConfig.setReservationAdmissionPolicy(
          allocationFileParser.getReservationAdmissionPolicy().get());
    }
    if (allocationFileParser.getReservationAgent().isPresent()) {
      globalReservationQueueConfig.setReservationAgent(
          allocationFileParser.getReservationAgent().get());
    }
    return globalReservationQueueConfig;
  }

  /**
   * 返回默认权限列表，根队列默认开放所有人访问，其他队列默认无访问权限
   * 默认权限会在分配文件权限加载前生效
   * @return 默认权限列表
   */
  protected List<Permission> getDefaultPermissions() {
    if (defaultPermissions == null) {
      defaultPermissions = new ArrayList<>();
      Map<AccessType, AccessControlList> acls =
          new HashMap<>();
      for (QueueACL acl : QueueACL.values()) {
        acls.put(SchedulerUtils.toAccessType(acl), EVERYBODY_ACL);
      }
      defaultPermissions.add(new Permission(
          new PrivilegedEntity(EntityType.QUEUE, ROOT), acls));
    }
    return defaultPermissions;
  }

  /**
   * 配置重载事件监听器接口，定义分配配置重载和检查的回调方法
   */
  public interface Listener {
    /**
     * 分配配置重载完成后回调
     * @param info 新加载的分配配置对象
     * @throws IOException IO异常
     */
    void onReload(AllocationConfiguration info) throws IOException;

    /**
     * 每次检查分配文件前回调，默认空实现
     */
    default void onCheck() {
    }
  }
}