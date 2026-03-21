// 这个文件已经全部加上中文注释
/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE
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

import org.apache.hadoop.classification.InterfaceStability;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonRawValue;

/**
 * Runc容器执行器配置类，用于序列化生成JSON配置传递给container-executor。
 * 对应OCI runtime规范，完整定义了runc容器运行所需的所有配置结构。
 * 用于 {@link org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.RuncContainerRuntime}
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@InterfaceStability.Unstable
public class RuncContainerExecutorConfig {
  final private String version;
  final private String runAsUser;
  final private String username;
  final private String containerId;
  final private String applicationId;
  final private String pidFile;
  final private String containerScriptPath;
  final private String containerCredentialsPath;
  final private int https;
  final private String keystorePath;
  final private String truststorePath;
  final private List<String> localDirs;
  final private List<String> logDirs;
  final private List<OCILayer> layers;
  final private int reapLayerKeepCount;
  final private OCIRuntimeConfig ociRuntimeConfig;

  public String getVersion() {
    return version;
  }

  public String getRunAsUser() {
    return runAsUser;
  }

  public String getUsername() {
    return username;
  }

  public String getContainerId() {
    return containerId;
  }

  public String getApplicationId() {
    return applicationId;
  }

  public String getPidFile() {
    return pidFile;
  }

  public String getContainerScriptPath() {
    return containerScriptPath;
  }

  public String getContainerCredentialsPath() {
    return containerCredentialsPath;
  }

  public int getHttps() {
    return https;
  }
  public String getKeystorePath() {
    return keystorePath;
  }
  public String getTruststorePath() {
    return truststorePath;
  }
  public List<String> getLocalDirs() {
    return localDirs;
  }

  public List<String> getLogDirs() {
    return logDirs;
  }

  public List<OCILayer> getLayers() {
    return layers;
  }

  public int getReapLayerKeepCount() {
    return reapLayerKeepCount;
  }

  public OCIRuntimeConfig getOciRuntimeConfig() {
    return ociRuntimeConfig;
  }

  /**
   * 空构造函数，用于Jackson反序列化
   */
  public RuncContainerExecutorConfig() {
    this(null, null, null, null, null, null, null, null, 0, null,
        null, null, null, null, 0, null);
  }

  /**
   * 带版本默认值的构造函数
   * @param runAsUser 运行用户
   * @param username 用户名
   * @param containerId 容器ID
   * @param applicationId 应用ID
   * @param pidFile PID文件路径
   * @param containerScriptPath 容器脚本路径
   * @param containerCredentialsPath 容器凭证路径
   * @param https HTTPS启用标识
   * @param keystorePath 密钥库路径
   * @param truststorePath 信任库路径
   * @param localDirs 本地目录列表
   * @param logDirs 日志目录列表
   * @param layers OCI镜像层列表
   * @param reapLayerKeepCount 回收层保留数量
   * @param ociRuntimeConfig OCI运行时配置
   */
  public RuncContainerExecutorConfig(String runAsUser, String username,
      String containerId, String applicationId,
      String pidFile, String containerScriptPath,
      String containerCredentialsPath,
      int https, String keystorePath, String truststorePath,
      List<String> localDirs,
      List<String> logDirs, List<OCILayer> layers, int reapLayerKeepCount,
      OCIRuntimeConfig ociRuntimeConfig) {
    this("0.1", runAsUser, username, containerId, applicationId, pidFile,
        containerScriptPath, containerCredentialsPath, https, keystorePath,
        truststorePath, localDirs, logDirs,
        layers, reapLayerKeepCount, ociRuntimeConfig);
  }

  /**
   * 全参数构造函数
   * @param version 配置版本
   * @param runAsUser 运行用户
   * @param username 用户名
   * @param containerId 容器ID
   * @param applicationId 应用ID
   * @param pidFile PID文件路径
   * @param containerScriptPath 容器脚本路径
   * @param containerCredentialsPath 容器凭证路径
   * @param https HTTPS启用标识
   * @param keystorePath 密钥库路径
   * @param truststorePath 信任库路径
   * @param localDirs 本地目录列表
   * @param logDirs 日志目录列表
   * @param layers OCI镜像层列表
   * @param reapLayerKeepCount 回收层保留数量
   * @param ociRuntimeConfig OCI运行时配置
   */
  public RuncContainerExecutorConfig(String version, String runAsUser,
      String username, String containerId, String applicationId,
      String pidFile, String containerScriptPath,
      String containerCredentialsPath,
      int https, String keystorePath, String truststorePath,
      List<String> localDirs,
      List<String> logDirs, List<OCILayer> layers, int reapLayerKeepCount,
      OCIRuntimeConfig ociRuntimeConfig) {
    this.version = version;
    this.runAsUser = runAsUser;
    this.username = username;
    this.containerId = containerId;
    this.applicationId = applicationId;
    this.pidFile = pidFile;
    this.containerScriptPath = containerScriptPath;
    this.containerCredentialsPath = containerCredentialsPath;
    this.https = https;
    this.keystorePath = keystorePath;
    this.truststorePath = truststorePath;
    this.localDirs = localDirs;
    this.logDirs = logDirs;
    this.layers = layers;
    this.reapLayerKeepCount = reapLayerKeepCount;
    this.ociRuntimeConfig = ociRuntimeConfig;
  }

  /**
   * OCI镜像层Java表示，对应OCI镜像规范中的层结构
   */
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  @InterfaceStability.Unstable
  public static class OCILayer {
    final private String mediaType;
    final private String path;

    public String getMediaType() {
      return mediaType;
    }

    public String getPath() {
      return path;
    }

    public OCILayer(String mediaType, String path) {
      this.mediaType = mediaType;
      this.path = path;
    }

    public OCILayer() {
      this(null, null);
    }
  }

  /**
   * OCI运行时配置根结构，对应OCI Runtime Specification完整结构
   */
  @InterfaceStability.Unstable
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public static class OCIRuntimeConfig {
    final private OCIRootConfig root;
    final private List<OCIMount> mounts;
    final private OCIProcessConfig process;
    final private OCIHooksConfig hooks;
    final private OCIAnnotationsConfig annotations;
    final private OCILinuxConfig linux;

    public OCIRootConfig getRoot() {
      return root;
    }

    public List<OCIMount> getMounts() {
      return mounts;
    }

    public OCIProcessConfig getProcess() {
      return process;
    }

    public String getHostname() {
      return hostname;
    }

    public OCIHooksConfig getHooks() {
      return hooks;
    }

    public OCIAnnotationsConfig getAnnotations() {
      return annotations;
    }

    public OCILinuxConfig getLinux() {
      return linux;
    }

    final private String hostname;

    public OCIRuntimeConfig() {
      this(null, null, null, null, null, null, null);
    }


    public OCIRuntimeConfig(OCIRootConfig root, List<OCIMount> mounts,
        OCIProcessConfig process, String hostname,
        OCIHooksConfig hooks,
        OCIAnnotationsConfig annotations,
        OCILinuxConfig linux) {
      this.root = root;
      this.mounts = mounts;
      this.process = process;
      this.hostname = hostname;
      this.hooks = hooks;
      this.annotations = annotations;
      this.linux= linux;
    }

    /**
     * OCI根文件系统配置，对应OCI规范中的root节
     */
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public static class OCIRootConfig {
      public String getPath() {
        return path;
      }

      public boolean isReadonly() {
        return readonly;
      }

      final private String path;
      final private boolean readonly;

      public OCIRootConfig(String path, boolean readonly) {
        this.path = path;
        this.readonly = readonly;
      }

      public OCIRootConfig() {
        this(null, false);
      }
    }

    /**
     * OCI挂载点配置，对应OCI规范中的mounts节
     */
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public static class OCIMount {
      final private String destination;
      final private String type;
      final private String source;
      final private List<String> options;

      public String getDestination() {
        return destination;
      }

      public String getType() {
        return type;
      }

      public String getSource() {
        return source;
      }

      public List<String> getOptions() {
        return options;
      }

      public OCIMount(String destination, String type, String source,
          List<String> options) {
        this.destination = destination;
        this.type = type;
        this.source = source;
        this.options = options;
      }

      public OCIMount(String destination, String source, List<String> options) {
        this.destination = destination;
        this.type = null;
        this.source = source;
        this.options = options;
      }

      public OCIMount() {
        this(null, null, null, null);
      }
    }


    /**
     * OCI进程配置，对应OCI规范中的process节
     */
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public static class OCIProcessConfig {
      final private boolean terminal;
      final private ConsoleSize consoleSize;
      final private String cwd;
      final private List<String> env;
      final private List<String> args;
      final private RLimits rlimits;
      final private String apparmorProfile;
      final private Capabilities capabilities;
      final private boolean noNewPrivileges;
      final private int oomScoreAdj;
      final private String selinuxLabel;
      final private User user;

      public boolean isTerminal() {
        return terminal;
      }

      public ConsoleSize getConsoleSize() {
        return consoleSize;
      }

      public String getCwd() {
        return cwd;
      }

      public List<String> getEnv() {
        return env;
      }

      public List<String> getArgs() {
        return args;
      }

      public RLimits getRlimits() {
        return rlimits;
      }

      public String getApparmorProfile() {
        return apparmorProfile;
      }

      public Capabilities getCapabilities() {
        return capabilities;
      }

      public boolean isNoNewPrivileges() {
        return noNewPrivileges;
      }

      public int getOomScoreAdj() {
        return oomScoreAdj;
      }

      public String getSelinuxLabel() {
        return selinuxLabel;
      }

      public User getUser() {
        return user;
      }


      public OCIProcessConfig(boolean terminal, ConsoleSize consoleSize,
          String cwd, List<String> env, List<String> args, RLimits rlimits,
          String apparmorProfile, Capabilities capabilities,
          boolean noNewPrivileges, int oomScoreAdj, String selinuxLabel,
          User user) {
        this.terminal = terminal;
        this.consoleSize = consoleSize;
        this.cwd = cwd;
        this.env = env;
        this.args = args;
        this.rlimits = rlimits;
        this.apparmorProfile = apparmorProfile;
        this.capabilities = capabilities;
        this.noNewPrivileges = noNewPrivileges;
        this.oomScoreAdj = oomScoreAdj;
        this.selinuxLabel = selinuxLabel;
        this.user = user;
      }

      public OCIProcessConfig() {
        this(false, null, null, null, null, null, null, null,
            false, 0, null, null);
      }


      /**
       * 控制台尺寸配置，对应OCI规范中的consoleSize节
       */
      @JsonInclude(JsonInclude.Include.NON_DEFAULT)
      public static class ConsoleSize {
        public int getHeight() {
          return height;
        }

        public int getWidth() {
          return width;
        }

        final private int height;

        public ConsoleSize(int height, int width) {
          this.height = height;
          this.width = width;
        }

        public ConsoleSize() {
          this(0, 0);
        }

        final private int width;
      }

      /**
       * 资源限制配置，对应OCI规范中的rlimits节
       */
      @JsonInclude(JsonInclude.Include.NON_DEFAULT)
      public static class RLimits {
        public String getType() {
          return type;
        }

        public long getSoft() {
          return soft;
        }

        public long getHard() {
          return hard;
        }

        final private String type;

        public RLimits(String type, long soft, long hard) {
          this.type = type;
          this.soft = soft;
          this.hard = hard;
        }

        public RLimits() {
          this(null, 0, 0);
        }

        final private long soft;
        final private long hard;
      }

      /**
       * Linux能力配置，对应OCI规范中的capabilities节
       */
      @JsonInclude(JsonInclude.Include.NON_DEFAULT)
      public static class Capabilities {
        final private List<String> effective;
        final private List<String> bounding;
        final private List<String> inheritable;
        final private List<String> permitted;
        final private List<String> ambient;

        public List<String> getEffective() {
          return effective;
        }

        public List<String> getBounding() {
          return bounding;
        }

        public List<String> getInheritable() {
          return inheritable;
        }

        public List<String> getPermitted() {
          return permitted;
        }

        public List<String> getAmbient() {
          return ambient;
        }


        public Capabilities(List<String> effective, List<String> bounding,
            List<String> inheritable, List<String> permitted,
            List<String> ambient) {
          this.effective = effective;
          this.bounding = bounding;
          this.inheritable = inheritable;
          this.permitted = permitted;
          this.ambient = ambient;
        }

        public Capabilities() {
          this(null, null, null, null, null);
        }

      }

      /**
       * 用户ID配置，对应OCI规范中的user节
       */
      public static class User {
        final private int uid;
        final private int gid;
        final private List<Integer> additionalGids;

        public User(int uid, int gid, List<Integer> additionalGids) {
          this.uid = uid;
          this.gid = gid;
          this.additionalGids = additionalGids;
        }

        public User() {
          this(0, 0, null);
        }
      }
    }

    /**
     * OCI钩子配置，对应OCI规范中的hooks节
     */
    @JsonInclude(JsonInclude