// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.yarn.server.nodemanager.
    containermanager.linux.runtime;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FilePermission;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.AllPermission;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.Path.SEPARATOR;
import static org.apache.hadoop.util.Shell.SYSPROP_HADOOP_HOME_DIR;
import static org.apache.hadoop.yarn.api.ApplicationConstants.Environment.JAVA_HOME;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.YARN_CONTAINER_SANDBOX;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.YARN_CONTAINER_SANDBOX_POLICY_GROUP_PREFIX;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_ID_STR;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_LOCAL_DIRS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.CONTAINER_RUN_CMDS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.LOCALIZED_RESOURCES;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.USER;
/**
 * <p>This class extends the {@link DefaultLinuxContainerRuntime} specifically
 * for containers which run Java commands.  It generates a new java security
 * policy file per container and modifies the java command to enable the
 * Java Security Manager with the generated policy.</p>
 *
 * The behavior of the {@link JavaSandboxLinuxContainerRuntime} can be modified
 * using the following settings:
 *
 * <ul>
 *   <li>
 *     {@value
 *     org.apache.hadoop.yarn.conf.YarnConfiguration#YARN_CONTAINER_SANDBOX} :
 *     This yarn-site.xml setting has three options:
 *     <ul>
 *     <li>disabled - Default behavior. {@link LinuxContainerRuntime}
 *     is disabled</li>
 *     <li>permissive - JVM containers will run with Java Security Manager
 *     enabled.  Non-JVM containers will run normally</li>
 *     <li>enforcing - JVM containers will run with Java Security Manager
 *     enabled.  Non-JVM containers will be prevented from executing and an
 *     {@link ContainerExecutionException} will be thrown.</li>
 *     </ul>
 *   </li>
 *   <li>
 *     {@value
 *     org.apache.hadoop.yarn.conf.YarnConfiguration#YARN_CONTAINER_SANDBOX_FILE_PERMISSIONS}
 *     :
 *     Determines the file permissions for the application directories.  The
 *     permissions come in the form of comma separated values
 *     (e.g. read,write,execute,delete). Defaults to {@code read} for read-only.
 *   </li>
 *   <li>
 *     {@value
 *     org.apache.hadoop.yarn.conf.YarnConfiguration#YARN_CONTAINER_SANDBOX_POLICY}
 *     :
 *     Accepts canonical path to a java policy file on the local filesystem.
 *     This file will be loaded as the base policy, any additional container
 *     grants will be appended to this base file.  If not specified, the default
 *     java.policy file provided with hadoop resources will be used.
 *   </li>
 *   <li>
 *     {@value
 *     org.apache.hadoop.yarn.conf.YarnConfiguration#YARN_CONTAINER_SANDBOX_WHITELIST_GROUP}
 *     :
 *     Optional setting to specify a YARN queue which will be exempt from the
 *     sand-boxing process.
 *   </li>
 *   <li>
 *     {@value
 *     org.apache.hadoop.yarn.conf.YarnConfiguration#YARN_CONTAINER_SANDBOX_POLICY_GROUP_PREFIX}$groupName
 *     :
 *     Optional setting to map groups to java policy files.  The value is a path
 *     to the java policy file for $groupName.  A user which is a member of
 *     multiple groups with different policies will receive the superset of all
 *     the permissions across their groups.
 *   </li>
 * </ul>
 * 对Java容器提供基于Java Security Manager的沙箱隔离运行时，为每个容器生成独立安全策略文件，限制容器文件访问权限
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JavaSandboxLinuxContainerRuntime
    extends DefaultLinuxContainerRuntime {
  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultLinuxContainerRuntime.class);
  // YARN配置对象
  private Configuration configuration;
  // 沙箱运行模式
  private SandboxMode sandboxMode;

  public static final String POLICY_FILE_DIR = "nm-sandbox-policies";

  private static Path policyFileDir;
  private static final FileAttribute<Set<PosixFilePermission>> POLICY_ATTR =
      PosixFilePermissions.asFileAttribute(
          PosixFilePermissions.fromString("rwxr-xr-x"));

  // 容器ID -> 对应安全策略文件路径映射
  private Map<String, Path> containerPolicies = new HashMap<>();

  /**
   * Create an instance using the given {@link PrivilegedOperationExecutor}
   * instance for performing operations.
   *
   * @param privilegedOperationExecutor the {@link PrivilegedOperationExecutor}
   * instance
   * 构造Java沙箱容器运行时实例
   */
  public JavaSandboxLinuxContainerRuntime(
      PrivilegedOperationExecutor privilegedOperationExecutor) {
    super(privilegedOperationExecutor);
  }

  @Override
  public void initialize(Configuration conf, Context nmContext)
      throws ContainerExecutionException {
    this.configuration = conf;
    // 从配置加载沙箱运行模式
    this.sandboxMode =
        SandboxMode.get(
            this.configuration.get(YARN_CONTAINER_SANDBOX,
                YarnConfiguration.DEFAULT_YARN_CONTAINER_SANDBOX));

    super.initialize(conf, nmContext);
  }

  /**
   * Initialize the Java Security Policy directory.  Either creates the
   * directory if it doesn't exist, or clears the contents of the directory if
   * already created.
   * @throws ContainerExecutionException If unable to resolve policy directory
   * 初始化安全策略文件目录，不存在则创建，已存在则清空旧策略文件
   */
  private void initializePolicyDir() throws ContainerExecutionException {
    String hadoopTempDir = configuration.get("hadoop.tmp.dir");
    if (hadoopTempDir == null) {
      throw new ContainerExecutionException("hadoop.tmp.dir not set!");
    }
    policyFileDir = Paths.get(hadoopTempDir, POLICY_FILE_DIR);
    // 如果目录已存在，删除所有已有策略文件
    if(Files.exists(policyFileDir)){
      try (DirectoryStream<Path> stream =
         Files.newDirectoryStream(policyFileDir)){
        for(Path policyFile : stream){
          Files.delete(policyFile);
        }
      }catch(IOException e){
        throw new ContainerExecutionException("Unable to initialize policy "
            + "directory: " + e);
      }
    } else {
      try {
        // 创建策略目录并设置正确权限
        policyFileDir = Files.createDirectories(
            Paths.get(hadoopTempDir, POLICY_FILE_DIR), POLICY_ATTR);
      } catch (IOException e) {
        throw new ContainerExecutionException("Unable to create policy file " +
            "directory: " + e);
      }
    }
  }

  /**
   *  Prior to environment from being written locally need to generate
   *  policy file which limits container access to a small set of directories.
   *  Additionally the container run command needs to be modified to include
   *  flags to enable the java security manager with the generated policy.
   *  <br>
   *  The Java Sandbox will be circumvented if the user is a member of the
   *  group specified in:
   *  {@value
   *  org.apache.hadoop.yarn.conf.YarnConfiguration#YARN_CONTAINER_SANDBOX_WHITELIST_GROUP}
   *  and if they do not include the JVM flag
   *  <code>-Djava.security.manager</code>.
   *
   * @param ctx The {@link ContainerRuntimeContext} containing container
   *            setup properties.
   * @throws ContainerExecutionException Exception thrown if temporary policy
   * file directory can't be created, or if any exceptions occur during policy
   * file parsing and generation.
   * 容器启动前准备：生成容器专属Java安全策略文件，修改启动命令启用Java Security Manager
   */
  @Override
  public void prepareContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {

    @SuppressWarnings("unchecked")
    List<String> localDirs =
        ctx.getExecutionAttribute(CONTAINER_LOCAL_DIRS);
    @SuppressWarnings("unchecked")
    Map<org.apache.hadoop.fs.Path, List<String>> resources =
        ctx.getExecutionAttribute(LOCALIZED_RESOURCES);
    @SuppressWarnings("unchecked")
    List<String> commands =
        ctx.getExecutionAttribute(CONTAINER_RUN_CMDS);
    Map<String, String> env =
        ctx.getContainer().getLaunchContext().getEnvironment();
    String username =
        ctx.getExecutionAttribute(USER);

    // 仅对白名单外容器启用沙箱
    if(!isSandboxContainerWhitelisted(username, commands)) {
      String tmpDirBase = configuration.get("hadoop.tmp.dir");
      if (tmpDirBase == null) {
        throw new ContainerExecutionException("hadoop.tmp.dir not set!");
      }

      try {
        String containerID = ctx.getExecutionAttribute(CONTAINER_ID_STR);
        // 初始化策略文件目录
        initializePolicyDir();

        // 获取用户所属组对应的策略文件列表
        List<String> groupPolicyFiles =
            getGroupPolicyFiles(configuration, ctx.getExecutionAttribute(USER));
        // 创建当前容器专属策略文件
        Path policyFilePath = Files.createFile(
            Paths.get(policyFileDir.toString(),
            containerID + "-" + NMContainerPolicyUtils.POLICY_FILE),
            POLICY_ATTR);

        try(OutputStream policyOutputStream =
                Files.newOutputStream(policyFilePath)) {

          // 保存容器策略文件路径映射
          containerPolicies.put(containerID, policyFilePath);

          // 生成完整策略文件
          NMContainerPolicyUtils.generatePolicyFile(policyOutputStream,
              localDirs, groupPolicyFiles, resources, configuration);
          // 修改容器启动命令，添加Java Security Manager和策略文件参数
          NMContainerPolicyUtils.appendSecurityFlags(
              commands, env, policyFilePath, sandboxMode);
        }
      } catch (IOException e) {
        throw new ContainerExecutionException(e);
      }
    }
  }

  @Override
  public void launchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    try {
      super.launchContainer(ctx);
    } finally {
      // 容器启动后删除策略文件
      deletePolicyFiles(ctx);
    }
  }

  @Override
  public void relaunchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    try {
      super.relaunchContainer(ctx);
    } finally {
      // 容器重启动后删除策略文件
      deletePolicyFiles(ctx);
    }
  }

  /**
   * Determine if JVMSandboxLinuxContainerRuntime should be used.  This is
   * decided based on the value of
   * {@value
   * org.apache.hadoop.yarn.conf.YarnConfiguration#YARN_CONTAINER_SANDBOX}
   * @param env the environment variable settings for the operation
   * @return true if Sandbox is requested, false otherwise
   * 判断是否需要启用本Java沙箱运行时
   */
  @Override
  public boolean isRuntimeRequested(Map<String, String> env) {
    return sandboxMode != SandboxMode.disabled;
  }

  /**
   * 获取用户所属组对应的自定义安全策略文件路径列表
   */
  private static List<String> getGroupPolicyFiles(Configuration conf,
      String user) throws ContainerExecutionException {
    Groups groups = Groups.getUserToGroupsMappingService(conf);
    Set<String> userGroups;
    try {
      // 获取用户所属所有用户组
      userGroups = groups.getGroupsSet(user);
    } catch (IOException e) {
      throw new ContainerExecutionException("Container user does not exist");
    }

    // 收集所有组对应的策略文件路径
    return userGroups.stream()
        .map(group -> conf.get(YARN_CONTAINER_SANDBOX_POLICY_GROUP_PREFIX
            + group))
        .filter(groupPolicy -> groupPolicy != null)
        .collect(Collectors.toList());
  }

  /**
   * Determine if the container should be whitelisted (i.e. exempt from the
   * Java Security Manager).
   * @param username The name of the user running the container
   * @param commands The list of run commands for the container
   * @return boolean value denoting whether the container should be whitelisted.
   * @throws ContainerExecutionException If container user can not be resolved
   * 判断当前容器是否在白名单中，白名单容器免除Java沙箱限制
   */
  private boolean isSandboxContainerWhitelisted(String username,
      List<String> commands) throws ContainerExecutionException {
    String whitelistGroup = configuration.get(
        YarnConfiguration.YARN_CONTAINER_SANDBOX_WHITELIST_GROUP);
    Groups groups = Groups.getUserToGroupsMappingService(configuration);
    Set<String> userGroups;
    boolean isWhitelisted = false;

    try {
      userGroups = groups.getGroupsSet(username);
    } catch (IOException e) {
      throw new ContainerExecutionException("Container user does not exist");
    }

    // 用户属于白名单组
    if(whitelistGroup != null && userGroups.contains(whitelistGroup)) {
      // 如果命令中已经包含安全标志，则不启用白名单（强制沙箱）
      for(String cmd : commands) {
        if(cmd.contains(NMContainerPolicyUtils.SECURITY_FLAG)){
          isWhitelisted = false;
          break;
        } else {
          isWhitelisted = true;
        }
      }
    }
    return isWhitelisted;
  }

  /**
   * Deletes policy files for container specified by parameter.  Additionally
   * this method will age off any stale policy files generated by
   * {@link JavaSandboxLinuxContainerRuntime}
   * @param ctx Container context for files to be deleted
   * @throws ContainerExecutionException if unable to access or delete policy
   * files or generated policy file directory
   * 删除容器对应的安全策略文件
   */
  private void deletePolicyFiles(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    try {
      Files.delete(containerPolicies.remove(
          ctx.getExecutionAttribute(CONTAINER_ID_STR)));
    } catch (IOException e) {
      throw new ContainerExecutionException("Unable to delete policy file: "
          + e);
    }
  }

  /**
   * Enumeration of the modes the JavaSandboxLinuxContainerRuntime can use.
   * See {@link JavaSandboxLinuxContainerRuntime} for details on the
   * behavior of each setting.
   * Java沙箱运行模式枚举
   */
  public enum SandboxMode {
    // 强制模式：仅允许JVM容器运行，非JVM容器直接拒绝
    enforcing("enforcing"),
    // 宽容模式：JVM容器启用沙箱，