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

package org.apache.hadoop.mapreduce.v2.hs.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.v2.api.HSAdminRefreshProtocol;
import org.apache.hadoop.mapreduce.v2.hs.HSProxies;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * History Server 管理命令行工具，提供对 MapReduce 历史服务器的各类管理操作。
 * 实现了 Tool 接口，可通过 ToolRunner 执行命令行指令，支持用户组映射刷新、ACL刷新、缓存刷新等运维操作。
 */
@Private
public class HSAdmin extends Configured implements Tool {

  public HSAdmin() {
    super();
  }

  public HSAdmin(JobConf conf) {
    super(conf);
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf != null) {
      // 添加安全认证相关配置
      conf = addSecurityConfiguration(conf);
    }
    super.setConf(conf);
  }

  /**
   * 添加安全认证配置，将历史服务器Kerberos主体设置到安全配置项中。
   * @param conf 原始配置
   * @return 添加安全配置后的新配置对象
   */
  private Configuration addSecurityConfiguration(Configuration conf) {
    conf = new JobConf(conf);
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY,
        conf.get(JHAdminConfig.MR_HISTORY_PRINCIPAL, ""));
    return conf;
  }

  /**
   * 打印指定命令的使用格式说明。
   * @param cmd 需要打印帮助的命令名称
   */
  private static void printUsage(String cmd) {
    if ("-refreshUserToGroupsMappings".equals(cmd)) {
      System.err
          .println("Usage: mapred hsadmin [-refreshUserToGroupsMappings]");
    } else if ("-refreshSuperUserGroupsConfiguration".equals(cmd)) {
      System.err
          .println("Usage: mapred hsadmin [-refreshSuperUserGroupsConfiguration]");
    } else if ("-refreshAdminAcls".equals(cmd)) {
      System.err.println("Usage: mapred hsadmin [-refreshAdminAcls]");
    } else if ("-refreshLoadedJobCache".equals(cmd)) {
      System.err.println("Usage: mapred hsadmin [-refreshLoadedJobCache]");
    } else if ("-refreshJobRetentionSettings".equals(cmd)) {
      System.err
          .println("Usage: mapred hsadmin [-refreshJobRetentionSettings]");
    } else if ("-refreshLogRetentionSettings".equals(cmd)) {
      System.err
          .println("Usage: mapred hsadmin [-refreshLogRetentionSettings]");
    } else if ("-getGroups".equals(cmd)) {
      System.err.println("Usage: mapred hsadmin" + " [-getGroups [username]]");
    } else {
      System.err.println("Usage: mapred hsadmin");
      System.err.println("           [-refreshUserToGroupsMappings]");
      System.err.println("           [-refreshSuperUserGroupsConfiguration]");
      System.err.println("           [-refreshAdminAcls]");
      System.err.println("           [-refreshLoadedJobCache]");
      System.err.println("           [-refreshJobRetentionSettings]");
      System.err.println("           [-refreshLogRetentionSettings]");
      System.err.println("           [-getGroups [username]]");
      System.err.println("           [-help [cmd]]");
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
    }
  }

  /**
   * 打印指定命令或所有命令的详细帮助说明。
   * @param cmd 需要打印帮助的命令名称，为空则打印所有命令帮助
   */
  private static void printHelp(String cmd) {
    String summary = "hsadmin is the command to execute Job History server administrative commands.\n"
        + "The full syntax is: \n\n"
        + "mapred hsadmin"
        + " [-refreshUserToGroupsMappings]"
        + " [-refreshSuperUserGroupsConfiguration]"
        + " [-refreshAdminAcls]"
        + " [-refreshLoadedJobCache]"
        + " [-refreshLogRetentionSettings]"
        + " [-refreshJobRetentionSettings]"
        + " [-getGroups [username]]" + " [-help [cmd]]\n";

    String refreshUserToGroupsMappings = "-refreshUserToGroupsMappings: Refresh user-to-groups mappings\n";

    String refreshSuperUserGroupsConfiguration = "-refreshSuperUserGroupsConfiguration: Refresh superuser proxy groups mappings\n";

    String refreshAdminAcls = "-refreshAdminAcls: Refresh acls for administration of Job history server\n";

    String refreshLoadedJobCache = "-refreshLoadedJobCache: Refresh loaded job cache of Job history server\n";

    String refreshJobRetentionSettings = "-refreshJobRetentionSettings:" + 
        "Refresh job history period,job cleaner settings\n";

    String refreshLogRetentionSettings = "-refreshLogRetentionSettings:" + 
        "Refresh log retention period and log retention check interval\n";
    
    String getGroups = "-getGroups [username]: Get the groups which given user belongs to\n";

    String help = "-help [cmd]: \tDisplays help for the given command or all commands if none\n"
        + "\t\tis specified.\n";

    if ("refreshUserToGroupsMappings".equals(cmd)) {
      System.out.println(refreshUserToGroupsMappings);
    } else if ("help".equals(cmd)) {
      System.out.println(help);
    } else if ("refreshSuperUserGroupsConfiguration".equals(cmd)) {
      System.out.println(refreshSuperUserGroupsConfiguration);
    } else if ("refreshAdminAcls".equals(cmd)) {
      System.out.println(refreshAdminAcls);
    } else if ("refreshLoadedJobCache".equals(cmd)) {
      System.out.println(refreshLoadedJobCache);
    } else if ("refreshJobRetentionSettings".equals(cmd)) {
      System.out.println(refreshJobRetentionSettings);
    } else if ("refreshLogRetentionSettings".equals(cmd)) {
      System.out.println(refreshLogRetentionSettings);
    } else if ("getGroups".equals(cmd)) {
      System.out.println(getGroups);
    } else {
      System.out.println(summary);
      System.out.println(refreshUserToGroupsMappings);
      System.out.println(refreshSuperUserGroupsConfiguration);
      System.out.println(refreshAdminAcls);
      System.out.println(refreshLoadedJobCache);
      System.out.println(refreshJobRetentionSettings);
      System.out.println(refreshLogRetentionSettings);
      System.out.println(getGroups);
      System.out.println(help);
      System.out.println();
      ToolRunner.printGenericCommandUsage(System.out);
    }
  }

  /**
   * 查询指定用户所属的用户组，通过RPC调用历史服务器获取结果并打印。
   * @param usernames 待查询的用户名数组
   * @return 执行结果退出码，0表示成功
   * @throws IOException RPC调用或配置读取异常
   */
  private int getGroups(String[] usernames) throws IOException {
    // 如果未指定用户名，使用当前登录用户
    if (usernames.length == 0) {
      usernames = new String[] { UserGroupInformation.getCurrentUser()
          .getUserName() };
    }

    Configuration conf = getConf();
    // 获取历史服务器管理服务地址
    InetSocketAddress address = conf.getSocketAddr(
        JHAdminConfig.JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_PORT);

    // 创建用户映射查询协议代理
    GetUserMappingsProtocol getUserMappingProtocol = HSProxies.createProxy(
        conf, address, GetUserMappingsProtocol.class,
        UserGroupInformation.getCurrentUser());
    // 遍历查询每个用户并打印结果
    for (String username : usernames) {
      StringBuilder sb = new StringBuilder();
      sb.append(username + " :");
      for (String group : getUserMappingProtocol.getGroupsForUser(username)) {
        sb.append(" ");
        sb.append(group);
      }
      System.out.println(sb);
    }

    return 0;
  }

  /**
   * 通知历史服务器刷新用户到组的映射关系缓存。
   * @return 执行结果退出码，0表示成功
   * @throws IOException RPC调用或配置读取异常
   */
  private int refreshUserToGroupsMappings() throws IOException {
    Configuration conf = getConf();
    // 获取历史服务器管理服务地址
    InetSocketAddress address = conf.getSocketAddr(
        JHAdminConfig.JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_PORT);

    // 创建刷新协议代理
    RefreshUserMappingsProtocol refreshProtocol = HSProxies.createProxy(conf,
        address, RefreshUserMappingsProtocol.class,
        UserGroupInformation.getCurrentUser());
    // 调用刷新接口
    refreshProtocol.refreshUserToGroupsMappings();

    return 0;
  }

  /**
   * 通知历史服务器刷新超级用户代理组配置。
   * @return 执行结果退出码，0表示成功
   * @throws IOException RPC调用或配置读取异常
   */
  private int refreshSuperUserGroupsConfiguration() throws IOException {
    Configuration conf = getConf();
    // 获取历史服务器管理服务地址
    InetSocketAddress address = conf.getSocketAddr(
        JHAdminConfig.JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_PORT);

    // 创建刷新协议代理
    RefreshUserMappingsProtocol refreshProtocol = HSProxies.createProxy(conf,
        address, RefreshUserMappingsProtocol.class,
        UserGroupInformation.getCurrentUser());
    // 调用刷新接口
    refreshProtocol.refreshSuperUserGroupsConfiguration();

    return 0;
  }

  /**
   * 通知历史服务器刷新管理员访问控制列表(ACL)。
   * @return 执行结果退出码，0表示成功
   * @throws IOException RPC调用或配置读取异常
   */
  private int refreshAdminAcls() throws IOException {
    Configuration conf = getConf();
    // 获取历史服务器管理服务地址
    InetSocketAddress address = conf.getSocketAddr(
        JHAdminConfig.JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_PORT);

    // 创建历史服务器管理刷新协议代理
    HSAdminRefreshProtocol refreshProtocol = HSProxies.createProxy(conf,
        address, HSAdminRefreshProtocol.class,
        UserGroupInformation.getCurrentUser());

    // 调用刷新接口
    refreshProtocol.refreshAdminAcls();
    return 0;
  }

  /**
   * 通知历史服务器刷新已加载作业缓存。
   * @return 执行结果退出码，0表示成功
   * @throws IOException RPC调用或配置读取异常
   */
  private int refreshLoadedJobCache() throws IOException {
    Configuration conf = getConf();
    // 获取历史服务器管理服务地址
    InetSocketAddress address = conf.getSocketAddr(
        JHAdminConfig.JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_PORT);

    // 创建历史服务器管理刷新协议代理
    HSAdminRefreshProtocol refreshProtocol = HSProxies.createProxy(conf,
        address, HSAdminRefreshProtocol.class,
        UserGroupInformation.getCurrentUser());

    // 调用刷新接口
    refreshProtocol.refreshLoadedJobCache();
    return 0;
  }
    
  /**
   * 通知历史服务器刷新作业历史保留配置。
   * @return 执行结果退出码，0表示成功
   * @throws IOException RPC调用或配置读取异常
   */
  private int refreshJobRetentionSettings() throws IOException {
    Configuration conf = getConf();
    // 获取历史服务器管理服务地址
    InetSocketAddress address = conf.getSocketAddr(
        JHAdminConfig.JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_PORT);

    // 创建历史服务器管理刷新协议代理
    HSAdminRefreshProtocol refreshProtocol = HSProxies.createProxy(conf,
        address, HSAdminRefreshProtocol.class,
        UserGroupInformation.getCurrentUser());

    // 调用刷新接口
    refreshProtocol.refreshJobRetentionSettings();
    return 0;
  }

  /**
   * 通知历史服务器刷新日志保留配置。
   * @return 执行结果退出码，0表示成功
   * @throws IOException RPC调用或配置读取异常
   */
  private int refreshLogRetentionSettings() throws IOException {
    Configuration conf = getConf();
    // 获取历史服务器管理服务地址
    InetSocketAddress address = conf.getSocketAddr(
        JHAdminConfig.JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_PORT);

    // 创建历史服务器管理刷新协议代理
    HSAdminRefreshProtocol refreshProtocol = HSProxies.createProxy(conf,
        address, HSAdminRefreshProtocol.class,
        UserGroupInformation.getCurrentUser());

    // 调用刷新接口
    refreshProtocol.refreshLogRetentionSettings();
    return 0;
  }

  @Override
  /**
   * 解析命令行参数并分发执行对应管理命令。
   * @param args 命令行参数数组
   * @return 执行结果退出码，0表示成功，非0表示失败
   * @throws Exception 执行过程中发生的各类异常
   */
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      printUsage("");
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = args[i++];

    // 检查无参数命令的参数个数是否正确
    if ("-refreshUserToGroupsMappings".equals(cmd)
        || "-refreshSuperUserGroupsConfiguration".equals(cmd)
        || "-refreshAdminAcls".equals(cmd)
        || "-refreshLoadedJobCache".equals(cmd)
        || "-refreshJobRetentionSettings".equals(cmd)
        || "-refreshLogRetentionSettings".equals(cmd)) {
      if (args.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    }

    exitCode = 0;
    // 根据命令分发到对应处理方法
    if ("-refreshUserToGroupsMappings".equals(cmd)) {
      exitCode = refreshUserToGroupsMappings();
    } else if ("-refreshSuperUserGroupsConfiguration".equals(cmd)) {
      exitCode = refreshSuperUserGroupsConfiguration();
    } else if ("-refreshAdminAcls".equals(cmd)) {
      exitCode = refreshAdminAcls();
    } else if ("-refreshLoadedJobCache".equals(cmd)) {
      exitCode = refreshLoadedJobCache();
    } else if ("-refreshJobRetentionSettings".equals(cmd)) {
      exitCode = refreshJobRetentionSettings();
    } else if ("-refreshLogRetentionSettings".equals(cmd)) {
      exitCode = refreshLogRetentionSettings();
    } else if ("-getGroups".equals(cmd)) {
      // 提取用户名参数
      String[] usernames = Arrays.copyOfRange(args, i, args.length);
      exitCode = getGroups(usernames);
    } else if ("-help".equals(cmd)) {
      // 打印帮助信息
      if (i < args.length) {
        printHelp(args[i]);
      } else {
        printHelp("");
      }
    } else {
      // 未知命令处理
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": Unknown command");
      printUsage("");
    }
    return exitCode;
  }

  /**
   * HSAdmin命令行入口方法，初始化配置并启动命令执行。
   * @param args 命令行参数
   * @throws Exception 执行过程中发生的各类异常
   */
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf();
    int result = ToolRunner.run(new HSAdmin(conf), args);
    System.exit(result);
  }
}