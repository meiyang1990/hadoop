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
package org.apache.hadoop.hdfs.tools;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import javax.management.AttributeNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.ExitUtil;

/**
 * HDFS JMX指标查询命令行工具，通过JMX从NameNode或DataNode获取MBean指标数据。
 * 支持查询hadoop域下的多种MBean，包括FSNamesystemState、NameNodeActivity、RpcActivity等。
 * 所有日志输出到System.err，查询结果输出到System.out，符合命令行工具使用习惯。
 */
@InterfaceAudience.Private
public class JMXGet {

  private static final String format = "%s=%s%n";
  private ArrayList<ObjectName> hadoopObjectNames;
  private MBeanServerConnection mbsc;
  private String service = "NameNode", port = "", server = "localhost";
  private String localVMUrl = null;

  /**
   * 构造空的JMXGet实例，后续通过setter设置参数。
   */
  public JMXGet() {
  }

  /**
   * 设置要查询的JMX服务名称。
   * @param service 服务名称，NameNode或DataNode
   */
  public void setService(String service) {
    this.service = service;
  }

  /**
   * 设置JMX服务端口。
   * @param port JMX端口号
   */
  public void setPort(String port) {
    this.port = port;
  }

  /**
   * 设置JMX服务地址。
   * @param server 服务主机地址，默认localhost
   */
  public void setServer(String server) {
    this.server = server;
  }

  /**
   * 设置本地VM的JMX连接地址。
   * @param url 本地JMX连接器地址
   */
  public void setLocalVMUrl(String url) {
    this.localVMUrl = url;
  }

  /**
   * 打印所有匹配服务下所有MBean的所有属性值。
   * @throws Exception 连接或查询过程中可能出现异常
   */
  public void printAllValues() throws Exception {
    err("List of all the available keys:");

    Object val = null;

    for (ObjectName oname : hadoopObjectNames) {
      err(">>>>>>>>jmx name: " + oname.getCanonicalKeyPropertyListString());
      // 获取MBean元信息
      MBeanInfo mbinfo = mbsc.getMBeanInfo(oname);
      // 获取所有属性信息
      MBeanAttributeInfo[] mbinfos = mbinfo.getAttributes();

      for (MBeanAttributeInfo mb : mbinfos) {
        // 查询属性值并打印
        val = mbsc.getAttribute(oname, mb.getName());
        System.out.format(format, mb.getName(), (val==null)?"":val.toString());
      }
    }
  }

  /**
   * 打印所有匹配名称正则表达式的属性值。
   * @param attrRegExp 属性名称匹配正则表达式
   * @throws Exception 连接或查询过程中可能出现异常
   */
  public void printAllMatchedAttributes(String attrRegExp) throws Exception {
    err("List of the keys matching " + attrRegExp + " :");
    Object val = null;
    Pattern p = Pattern.compile(attrRegExp);
    for (ObjectName oname : hadoopObjectNames) {
      err(">>>>>>>>jmx name: " + oname.getCanonicalKeyPropertyListString());
      MBeanInfo mbinfo = mbsc.getMBeanInfo(oname);
      MBeanAttributeInfo[] mbinfos = mbinfo.getAttributes();
      for (MBeanAttributeInfo mb : mbinfos) {
        // 匹配属性名称前缀
        if (p.matcher(mb.getName()).lookingAt()) {
          val = mbsc.getAttribute(oname, mb.getName());
          System.out.format(format, mb.getName(), (val == null) ? "" : val.toString());
        }
      }
    }
  }

  /**
   * 根据属性名称查询单个属性值。
   * @param key 要查询的属性名称
   * @return 属性值字符串，未找到返回空字符串
   * @throws Exception 连接或查询过程中可能出现异常
   */
  public String getValue(String key) throws Exception {

    Object val = null;

    // 遍历所有匹配的MBean查找属性
    for (ObjectName oname : hadoopObjectNames) {
      try {
        val = mbsc.getAttribute(oname, key);
      } catch (AttributeNotFoundException anfe) {
        // 当前MBean不存在该属性，继续查找下一个
        continue;
      } catch (ReflectionException re) {
        // 方法不存在也继续查找下一个
        if (re.getCause() instanceof NoSuchMethodException) {
          continue;
        }
      }
      err("Info: key = " + key + "; val = " +
          (val == null ? "null" : val.getClass()) + ":" + val);
      break;
    }

    return (val == null) ? "" : val.toString();
  }

  /**
   * 初始化JMX连接，获取MBean服务器连接并查询对应服务的MBean列表。
   * @throws Exception 初始化过程中可能出现连接、查询异常
   */
  public void init() throws Exception {

    err("init: server=" + server + ";port=" + port + ";service=" + service
        + ";localVMUrl=" + localVMUrl);

    String url_string = null;
    // 构建JMX连接地址
    if (localVMUrl != null) {
      // 使用本地VM提供的JMX地址连接
      url_string = localVMUrl;
      err("url string for local pid = " + localVMUrl + " = " + url_string);

    } else if (!port.isEmpty() && !server.isEmpty()) {
      // 使用指定的服务端地址和端口构建RMI连接地址
      url_string = "service:jmx:rmi:///jndi/rmi://" + server + ":" + port
      + "/jmxrmi";
    } // 否则地址为空，使用本地VM平台MBean服务器

    // 创建RMI连接器客户端，连接到RMI连接器服务器

    if (url_string == null) { // 假设连接到本地VM（多用于测试）
      mbsc = ManagementFactory.getPlatformMBeanServer();
    } else {
      JMXServiceURL url = new JMXServiceURL(url_string);

      err("Create RMI connector and connect to the RMI connector server" + url);

      // 建立JMX连接
      JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
      // 获取MBean服务器连接
      err("\nGet an MBeanServerConnection");
      mbsc = jmxc.getMBeanServerConnection();
    }

    // 从MBean服务器获取所有域
    err("\nDomains:");

    String domains[] = mbsc.getDomains();
    Arrays.sort(domains);
    for (String domain : domains) {
      err("\tDomain = " + domain);
    }

    // 获取MBean服务器默认域
    err("\nMBeanServer default domain = " + mbsc.getDefaultDomain());

    // 获取MBean总数
    err("\nMBean count = " + mbsc.getMBeanCount());

    // 查询特定hadoop域和服务的MBean名称
    ObjectName query = new ObjectName("Hadoop:service=" + service + ",*");
    hadoopObjectNames = new ArrayList<ObjectName>(5);
    err("\nQuery MBeanServer MBeans:");
    // 执行查询并排序结果
    Set<ObjectName> names = new TreeSet<ObjectName>(mbsc
        .queryNames(query, null));

    for (ObjectName name : names) {
      hadoopObjectNames.add(name);
      err("Hadoop service: " + name);
    }

  }

  /**
   * 打印工具帮助信息。
   * @param opts 命令行选项定义
   */
  static void printUsage(Options opts) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("jmxget options are: ", opts);
  }

  /**
   * 输出错误信息到标准错误流。
   * @param msg 错误信息内容
   */
  private static void err(String msg) {
    System.err.println(msg);
  }

  /**
   * 解析命令行参数，生成命令行对象。
   * @param opts 命令行选项定义对象
   * @param args 原始命令行参数数组
   * @return 解析后的命令行对象
   * @throws IllegalArgumentException 参数解析失败抛出异常
   */
  private static CommandLine parseArgs(Options opts, String... args)
  throws IllegalArgumentException {

    // 定义服务选项
    Option jmxService = Option.builder("service")
        .argName("NameNode|DataNode").hasArg()
        .desc("specify jmx service (NameNode by default)").build();

    // 定义服务地址选项
    Option jmxServer = Option.builder("server")
        .argName("mbean server").hasArg()
        .desc("specify mbean server (localhost by default)").build();

    // 定义帮助选项
    Option jmxHelp = Option.builder("help").desc("print help").build();

    // 定义端口选项
    Option jmxPort = Option.builder("port")
        .argName("mbean server port")
        .hasArg().desc("specify mbean server port, "
        + "if missing - it will try to connect to MBean Server in the same VM").build();

    // 定义本地VM连接选项
    Option jmxLocalVM = Option.builder("localVM")
        .argName("VM's connector url").hasArg()
        .desc("connect to the VM on the same machine;"
        + "\n use:\n jstat -J-Djstat.showUnsupported=true -snap <vmpid> | "
        + "grep sun.management.JMXConnectorServer.address\n "
        + "to find the url").build();

    opts.addOption(jmxServer);
    opts.addOption(jmxHelp);
    opts.addOption(jmxService);
    opts.addOption(jmxPort);
    opts.addOption(jmxLocalVM);

    CommandLine commandLine = null;
    CommandLineParser parser = new GnuParser();
    try {
      commandLine = parser.parse(opts, args, true);
    } catch (ParseException e) {
      printUsage(opts);
      throw new IllegalArgumentException("invalid args: " + e.getMessage());
    }
    return commandLine;
  }

  /**
   * JMXGet工具主入口，处理命令行参数并执行查询。
   * @param args 命令行参数
   */
  public static void main(String[] args) {
    int res = -1;

    // 解析命令行参数
    Options opts = new Options();
    CommandLine commandLine = null;
    try {
      commandLine = parseArgs(opts, args);
    } catch (IllegalArgumentException iae) {
      commandLine = null;
    }

    if (commandLine == null) {
      // 参数非法，打印帮助并退出
      err("Invalid args");
      printUsage(opts);
      ExitUtil.terminate(-1);      
    }

    JMXGet jm = new JMXGet();

    // 处理端口参数
    if (commandLine.hasOption("port")) {
      jm.setPort(commandLine.getOptionValue("port"));
    }
    // 处理服务参数
    if (commandLine.hasOption("service")) {
      jm.setService(commandLine.getOptionValue("service"));
    }
    // 处理服务地址参数
    if (commandLine.hasOption("server")) {
      jm.setServer(commandLine.getOptionValue("server"));
    }

    // 处理本地VM连接参数
    if (commandLine.hasOption("localVM")) {
      jm.setLocalVMUrl(commandLine.getOptionValue("localVM"));
    }

    // 处理帮助请求
    if (commandLine.hasOption("help")) {
      printUsage(opts);
      ExitUtil.terminate(0);
    }

    // 获取剩余参数（要查询的属性名称）
    args = commandLine.getArgs();

    try {
      // 初始化JMX连接
      jm.init();

      if (args.length == 0) {
        // 无参数，打印所有属性
        jm.printAllValues();
      } else {
        // 逐个查询指定属性并打印结果
        for (String key : args) {
          err("key = " + key);
          String val = jm.getValue(key);
          if (val != null)
            System.out.format(JMXGet.format, key, val);
        }
      }
      // 执行成功，返回码0
      res = 0;
    } catch (Exception re) {
      // 执行异常，打印堆栈，返回码-1
      re.printStackTrace();
      res = -1;
    }

    ExitUtil.terminate(res);
  }
}