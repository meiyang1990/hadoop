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
package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.tools.offlineEditsViewer.OfflineEditsLoader.OfflineEditsLoaderFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 * HDFS离线编辑日志查看工具主类，用于离线解析和查看HDFS的edits编辑日志文件
 * 支持将二进制或XML格式的edits日志转换为多种输出格式，便于调试和问题排查
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class OfflineEditsViewer extends Configured implements Tool {
  private static final String HELP_OPT = "-h";
  private static final String HELP_LONGOPT = "--help";
  private final static String defaultProcessor = "xml";

  /**
   * 打印工具使用帮助信息，输出命令行参数说明和使用示例
   */  
  private void printHelp() {
    String summary =
      "Usage: bin/hdfs oev [OPTIONS] -i INPUT_FILE -o OUTPUT_FILE\n" +
      "Offline edits viewer\n" +
      "Parse a Hadoop edits log file INPUT_FILE and save results\n" +
      "in OUTPUT_FILE.\n" +
      "Required command line arguments:\n" +
      "-i,--inputFile <arg>   edits file to process, xml (case\n" +
      "                       insensitive) extension means XML format,\n" +
      "                       any other filename means binary format.\n" +
      "                       XML/Binary format input file is not allowed\n" +
      "                       to be processed by the same type processor.\n" +
      "-o,--outputFile <arg>  Name of output file. If the specified\n" +
      "                       file exists, it will be overwritten,\n" +
      "                       format of the file is determined\n" +
      "                       by -p option\n" +
      "\n" + 
      "Optional command line arguments:\n" +
      "-p,--processor <arg>   Select which type of processor to apply\n" +
      "                       against image file, currently supported\n" +
      "                       processors are: binary (native binary format\n" +
      "                       that Hadoop uses), xml (default, XML\n" +
      "                       format), stats (prints statistics about\n" +
      "                       edits file)\n" +
      "-h,--help              Display usage information and exit\n" +
      "-f,--fix-txids         Renumber the transaction IDs in the input,\n" +
      "                       so that there are no gaps or invalid\n" +
      "                       transaction IDs.\n" +
      "-r,--recover           When reading binary edit logs, use recovery \n" +
      "                       mode.  This will give you the chance to skip \n" +
      "                       corrupt parts of the edit log.\n" +
      "-v,--verbose           More verbose output, prints the input and\n" +
      "                       output filenames, for processors that write\n" +
      "                       to a file, also output to screen. On large\n" +
      "                       image files this will dramatically increase\n" +
      "                       processing time (default is false).\n";


    System.out.println(summary);
    System.out.println();
    ToolRunner.printGenericCommandUsage(System.out);
  }

  /**
   * 构建工具支持的所有命令行选项定义
   *
   * @return 完整的命令行选项集合
   */
  public static Options buildOptions() {
    Options options = new Options();

    // 添加必填的输出文件选项
    Option optionOutputFileName =
        Option.builder("o").required().hasArgs().longOpt("outputFilename").build();
    options.addOption(optionOutputFileName);

    // 添加必填的输入文件选项
    Option optionInputFilename =
        Option.builder("i").required().hasArgs().longOpt("inputFilename").build();
    options.addOption(optionInputFilename);
    
    options.addOption("p", "processor", true, "");
    options.addOption("v", "verbose", false, "");
    options.addOption("f", "fix-txids", false, "");
    options.addOption("r", "recover", false, "");
    options.addOption("h", "help", false, "");

    return options;
  }

  /**
   * 执行编辑日志处理流程，根据指定参数加载并处理edits日志，输出到目标文件
   * 
   * @param inputFileName   待处理的输入edits日志文件路径
   * @param outputFileName  输出结果文件路径
   * @param processor       输出处理器类型，仅当visitor为null时生效
   * @param flags           处理标记，控制修复、恢复、 verbose等行为
   * @param visitor         外部传入的edits访问器，可用于自定义处理逻辑
   * 
   * @return                处理成功返回0，失败返回错误码-1
   */
  public int go(String inputFileName, String outputFileName, String processor,
      Flags flags, OfflineEditsVisitor visitor)
  {
    // verbose模式下打印输入输出路径
    if (flags.getPrintToScreen()) {
      System.out.println("input  [" + inputFileName  + "]");
      System.out.println("output [" + outputFileName + "]");
    }

    // 根据文件名后缀判断输入格式是否为XML
    boolean xmlInput = StringUtils.toLowerCase(inputFileName).endsWith(".xml");
    // 禁止同格式输入输出，避免逻辑错误
    if (xmlInput && StringUtils.equalsIgnoreCase("xml", processor)) {
      System.err.println("XML format input file is not allowed"
          + " to be processed by XML processor.");
      return -1;
    } else if(!xmlInput && StringUtils.equalsIgnoreCase("binary", processor)) {
      System.err.println("Binary format input file is not allowed"
          + " to be processed by Binary processor.");
      return -1;
    }

    try {
      // 未传入自定义访问器时，根据处理器类型创建默认访问器
      if (visitor == null) {
        visitor = OfflineEditsVisitorFactory.getEditsVisitor(
            outputFileName, processor, flags.getPrintToScreen());
      }

      // 创建对应格式的加载器并执行edits日志加载处理
      OfflineEditsLoader loader = OfflineEditsLoaderFactory.
          createLoader(visitor, inputFileName, xmlInput, flags);
      loader.loadEdits();
    } catch(Exception e) {
      System.err.println("Encountered exception. Exiting: " + e.getMessage());
      e.printStackTrace(System.err);
      return -1;
    }
    return 0;
  }

  /**
   * 存储离线编辑日志处理的各类配置标记，用于控制处理行为
   */
  public static class Flags {
    private boolean printToScreen = false;
    private boolean fixTxIds = false;
    private boolean recoveryMode = false;
    
    public Flags() {
    }
    
    public boolean getPrintToScreen() {
      return printToScreen;
    }
    
    public void setPrintToScreen() {
      printToScreen = true;
    }
    
    public boolean getFixTxIds() {
      return fixTxIds;
    }
    
    public void setFixTxIds() {
      fixTxIds = true;
    }
    
    public boolean getRecoveryMode() {
      return recoveryMode;
    }
    
    public void setRecoveryMode() {
      recoveryMode = true;
    }
  }
  
  /**
   * ToolRunner入口方法，解析命令行参数并启动处理流程
   *
   * @param argv 命令行参数数组
   * @return 处理成功返回0，失败返回非0错误码
   */
  @Override
  public int run(String[] argv) throws Exception {
    Options options = buildOptions();
    // 无参数时直接打印帮助
    if(argv.length == 0) {
      printHelp();
      return 0;
    }
    // 仅输入help参数时打印帮助并退出
    if (argv.length == 1 && isHelpOption(argv[0])) {
      printHelp();
      return 0;
    }
    CommandLineParser parser = new PosixParser();
    CommandLine cmd;
    try {
      // 解析命令行参数
      cmd = parser.parse(options, argv);
    } catch (ParseException e) {
      System.out.println(
        "Error parsing command-line options: " + e.getMessage());
      printHelp();
      return -1;
    }
    
    // 参数中包含help时打印帮助
    if (cmd.hasOption("h")) {
      printHelp();
      return -1;
    }
    // 提取必填参数
    String inputFileName = cmd.getOptionValue("i");
    String outputFileName = cmd.getOptionValue("o");
    String processor = cmd.getOptionValue("p");
    // 未指定处理器时使用默认值XML
    if(processor == null) {
      processor = defaultProcessor;
    }
    // 根据命令行选项构建处理标记
    Flags flags = new Flags();
    if (cmd.hasOption("r")) {
      flags.setRecoveryMode();
    }
    if (cmd.hasOption("f")) {
      flags.setFixTxIds();
    }
    if (cmd.hasOption("v")) {
      flags.setPrintToScreen();
    }
    // 启动处理流程
    return go(inputFileName, outputFileName, processor, flags, null);
  }

  /**
   * 主方法，通过ToolRunner启动离线编辑日志查看工具
   *
   * @param argv 命令行参数
   */
  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new OfflineEditsViewer(), argv);
    System.exit(res);
  }

  /**
   * 判断输入参数是否为帮助请求
   * @param arg 输入参数
   * @return 如果是帮助选项返回true，否则返回false
   */
  private static boolean isHelpOption(String arg) {
    return arg.equalsIgnoreCase(HELP_OPT) ||
        arg.equalsIgnoreCase(HELP_LONGOPT);
  }
}