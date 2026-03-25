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

package org.apache.hadoop.mapreduce.lib.aggregate;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 文件级注释：聚合计算框架作业工厂类，用于快捷创建基于ValueAggregator框架的MapReduce聚合作业
 * 
 * 本类是Aggregate聚合框架的主入口类，Aggregate框架是MapReduce框架的特殊化实现，
 * 专门用于处理各类简单聚合统计任务，抽象出了通用聚合逻辑，用户只需要实现简单的描述符接口
 * 即可快速完成聚合计算作业，无需重复编写完整的Map/Reduce逻辑。
 * 
 * 内置聚合器支持：数值求和、去重计数、值直方图、最值、中位数、平均值、标准差等常用统计
 * 
 * @see ValueAggregatorDescriptor
 * @see ValueAggregatorBaseDescriptor
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ValueAggregatorJob {

  /**
   * 创建ValueAggregator聚合作业组，支持指定自定义聚合描述符
   * @param args 命令行参数，包含输入路径、输出路径等配置
   * @param descriptors 自定义聚合描述符数组
   * @return 包含聚合作业的JobControl容器
   * @throws IOException 创建过程中IO异常
   */
  public static JobControl createValueAggregatorJobs(String args[],
    Class<? extends ValueAggregatorDescriptor>[] descriptors) 
  throws IOException {
    
    JobControl theControl = new JobControl("ValueAggregatorJobs");
    ArrayList<ControlledJob> dependingJobs = new ArrayList<ControlledJob>();
    Configuration conf = new Configuration();
    if (descriptors != null) {
      conf = setAggregatorDescriptors(descriptors);
    }
    Job job = createValueAggregatorJob(conf, args);
    ControlledJob cjob = new ControlledJob(job, dependingJobs);
    theControl.addJob(cjob);
    return theControl;
  }

  /**
   * 创建ValueAggregator聚合作业组，使用默认配置（从参数读取描述符）
   * @param args 命令行参数
   * @return 包含聚合作业的JobControl容器
   * @throws IOException 创建过程中IO异常
   */
  public static JobControl createValueAggregatorJobs(String args[]) 
      throws IOException {
    return createValueAggregatorJobs(args, null);
  }
  
  /**
   * 创建基于Aggregate框架的MapReduce作业
   * @param conf 作业配置对象
   * @param args 作业创建参数，支持通用Hadoop命令行参数
   * @return 配置完成可提交的Job对象
   * @throws IOException 创建过程中IO异常
   * @see GenericOptionsParser
   */
  public static Job createValueAggregatorJob(Configuration conf, String args[])
      throws IOException {

    // 解析Hadoop通用命令行参数
    GenericOptionsParser genericParser 
      = new GenericOptionsParser(conf, args);
    args = genericParser.getRemainingArgs();
    
    // 参数不足时输出用法并退出
    if (args.length < 2) {
      System.out.println("usage: inputDirs outDir "
          + "[numOfReducer [textinputformat|seq [specfile [jobName]]]]");
      GenericOptionsParser.printGenericCommandUsage(System.out);
      System.exit(2);
    }
    // 解析输入目录
    String inputDir = args[0];
    // 解析输出目录
    String outputDir = args[1];
    // 默认Reduce任务数为1
    int numOfReducers = 1;
    if (args.length > 2) {
      numOfReducers = Integer.parseInt(args[2]);
    }

    // 解析输入格式类型，默认使用SequenceFileInputFormat
    Class<? extends InputFormat> theInputFormat = null;
    if (args.length > 3 && 
        args[3].compareToIgnoreCase("textinputformat") == 0) {
      theInputFormat = TextInputFormat.class;
    } else {
      theInputFormat = SequenceFileInputFormat.class;
    }

    Path specFile = null;
    // 解析配置文件路径
    if (args.length > 4) {
      specFile = new Path(args[4]);
    }

    String jobName = "";
    // 解析作业名称
    if (args.length > 5) {
      jobName = args[5];
    }

    // 如果指定了额外配置文件，添加到配置中
    if (specFile != null) {
      conf.addResource(specFile);
    }
    // 获取用户自定义Jar路径
    String userJarFile = conf.get(ValueAggregatorJobBase.USER_JAR);
    if (userJarFile != null) {
      // 设置作业运行的Jar包
      conf.set(MRJobConfig.JAR, userJarFile);
    }

    // 创建作业实例
    Job theJob = Job.getInstance(conf);
    if (userJarFile == null) {
      // 未指定Jar时，使用当前ValueAggregator类所在Jar
      theJob.setJarByClass(ValueAggregator.class);
    } 
    // 设置作业名称
    theJob.setJobName("ValueAggregatorJob: " + jobName);
    // 添加输入路径
    FileInputFormat.addInputPaths(theJob, inputDir);
    // 设置输入格式类
    theJob.setInputFormatClass(theInputFormat);
    // 设置Mapper类为框架内置的ValueAggregatorMapper
    theJob.setMapperClass(ValueAggregatorMapper.class);
    // 设置输出路径
    FileOutputFormat.setOutputPath(theJob, new Path(outputDir));
    // 设置输出格式为TextOutputFormat
    theJob.setOutputFormatClass(TextOutputFormat.class);
    // 设置Map输出键值类型
    theJob.setMapOutputKeyClass(Text.class);
    theJob.setMapOutputValueClass(Text.class);
    // 设置最终输出键值类型
    theJob.setOutputKeyClass(Text.class);
    theJob.setOutputValueClass(Text.class);
    // 设置Reducer类为框架内置的ValueAggregatorReducer
    theJob.setReducerClass(ValueAggregatorReducer.class);
    // 设置Combiner类为框架内置的ValueAggregatorCombiner
    theJob.setCombinerClass(ValueAggregatorCombiner.class);
    // 设置Reduce任务数量
    theJob.setNumReduceTasks(numOfReducers);
    return theJob;
  }

  /**
   * 创建ValueAggregator聚合作业，指定自定义聚合描述符
   * @param args 命令行参数
   * @param descriptors 自定义聚合描述符数组
   * @return 配置完成可提交的Job对象
   * @throws IOException 创建过程中IO异常
   */
  public static Job createValueAggregatorJob(String args[], 
      Class<? extends ValueAggregatorDescriptor>[] descriptors) 
      throws IOException {
    return createValueAggregatorJob(
             setAggregatorDescriptors(descriptors), args);
  }
  
  /**
   * 将自定义聚合描述符数组注册到配置中
   * @param descriptors 自定义聚合描述符数组
   * @return 配置了描述符信息的Configuration对象
   */
  public static Configuration setAggregatorDescriptors(
      Class<? extends ValueAggregatorDescriptor>[] descriptors) {
    Configuration conf = new Configuration();
    // 设置描述符数量
    conf.setInt(ValueAggregatorJobBase.DESCRIPTOR_NUM, descriptors.length);
    // 逐个记录描述符类名到配置
    for(int i=0; i< descriptors.length; i++) {
      conf.set(ValueAggregatorJobBase.DESCRIPTOR + "." + i,
               "UserDefined," + descriptors[i].getName());
    }
    return conf;
  }
  
  /**
   * 主方法，从命令行创建并提交运行聚合作业
   * @param args 命令行参数
   * @throws IOException 作业创建/运行IO异常
   * @throws InterruptedException 作业运行被中断异常
   * @throws ClassNotFoundException 找不到类异常
   */
  public static void main(String args[]) 
      throws IOException, InterruptedException, ClassNotFoundException {
    Job job = ValueAggregatorJob.createValueAggregatorJob(
                new Configuration(), args);
    // 提交作业并等待完成，根据结果设置退出码
    int ret = job.waitForCompletion(true) ? 0 : 1;
    System.exit(ret);
  }
}