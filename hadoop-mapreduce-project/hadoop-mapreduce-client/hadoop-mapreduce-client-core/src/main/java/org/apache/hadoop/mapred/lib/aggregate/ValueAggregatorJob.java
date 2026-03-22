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

package org.apache.hadoop.mapred.lib.aggregate;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 文件级注释：基于聚合框架的MapReduce作业工厂类，为简单聚合类统计任务提供快速作业构建能力
 * 
 * 本类是MapReduce聚合框架的主入口类，聚合框架是MapReduce的一个特殊化实现，专门用于执行各类简单聚合计算任务。
 * 对于常见的计数、统计类应用，开发者无需自行实现完整的Map和Reduce函数，只需提供自定义的聚合描述符即可快速生成可运行的MapReduce作业。
 * 框架内置提供了多种常用聚合器：求和、去重计数、直方图、最大/最小值、中位数、平均值、标准差等。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ValueAggregatorJob {

  /**
   * 创建包含ValueAggregator作业的JobControl容器，用于管理作业生命周期
   * @param args 命令行参数，包含输入路径、输出路径等作业配置参数
   * @param descriptors 用户自定义的聚合描述符数组
   * @return 包含已配置作业的JobControl对象
   * @throws IOException 创建作业过程中发生IO异常时抛出
   */
  public static JobControl createValueAggregatorJobs(String args[]
    , Class<? extends ValueAggregatorDescriptor>[] descriptors) throws IOException {
    
    JobControl theControl = new JobControl("ValueAggregatorJobs");
    ArrayList<Job> dependingJobs = new ArrayList<Job>();
    JobConf aJobConf = createValueAggregatorJob(args);
    if(descriptors != null)
      setAggregatorDescriptors(aJobConf, descriptors);
    Job aJob = new Job(aJobConf, dependingJobs);
    theControl.addJob(aJob);
    return theControl;
  }

  /**
   * 使用默认描述符创建包含ValueAggregator作业的JobControl容器
   * @param args 命令行参数
   * @return 包含已配置作业的JobControl对象
   * @throws IOException 创建作业过程中发生IO异常时抛出
   */
  public static JobControl createValueAggregatorJobs(String args[]) throws IOException {
    return createValueAggregatorJobs(args, null);
  }
  
  /**
   * 创建基于聚合框架的MapReduce作业配置
   *
   * @param args 作业创建参数，支持Hadoop通用命令行参数
   * @param caller 调用方类，用于定位作业Jar包
   * @return 配置完成可提交的JobConf对象
   *
   * @throws IOException 解析参数、加载配置时发生IO异常时抛出
   * @see GenericOptionsParser
   */
  @SuppressWarnings("rawtypes")
  public static JobConf createValueAggregatorJob(String args[], Class<?> caller)
    throws IOException {

    Configuration conf = new Configuration();
    
    GenericOptionsParser genericParser 
      = new GenericOptionsParser(conf, args);
    // 获取解析通用参数后的剩余业务参数
    args = genericParser.getRemainingArgs();
    
    // 参数个数检查，不足则输出用法并退出
    if (args.length < 2) {
      System.out.println("usage: inputDirs outDir "
          + "[numOfReducer [textinputformat|seq [specfile [jobName]]]]");
      GenericOptionsParser.printGenericCommandUsage(System.out);
      System.exit(1);
    }
    String inputDir = args[0];
    String outputDir = args[1];
    int numOfReducers = 1;
    if (args.length > 2) {
      numOfReducers = Integer.parseInt(args[2]);
    }

    // 默认使用TextInputFormat
    Class<? extends InputFormat> theInputFormat =
      TextInputFormat.class;
    if (args.length > 3 && 
        args[3].compareToIgnoreCase("textinputformat") == 0) {
      theInputFormat = TextInputFormat.class;
    } else {
      theInputFormat = SequenceFileInputFormat.class;
    }

    Path specFile = null;

    // 如果指定了额外配置文件，则保存配置文件路径
    if (args.length > 4) {
      specFile = new Path(args[4]);
    }

    String jobName = "";
    
    // 如果指定了作业名称，则保存作业名称
    if (args.length > 5) {
      jobName = args[5];
    }
    
    JobConf theJob = new JobConf(conf);
    // 添加额外配置文件到作业配置
    if (specFile != null) {
      theJob.addResource(specFile);
    }
    String userJarFile = theJob.get("user.jar.file");
    // 设置作业Jar包位置
    if (userJarFile == null) {
      theJob.setJarByClass(caller != null ? caller : ValueAggregatorJob.class);
    } else {
      theJob.setJar(userJarFile);
    }
    theJob.setJobName("ValueAggregatorJob: " + jobName);

    // 设置输入路径
    FileInputFormat.addInputPaths(theJob, inputDir);

    // 设置输入格式类
    theJob.setInputFormat(theInputFormat);
    
    // 设置聚合框架默认Mapper类
    theJob.setMapperClass(ValueAggregatorMapper.class);
    // 设置输出路径
    FileOutputFormat.setOutputPath(theJob, new Path(outputDir));
    // 设置输出格式类为TextOutputFormat
    theJob.setOutputFormat(TextOutputFormat.class);
    // 设置Map输出Key/Value类型
    theJob.setMapOutputKeyClass(Text.class);
    theJob.setMapOutputValueClass(Text.class);
    // 设置最终输出Key/Value类型
    theJob.setOutputKeyClass(Text.class);
    theJob.setOutputValueClass(Text.class);
    // 设置聚合框架默认Reducer和Combiner类
    theJob.setReducerClass(ValueAggregatorReducer.class);
    theJob.setCombinerClass(ValueAggregatorCombiner.class);
    // 设置Map任务数和Reduce任务数
    theJob.setNumMapTasks(1);
    theJob.setNumReduceTasks(numOfReducers);
    return theJob;
  }

  /**
   * 创建基于聚合框架的MapReduce作业配置，使用默认调用类
   * 
   * @param args 作业创建参数，支持Hadoop通用命令行参数
   * @return 配置完成可提交的JobConf对象
   * 
   * @throws IOException 解析参数、加载配置时发生IO异常时抛出
   * @see GenericOptionsParser
   */
  public static JobConf createValueAggregatorJob(String args[])
    throws IOException {
    return createValueAggregatorJob(args, ValueAggregator.class);
  }

  /**
   * 创建基于聚合框架的MapReduce作业配置，并设置用户自定义聚合描述符
   * @param args 作业创建参数
   * @param descriptors 用户自定义聚合描述符数组
   * @return 配置完成可提交的JobConf对象
   * @throws IOException 解析参数、加载配置时发生IO异常时抛出
   */
  public static JobConf createValueAggregatorJob(String args[]
    , Class<? extends ValueAggregatorDescriptor>[] descriptors)
  throws IOException {
    JobConf job = createValueAggregatorJob(args);
    setAggregatorDescriptors(job, descriptors);
    return job;
  }
  
  /**
   * 将用户自定义聚合描述符配置写入JobConf
   * @param job 目标作业配置对象
   * @param descriptors 用户自定义聚合描述符数组
   */
  public static void setAggregatorDescriptors(JobConf job
      , Class<? extends ValueAggregatorDescriptor>[] descriptors) {
    // 写入描述符数量
    job.setInt("aggregator.descriptor.num", descriptors.length);
    // 遍历写入每个描述符的类名信息
    for(int i=0; i< descriptors.length; i++) {
      job.set("aggregator.descriptor." + i, "UserDefined," + descriptors[i].getName());
    }    
  }

  /**
   * 创建基于聚合框架的MapReduce作业配置，指定调用类和自定义聚合描述符
   * @param args 作业创建参数
   * @param descriptors 用户自定义聚合描述符数组
   * @param caller 调用方类，用于定位作业Jar包
   * @return 配置完成可提交的JobConf对象
   * @throws IOException 解析参数、加载配置时发生IO异常时抛出
   */
  public static JobConf createValueAggregatorJob(String args[],
      Class<? extends ValueAggregatorDescriptor>[] descriptors,
      Class<?> caller) throws IOException {
    JobConf job = createValueAggregatorJob(args, caller);
    setAggregatorDescriptors(job, descriptors);
    return job;
  }

  /**
   * 命令行入口方法，创建并运行基于聚合框架的MapReduce作业
   * 
   * @param args 命令行输入的作业参数
   * @throws IOException 作业创建或运行过程中发生IO异常时抛出
   */
  public static void main(String args[]) throws IOException {
    JobConf job = ValueAggregatorJob.createValueAggregatorJob(args);
    JobClient.runJob(job);
  }
}