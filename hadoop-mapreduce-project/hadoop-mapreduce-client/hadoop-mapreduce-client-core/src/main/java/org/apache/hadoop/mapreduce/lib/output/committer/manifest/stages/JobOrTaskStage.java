// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages;

import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.util.functional.FunctionRaisingIOE;

/**
 * 文件输出提交器清单提交流程的任务/作业阶段接口，定义阶段的通用能力。
 * 阶段是一个可抛出IO异常的函数，同时支持提供IO统计信息。
 * @param <IN> 阶段输入参数类型
 * @param <OUT> 阶段输出结果类型
 */
public interface JobOrTaskStage<IN, OUT> extends FunctionRaisingIOE<IN, OUT>,
    IOStatisticsSource {

}