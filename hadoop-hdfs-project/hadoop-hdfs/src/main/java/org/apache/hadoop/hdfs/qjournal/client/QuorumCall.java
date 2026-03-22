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
package org.apache.hadoop.hdfs.qjournal.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.Timer;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.FutureCallback;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.protobuf.Message;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;


/**
 * HDFS QJM（日志仲裁）模式下，代表一组需要获取多数节点响应的远程调用集合
 * 负责管理所有节点的调用结果、异常，并等待满足法定人数要求的响应
 * @param <KEY> 用于标识每个远程调用的键类型，对应不同日志节点
 * @param <RESULT> 远程调用返回结果的类型
 */
class QuorumCall<KEY, RESULT> {
  private final Map<KEY, RESULT> successes = Maps.newHashMap();
  private final Map<KEY, Throwable> exceptions = Maps.newHashMap();

  /**
   * 等待法定响应过程中，打印进度日志的时间间隔，单位毫秒
   */
  private static final int WAIT_PROGRESS_INTERVAL_MILLIS = 1000;
  
  /**
   * 等待超过配置超时时间的该比例后，开始按INFO级别周期性打印进度日志
   */
  private static final float WAIT_PROGRESS_INFO_THRESHOLD = 0.3f;
  /**
   * 等待超过配置超时时间的该比例后，开始按WARN级别打印进度日志
   */
  private static final float WAIT_PROGRESS_WARN_THRESHOLD = 0.7f;
  private final StopWatch quorumStopWatch;
  private final Timer timer;
  private final List<ListenableFuture<RESULT>> allCalls;
  
  /**
   * 工厂方法，创建QuorumCall实例并为所有调用添加回调处理
   * @param calls 所有节点的异步调用Future映射
   * @param timer 计时器，用于统计等待时间
   * @return 创建完成的QuorumCall实例
   */
  static <KEY, RESULT> QuorumCall<KEY, RESULT> create(
      Map<KEY, ? extends ListenableFuture<RESULT>> calls, Timer timer) {
    final QuorumCall<KEY, RESULT> qr = new QuorumCall<KEY, RESULT>(timer);
    for (final Entry<KEY, ? extends ListenableFuture<RESULT>> e : calls.entrySet()) {
      Preconditions.checkArgument(e.getValue() != null,
          "null future for key: " + e.getKey());
      qr.addCall(e.getValue());
      Futures.addCallback(e.getValue(), new FutureCallback<RESULT>() {
        @Override
        public void onFailure(Throwable t) {
          qr.addException(e.getKey(), t);
        }

        @Override
        public void onSuccess(RESULT res) {
          qr.addResult(e.getKey(), res);
        }
      }, MoreExecutors.directExecutor());
    }
    return qr;
  }

  /**
   * 工厂方法，使用默认计时器创建QuorumCall实例
   * @param calls 所有节点的异步调用Future映射
   * @return 创建完成的QuorumCall实例
   */
  static <KEY, RESULT> QuorumCall<KEY, RESULT> create(
      Map<KEY, ? extends ListenableFuture<RESULT>> calls) {
    return create(calls, new Timer());
  }

  /**
   * 不允许外部直接使用的私有构造
   */
  private QuorumCall() {
    this(new Timer());
  }

  private QuorumCall(Timer timer) {
    // Only instantiated from factory method above
    this.timer = timer;
    this.quorumStopWatch = new StopWatch(timer);
    this.allCalls = new ArrayList<>();
  }

  private void addCall(ListenableFuture<RESULT> call) {
    allCalls.add(call);
  }

  /**
   * 重启法定人数调用的计时器，用于检测系统暂停（如Full GC）
   */
  private void restartQuorumStopWatch() {
    quorumStopWatch.reset().start();
  }

  /**
   * 检测自上次重启计时器后是否发生系统暂停（如Full GC），如果发生则返回暂停时长用于调整超时
   * @param offset 对已用时间的偏移量，用于处理预期内的暂停
   * @param millis 配置的总超时时间，单位毫秒
   * @return 如果检测到暂停返回暂停时长，否则返回-1
   */
  private long getQuorumTimeoutIncreaseMillis(long offset, int millis) {
    long elapsed = quorumStopWatch.now(TimeUnit.MILLISECONDS);
    long pauseTime = elapsed + offset;
    if (pauseTime > (millis * WAIT_PROGRESS_INFO_THRESHOLD)) {
      QuorumJournalManager.LOG.info("Pause detected while waiting for " +
          "QuorumCall response; increasing timeout threshold by pause time " +
          "of " + pauseTime + " ms.");
      return pauseTime;
    } else {
      return -1;
    }
  }

  
  /**
   * 等待满足法定人数的响应条件，阻塞直到条件满足或超时
   * 
   * 注意：方法返回后仍可能有后续响应到达，会改变本类其他方法的返回结果
   *
   * @param minResponses 只要收到至少该数量的响应（不管成功失败）就返回
   * @param minSuccesses 只要收到至少该数量的成功响应就返回
   * @param maxExceptions 只要收到超过该数量的异常响应就返回；传0表示收到任何异常立即返回
   * @param millis 最大等待超时时间，单位毫秒
   * @param operationName 当前操作名称，用于日志打印
   * @throws InterruptedException 等待过程中被中断抛出
   * @throws TimeoutException 超时仍未满足条件抛出
   */
  public synchronized void waitFor(
      int minResponses, int minSuccesses, int maxExceptions,
      int millis, String operationName)
      throws InterruptedException, TimeoutException {
    // 记录开始等待时间
    long st = timer.monotonicNow();
    // 下一次打印日志的时间点
    long nextLogTime = st + (long)(millis * WAIT_PROGRESS_INFO_THRESHOLD);
    // 超时截止时间
    long et = st + millis;
    // 循环等待直到满足条件或超时
    while (true) {
      restartQuorumStopWatch();
      // 检查是否有断言错误，有则立即抛出
      checkAssertionErrors();
      // 检查是否满足退出条件，满足则直接返回
      if (minResponses > 0 && countResponses() >= minResponses) return;
      if (minSuccesses > 0 && countSuccesses() >= minSuccesses) return;
      if (maxExceptions >= 0 && countExceptions() > maxExceptions) return;
      // 获取当前时间
      long now = timer.monotonicNow();
      
      // 到达日志打印时间点，打印当前等待进度
      if (now > nextLogTime) {
        long waited = now - st;
        String msg = String.format(
            "Waited %s ms (timeout=%s ms) for a response for %s",
            waited, millis, operationName);
        // 添加已成功节点信息
        if (!successes.isEmpty()) {
          msg += ". Succeeded so far: [" + Joiner.on(",").join(successes.keySet()) + "]";
        }
        // 添加已异常节点信息
        if (!exceptions.isEmpty()) {
          msg += ". Exceptions so far: [" + getExceptionMapString() + "]";
        }
        // 还没有任何响应的提示
        if (successes.isEmpty() && exceptions.isEmpty()) {
          msg += ". No responses yet.";
        }
        // 根据等待时长选择日志级别
        if (waited > millis * WAIT_PROGRESS_WARN_THRESHOLD) {
          QuorumJournalManager.LOG.warn(msg);
        } else {
          QuorumJournalManager.LOG.info(msg);
        }
        // 更新下一次打印日志的时间点
        nextLogTime = now + WAIT_PROGRESS_INTERVAL_MILLIS;
      }
      // 计算剩余等待时间
      long rem = et - now;
      if (rem <= 0) {
        // 如果发生了GC暂停，则增加超时时间，否则抛出超时异常
        long timeoutIncrease = getQuorumTimeoutIncreaseMillis(0, millis);
        if (timeoutIncrease > 0) {
          et += timeoutIncrease;
        } else {
          throw new TimeoutException();
        }
      }
      restartQuorumStopWatch();
      // 计算本次等待时长，不超过下一次日志打印时间，至少等待1ms
      rem = Math.min(rem, nextLogTime - now);
      rem = Math.max(rem, 1);
      // 等待，释放锁让其他线程更新结果
      wait(rem);
      // 检查等待过程中是否发生GC暂停，发生则增加超时时间
      long timeoutIncrease = getQuorumTimeoutIncreaseMillis(-rem, millis);
      if (timeoutIncrease > 0) {
        et += timeoutIncrease;
      }
    }
  }

  /**
   * 取消所有未完成的远程调用
   */
  void cancelCalls() {
    for (ListenableFuture<RESULT> call : allCalls) {
      call.cancel(true);
    }
  }

  /**
   * 检查异常中是否包含AssertionError，如果存在则直接重新抛出
   * 仅在断言开启时执行，目的是让测试用例可以快速失败，而不是忽略断言错误继续执行
   */
  private synchronized void checkAssertionErrors() {
    boolean assertsEnabled = false;
    assert assertsEnabled = true; // sets to true if enabled
    if (assertsEnabled) {
      for (Throwable t : exceptions.values()) {
        if (t instanceof AssertionError) {
          throw (AssertionError)t;
        } else if (t instanceof RemoteException &&
            ((RemoteException)t).getClassName().equals(
                AssertionError.class.getName())) {
          throw new AssertionError(t);
        }
      }
    }
  }

  /**
   * 添加成功调用的结果，通知等待线程
   */
  private synchronized void addResult(KEY k, RESULT res) {
    successes.put(k, res);
    notifyAll();
  }
  
  /**
   * 添加调用异常，通知等待线程
   */
  private synchronized void addException(KEY k, Throwable t) {
    exceptions.put(k, t);
    notifyAll();
  }
  
  /**
   * @return 总共收到的响应数量，无论成功还是失败
   */
  public synchronized int countResponses() {
    return successes.size() + exceptions.size();
  }
  
  /**
   * @return 收到的成功响应数量
   */
  public synchronized int countSuccesses() {
    return successes.size();
  }
  
  /**
   * @return 收到的异常响应数量
   */
  public synchronized int countExceptions() {
    return successes.size() > 0 ? exceptions.size() : exceptions.size();
  }

  /**
   * @return 所有成功响应的拷贝，后续新结果不会影响返回的映射
   */
  public synchronized Map<KEY, RESULT> getResults() {
    return Maps.newHashMap(successes);
  }

  /**
   * 将收集到的异常封装为QuorumException并抛出
   * @param msg 异常描述信息
   * @throws QuorumException 封装后的仲裁异常
   */
  public synchronized void rethrowException(String msg) throws QuorumException {
    Preconditions.checkState(!exceptions.isEmpty());
    throw QuorumException.create(msg, successes, exceptions);
  }

  /**
   * 将Protobuf消息映射转换为可打印的字符串，用于日志显示
   * @param map 键到Protobuf消息的映射
   * @return 格式化后的字符串
   */
  public static <K> String mapToString(
      Map<K, ? extends Message> map) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<K, ? extends Message> e : map.entrySet()) {
      if (!first) {
        sb.append("\n");
      }
      first = false;
      sb.append(e.getKey()).append(": ")
        .append(TextFormat.shortDebugString(e.getValue()));
    }
    return sb.toString();
  }

  /**
   * 获取已接收异常的拼接字符串，用于日志显示
   * @return 异常信息拼接后的字符串
   */
  private String getExceptionMapString() {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<KEY, Throwable> e : exceptions.entrySet()) {
      if (!first) {
        sb.append(", ");
      }
      first = false;
      sb.append(e.getKey()).append(": ")
        .append(e.getValue().getLocalizedMessage());
    }
    return sb.toString();
  }
}