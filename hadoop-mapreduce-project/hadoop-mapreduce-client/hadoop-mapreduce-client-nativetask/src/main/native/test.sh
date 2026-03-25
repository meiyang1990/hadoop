# 这个文件已经全部加上中文注释
#!/bin/sh
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

# Hadoop MapReduce 本地任务C++代码单元测试启动脚本
# 功能：根据参数筛选测试类型，配置动态库路径，启动Google Test测试

# 默认仅执行功能测试，排除性能测试
FILTER="--gtest_filter=-Perf.*"

# 参数为all时，执行所有测试（包含性能测试）
if [ "$1" = "all" ]; then
  shift
  FILTER=""
fi

# 参数为perf时，仅执行性能测试
if [ "$1" = "perf" ]; then
  shift
  FILTER="--gtest_filter=Perf.*"
fi

# macOS系统已经配置好了RPATH，不需要额外设置动态库搜索路径
if [ "${SYSTEM_MAC}" = "TRUE" ]; then
  ./nttest $FILTER $@
else
  # 提取JVM动态库所在目录
  JAVA_JVM_LIBRARY_DIR=`dirname ${JAVA_JVM_LIBRARY}`
  # 添加JVM动态库目录到动态库搜索路径，启动测试程序
  LD_LIBRARY_PATH=$JAVA_JVM_LIBRARY_DIR:$LD_LIBRARY_PATH ./nttest $FILTER $@
fi