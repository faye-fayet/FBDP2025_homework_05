# 作业5 - 股票新闻情感分析MapReduce程序设计报告

姓名：滕子鉴

学号：231275015

----

## 一、设计思路

### 1.1 问题分析

本作业需要对股票财经新闻数据集进行情感分类的词频统计，具体要求：

- 分别统计正面新闻（sentiment=1）和负面新闻（sentiment=-1）中的高频词
- 输出每类新闻的TOP 100高频词
- 需要进行文本预处理：忽略大小写、标点符号、数字、停用词

### 1.2 MapReduce设计方案

#### 伪代码

##### 一、主函数流程

```
FUNCTION main(args):
    // 1. 检查参数
    IF args.length < 3 THEN
        输出使用说明
        退出程序
    END IF
    
    // 2. 创建配置和作业
    创建 Hadoop 配置对象 conf
    创建作业 job，命名为 "Stock Word Count"
    
    // 3. 设置作业参数
    设置 jar 类为 StockWordCount.class
    设置 Mapper 类为 TokenizerMapper.class
    设置 Reducer 类为 IntSumReducer.class
    设置输出键类型为 Text
    设置输出值类型为 IntWritable
    
    // 4. 配置输入输出路径
    设置输入路径为 args[0]  // CSV文件路径
    添加分布式缓存文件 args[1]  // 停词文件
    设置输出路径为 args[2]  // 输出目录
    
    // 5. 提交作业并等待完成
    提交作业并等待完成
    根据完成状态退出程序
END FUNCTION
```

------

##### 二、Mapper 阶段

###### 2.1 初始化阶段

```
FUNCTION setup(context):
    // 加载停词表
    创建空的停词集合 stopWords (HashSet)
    
    TRY:
        // 从分布式缓存读取停词文件
        获取缓存文件列表 cacheFiles
        
        IF cacheFiles 不为空 THEN
            获取文件系统 fs
            打开停词文件路径
            创建 BufferedReader 读取文件
            
            // 逐行读取停词并加入集合
            WHILE 有下一行 line DO
                去除首尾空格并转为小写
                将 line 添加到 stopWords
            END WHILE
            
            关闭文件读取器
        END IF
        
    CATCH IOException e:
        输出错误信息
    END TRY
END FUNCTION
```

###### 2.2 映射阶段（map）

```
FUNCTION map(key, value, context):
    // 1. 跳过 CSV 表头
    IF key == 0 THEN
        RETURN  // 第一行是表头，跳过
    END IF
    
    // 2. 解析 CSV 行
    line = value.toString()
    找到最后一个逗号的位置 lastCommaIndex
    
    IF lastCommaIndex == -1 THEN
        RETURN  // 格式错误，跳过
    END IF
    
    // 3. 提取文本和情感标签
    text = line.substring(0, lastCommaIndex).trim()
    sentimentStr = line.substring(lastCommaIndex + 1).trim()
    
    // 4. 去除文本两端的引号
    IF text 以引号开始和结束 THEN
        text = 去除首尾引号
    END IF
    
    // 5. 解析情感标签
    TRY:
        sentiment = 将 sentimentStr 转为整数
    CATCH NumberFormatException:
        RETURN  // 解析失败，跳过
    END TRY
    
    // 6. 只处理正面(1)和负面(-1)新闻
    IF sentiment != 1 AND sentiment != -1 THEN
        RETURN
    END IF
    
    // 7. 文本预处理
    text = 转为小写
    text = 移除所有非字母字符（替换为空格）
    text = 将多个空格替换为单个空格并去除首尾空格
    
    // 8. 分词并输出
    words = 按空格分割 text
    
    FOR EACH word IN words DO
        word = 去除首尾空格
        
        // 过滤条件：非空、不在停词表、不是纯数字
        IF word 非空 AND word 不在 stopWords AND word 不是数字 THEN
            // 创建复合键：情感_单词
            compositeKey = sentiment + "_" + word
            输出 <compositeKey, 1> 到 context
        END IF
    END FOR
END FUNCTION
```

------

##### 三、Reducer 阶段

###### 3.1 聚合阶段（reduce）

```
FUNCTION reduce(key, values, context):
    // 1. 解析复合键
    keyStr = key.toString()
    parts = 按 "_" 分割 keyStr（最多分割成2部分）
    
    IF parts.length != 2 THEN
        RETURN  // 格式错误，跳过
    END IF
    
    sentiment = 将 parts[0] 转为整数
    word = parts[1]
    
    // 2. 累加词频
    sum = 0
    FOR EACH val IN values DO
        sum = sum + val.get()
    END FOR
    
    // 3. 根据情感标签分类存储
    IF sentiment == 1 THEN
        将 <word, sum> 存入 positiveWordCount 映射
    ELSE IF sentiment == -1 THEN
        将 <word, sum> 存入 negativeWordCount 映射
    END IF
END FUNCTION
```

###### 3.2 清理阶段（cleanup）

```
FUNCTION cleanup(context):
    // 1. 输出正面新闻词频 TOP 100
    输出标题 "========== POSITIVE NEWS TOP 100 WORDS =========="
    
    // 按词频降序排序
    positiveList = positiveWordCount 转为列表
    按 value 降序排序 positiveList
    
    count = 0
    FOR EACH entry IN positiveList DO
        IF count >= 100 THEN
            BREAK
        END IF
        输出 <entry.key, entry.value>
        count = count + 1
    END FOR
    
    // 2. 输出空行分隔
    输出空行
    
    // 3. 输出负面新闻词频 TOP 100
    输出标题 "========== NEGATIVE NEWS TOP 100 WORDS =========="
    
    // 按词频降序排序
    negativeList = negativeWordCount 转为列表
    按 value 降序排序 negativeList
    
    count = 0
    FOR EACH entry IN negativeList DO
        IF count >= 100 THEN
            BREAK
        END IF
        输出 <entry.key, entry.value>
        count = count + 1
    END FOR
END FUNCTION
```

------

##### 四、数据流示例

```
输入CSV行：
"Apple stock soars to new heights, investors optimistic",1

↓ Mapper处理 ↓

1. 解析：text = "Apple stock soars to new heights, investors optimistic"
        sentiment = 1
2. 预处理：text = "apple stock soars to new heights investors optimistic"
3. 分词：["apple", "stock", "soars", "to", "new", "heights", "investors", "optimistic"]
4. 过滤停词：["apple", "stock", "soars", "heights", "investors", "optimistic"]
5. 输出键值对：
   <"1_apple", 1>
   <"1_stock", 1>
   <"1_soars", 1>
   <"1_heights", 1>
   <"1_investors", 1>
   <"1_optimistic", 1>

↓ Shuffle & Sort ↓

相同键的值聚合在一起

↓ Reducer处理 ↓

对每个单词累加出现次数，分类存储到正面/负面词频映射

↓ Cleanup输出 ↓

按词频降序输出TOP 100单词
```

----



#### 编写java代码

##### **Map阶段设计**

Map阶段的核心任务是读取CSV数据并进行文本预处理：

```java
Map(key: LongWritable, value: Text):
    // 跳过CSV头部
    if key == 0:
        return
    
    // 解析CSV行
    line = value.toString()
    lastCommaIndex = line.lastIndexOf(',')
    text = line.substring(0, lastCommaIndex)
    sentimentStr = line.substring(lastCommaIndex + 1)
    
    // 提取情感标签
    sentiment = parseInt(sentimentStr)
    if sentiment != 1 AND sentiment != -1:
        return
    
    // 文本预处理
    text = text.toLowerCase()  // 转小写
    text = removeQuotes(text)   // 移除引号
    text = text.replaceAll("[^a-z\\s]", " ")  // 移除标点和数字
    text = text.trim()
    
    // 分词并输出
    words = text.split("\\s+")
    for each word in words:
        if word not empty AND word not in stopWords AND word not numeric:
            compositeKey = sentiment + "_" + word
            emit(compositeKey, 1)
```

**关键设计点：**

1. **复合键设计**：使用"sentiment_word"作为key，将情感标签和单词组合，便于后续按情感分类统计
2. **停用词过滤**：在setup()阶段通过分布式缓存加载停用词表
3. **文本清洗**：正则表达式`[^a-z\\s]`保留字母和空格，去除所有标点和数字

##### **Reduce阶段设计**

Reduce阶段负责词频统计和TOP 100筛选：

```java
Reduce(key: Text, values: Iterable<IntWritable>):
    // 解析复合键
    parts = key.split("_", 2)
    sentiment = parseInt(parts[0])
    word = parts[1]
    
    // 统计词频
    sum = 0
    for value in values:
        sum += value.get()
    
    // 分类存储
    if sentiment == 1:
        positiveWordCount.put(word, sum)
    else if sentiment == -1:
        negativeWordCount.put(word, sum)

cleanup():
    // 输出正面新闻TOP 100
    emit("========== POSITIVE NEWS TOP 100 WORDS ==========", 0)
    sortedPositive = sort(positiveWordCount by value DESC)
    for i = 0 to min(100, sortedPositive.size):
        emit(word, count)
    
    // 输出负面新闻TOP 100
    emit("========== NEGATIVE NEWS TOP 100 WORDS ==========", 0)
    sortedNegative = sort(negativeWordCount by value DESC)
    for i = 0 to min(100, sortedNegative.size):
        emit(word, count)
```

**关键设计点：**

1. **内存聚合**：在Reducer中使用HashMap存储所有词频，避免多次排序
2. **cleanup()输出**：在cleanup阶段进行排序和TOP K筛选，保证输出有序
3. **分类输出**：通过分隔符清晰区分正面和负面新闻的统计结果

### 1.3 技术架构

- **Hadoop版本**：3.4.0
- **Java版本**：JDK 8
- **构建工具**：Maven
- **依赖管理**：通过pom.xml统一管理Hadoop依赖

------

## 二、程序执行流程

### 2.1 准备阶段

```bash
# 1. 创建HDFS输入目录
hdfs dfs -mkdir -p /stock/input

# 2. 上传数据文件
hdfs dfs -put stock_data.csv /stock/input/
hdfs dfs -put stop-word-list.txt /stock/input/

# 3. 清理旧的输出目录（如果存在）
hdfs dfs -rm -r /stock/output
```

### 2.2 编译打包

```bash
# Maven编译打包
mvn clean package

# 生成jar文件
# target/stock-word-count-1.0-SNAPSHOT.jar
```

### 2.3 提交任务

```bash
hadoop jar target/stock-word-count-1.0-SNAPSHOT.jar \
  com.hadoop.stock.StockWordCount \
  /stock/input/stock_data.csv \
  /stock/input/stop-word-list.txt \
  /stock/output
```

### 2.4 查看结果

```bash
# 查看输出文件
hdfs dfs -ls /stock/output

# 查看统计结果
hdfs dfs -cat /stock/output/part-r-00000
```

------







## 三、程序运行结果说明


<img width="1293" height="779" alt="image" src="https://github.com/user-attachments/assets/3443204d-8316-4ff7-a9cd-9482b19a9a98" />
<img width="936" height="300" alt="image" src="https://github.com/user-attachments/assets/488139fe-d544-43a0-9013-dae15094e871" />



执行命令和终端输出：

```bash
bin/hadoop jar ~/Desktop/bigDataProcess/stock-word-count/target/stock-word-count-1.0-SNAPSHOT.jar \    /stock/input/stock_data.csv \    /stock/input/stop-word-list.txt \    /stock/output
Input CSV: /stock/input/stock_data.csv
Stopwords file: /stock/input/stop-word-list.txt
Output directory: /stock/output
2025-10-19 15:53:02,102 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at /0.0.0.0:8032
2025-10-19 15:53:03,346 WARN mapreduce.JobResourceUploader: Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy th
is.                                                                                                                                                                                                 2025-10-19 15:53:03,379 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/faye/.staging/job_1760860360156_0001
2025-10-19 15:53:03,804 INFO input.FileInputFormat: Total input files to process : 1
2025-10-19 15:53:03,932 INFO mapreduce.JobSubmitter: number of splits:1
2025-10-19 15:53:04,579 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1760860360156_0001
2025-10-19 15:53:04,579 INFO mapreduce.JobSubmitter: Executing with tokens: []
2025-10-19 15:53:05,272 INFO conf.Configuration: resource-types.xml not found
2025-10-19 15:53:05,274 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2025-10-19 15:53:06,671 INFO impl.YarnClientImpl: Submitted application application_1760860360156_0001
2025-10-19 15:53:06,825 INFO mapreduce.Job: The url to track the job: http://faye-virtual-machine:8088/proxy/application_1760860360156_0001/
2025-10-19 15:53:06,827 INFO mapreduce.Job: Running job: job_1760860360156_0001
2025-10-19 15:53:20,133 INFO mapreduce.Job: Job job_1760860360156_0001 running in uber mode : false
2025-10-19 15:53:20,135 INFO mapreduce.Job:  map 0% reduce 0%
2025-10-19 15:53:26,354 INFO mapreduce.Job:  map 100% reduce 0%
2025-10-19 15:53:31,388 INFO mapreduce.Job:  map 100% reduce 100%
2025-10-19 15:53:32,403 INFO mapreduce.Job: Job job_1760860360156_0001 completed successfully
2025-10-19 15:53:32,542 INFO mapreduce.Job: Counters: 54
        File System Counters
                FILE: Number of bytes read=687464
                FILE: Number of bytes written=1993579
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=482312
                HDFS: Number of bytes written=1821
                HDFS: Number of read operations=9
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
                HDFS: Number of bytes read erasure-coded=0
        Job Counters 
                Launched map tasks=1
                Launched reduce tasks=1
                Data-local map tasks=1
                Total time spent by all maps in occupied slots (ms)=3450
                Total time spent by all reduces in occupied slots (ms)=2982
                Total time spent by all map tasks (ms)=3450
                Total time spent by all reduce tasks (ms)=2982
                Total vcore-milliseconds taken by all map tasks=3450
                Total vcore-milliseconds taken by all reduce tasks=2982
                Total megabyte-milliseconds taken by all map tasks=14131200
                Total megabyte-milliseconds taken by all reduce tasks=12214272
        Map-Reduce Framework
                Map input records=6090
                Map output records=48213
                Map output bytes=591032
                Map output materialized bytes=687464
                Input split bytes=113
                Combine input records=0
                Combine output records=0
                Reduce input groups=11219
                Reduce shuffle bytes=687464
                Reduce input records=48213
                Reduce output records=203
                Spilled Records=96426
                Shuffled Maps =1
                Failed Shuffles=0
                Merged Map outputs=1
                GC time elapsed (ms)=130
                CPU time spent (ms)=3120
                Physical memory (bytes) snapshot=621355008
                Virtual memory (bytes) snapshot=5170720768
                Total committed heap usage (bytes)=487063552
                Peak Map Physical memory (bytes)=359055360
                Peak Map Virtual memory (bytes)=2588798976
                Peak Reduce Physical memory (bytes)=262299648
                Peak Reduce Virtual memory (bytes)=2581921792
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters 
                Bytes Read=479968
        File Output Format Counters 
                Bytes Written=1821

```



### 3.1 数据集概况

- **数据文件**：stock_data.csv
- **总记录数**：6000条左右
- **字段**：Text（新闻标题）, Sentiment（情感标签）
- **停用词数量**：319个常见英文停用词

### 3.2 正面新闻TOP 100高频词分析

根据输出文件part-r-00000，正面新闻的前10个高频词为：

| 排名 | 单词   | 出现次数 | 业务含义                              |
| ---- | ------ | -------- | ------------------------------------- |
| 1    | aap    | 518      | 股票代码（可能是Advanced Auto Parts） |
| 2    | user   | 443      | 用户相关讨论                          |
| 3    | t      | 403      | 常见缩写或股票符号                    |
| 4    | s      | 319      | 常见缩写                              |
| 5    | https  | 282      | 新闻包含链接                          |
| 6    | today  | 258      | 当日交易信息                          |
| 7    | volume | 251      | 成交量（重要交易指标）                |
| 8    | day    | 223      | 交易日                                |
| 9    | long   | 218      | 做多/长期持有                         |
| 10   | like   | 190      | 用户观点表达                          |

**正面新闻特征词汇**：

- **交易术语**：volume（251次）、buy（146次）、bullish（105次）、breakout（121次）
- **积极情绪**：good（175次）、nice（174次）、great（65次）
- **价格走势**：higher（134次）、highs（100次）、green（52次）

### 3.3 负面新闻TOP 100高频词分析

负面新闻的前10个高频词为：

| 排名 | 单词   | 出现次数 | 业务含义                       |
| ---- | ------ | -------- | ------------------------------ |
| 1    | t      | 529      | 常见缩写                       |
| 2    | https  | 413      | 新闻链接                       |
| 3    | aap    | 411      | 同一股票在负面新闻中也高频出现 |
| 4    | short  | 356      | 做空/卖空                      |
| 5    | s      | 233      | 常见缩写                       |
| 6    | user   | 203      | 用户讨论                       |
| 7    | like   | 88       | 观点表达                       |
| 8    | today  | 80       | 当日信息                       |
| 9    | weekly | 79       | 周线分析                       |
| 10   | goog   | 76       | Google股票代码                 |

**负面新闻特征词汇**：

- **做空相关**：short（356次）、puts（50次）、bearish（45次）
- **下跌趋势**：lower（74次）、drop（27次）、red（44次）
- **危机事件**：coronavirus（69次）、pandemic（25次）

### 3.4 对比分析

| 维度     | 正面新闻                               | 负面新闻                       | 差异分析               |
| -------- | -------------------------------------- | ------------------------------ | ---------------------- |
| 交易方向 | long(218), buy(146), calls(68)         | short(356), puts(50), sell(60) | 负面新闻做空词汇更突出 |
| 情绪词   | good(175), nice(174), great(65)        | bearish(45), bear(26)          | 正面情绪词出现频率更高 |
| 价格表达 | higher(134), highs(100), breakout(121) | lower(74), drop(27), red(44)   | 明显的上涨vs下跌对比   |
| 特殊事件 | —                                      | coronavirus(69), pandemic(25)  | 负面新闻关注疫情影响   |

### 3.5 一些发现

1. **股票代码高频**：aap在正负面新闻中都排名前3，说明该股票在数据集时间段内波动较大，成为市场焦点
2. **技术分析术语**：
   - 正面：breakout（突破）、triangle（三角形态）、resistance（阻力位）
   - 负面：support（支撑位）、gap（跳空）说明投资者高度关注技术形态
3. **社交媒体特征**：user、https等词高频出现，说明数据来源可能包含社交媒体平台的财经讨论
4. **市场情绪对比**：
   - 正面新闻："bullish"（看涨）出现105次
   - 负面新闻："bearish"（看跌）仅45次
   - 说明数据集中正面新闻的情绪表达更强烈

------



## 四、性能分析

### 4.1 任务执行概览

| 指标             | 实际值                 | 说明                            |
| ---------------- | ---------------------- | ------------------------------- |
| **作业ID**       | job_1760860360156_0001 | YARN分配的唯一标识              |
| **执行模式**     | 非Uber模式             | 使用独立容器运行Map和Reduce     |
| **Map任务数**    | 1个                    | 输入文件未超过块大小，单个split |
| **Reduce任务数** | 1个                    | 默认配置，用于全局排序          |
| **总执行时间**   | 约30秒                 | 从提交到完成的时钟时间          |
| **任务状态**     | SUCCEEDED              | 执行成功                        |

### 4.2 详细性能指标

#### **输入输出统计**

| 阶段            | 指标                          | 数值             | 分析                             |
| --------------- | ----------------------------- | ---------------- | -------------------------------- |
| **输入阶段**    | Map input records             | 6,090            | CSV文件总行数（含头部）          |
|                 | Input split bytes             | 113              | Split元数据大小                  |
|                 | HDFS bytes read               | 482,312 (471 KB) | 读取CSV(480KB) + 停用词文件(2KB) |
| **Map输出**     | Map output records            | 48,213           | 经过清洗后的有效词汇数           |
|                 | Map output bytes              | 591,032 (577 KB) | 序列化前的原始大小               |
|                 | Map output materialized bytes | 687,464 (671 KB) | 写入磁盘的实际大小               |
| **Shuffle传输** | Reduce shuffle bytes          | 687,464 (671 KB) | 网络传输的数据量                 |
|                 | Shuffled Maps                 | 1                | 从1个Map任务收集数据             |
| **Reduce输入**  | Reduce input groups           | 11,219           | 不同的单词数量                   |
|                 | Reduce input records          | 48,213           | 接收的总记录数                   |
| **最终输出**    | Reduce output records         | 203              | 正面100 + 负面100 + 3个标题行    |
|                 | HDFS bytes written            | 1,821 (1.78 KB)  | 最终结果文件大小                 |

**关键数据**：

- 数据压缩比：482 KB → 1.8 KB，压缩率 99.6%，说明TOP 100筛选有效
- 平均词/记录：48,213 ÷ 5,790 ≈ 8.3词/条新闻，符合标题长度预期
- 去重效果：48,213个词 → 11,219个唯一词，重复率 76.7%

#### **时间性能分析**

| 维度                | 数值 | 占比 | 说明                |
| ------------------- | ---- | ---- | ------------------- |
| **Map阶段耗时**     | 6秒  | 20%  | 从0%到100%的时间    |
| **Shuffle阶段耗时** | 5秒  | 17%  | Map完成到Reduce开始 |
| **Reduce阶段耗时**  | 5秒  | 17%  | 从0%到100%的时间    |
| **任务调度开销**    | 14秒 | 46%  | 作业提交到Map启动   |
| **总墙上时钟时间**  | 30秒 | 100% | 用户感知的总时间    |

```
时间线:
00s ─────── 14s ────── 20s ───── 25s ──── 30s
 |           |          |         |        |
提交作业    Map启动   Map完成  Reduce完成     完成
            └─ 6s ─┘   └── 5s ──┘
```

**性能瓶颈**：

- 任务调度占比过高（46%）：小数据集时，调度开销无法摊销
- Map执行高效：3.5秒处理6090条记录，约1750条/秒
- Shuffle占比较高：传输671KB耗时5秒，网络或序列化开销明显

#### **资源消耗统计**

| 资源类型     | Map峰值  | Reduce峰值 | 总消耗        |
| ------------ | -------- | ---------- | ------------- |
| **CPU时间**  | —        | —          | 3,120 ms      |
| **物理内存** | 359 MB   | 262 MB     | 621 MB        |
| **虚拟内存** | 2,588 MB | 2,581 MB   | 5,170 MB      |
| **堆内存**   | —        | —          | 487 MB        |
| **GC时间**   | —        | —          | 130 ms (4.2%) |

**资源使用特点**：

- **内存使用合理**：Reduce仅用262MB处理11,219个唯一词
- **GC压力小**：GC时间占CPU时间的4.2%，属于健康水平
- **虚拟内存高**：JVM预分配的最大堆空间，实际未使用

#### **溢写和合并统计**

| 指标                   | 数值   | 说明                            |
| ---------------------- | ------ | ------------------------------- |
| **Spilled Records**    | 96,426 | Map和Reduce溢写到磁盘的总记录数 |
| **Merged Map outputs** | 1      | Reduce端合并的Map输出分区数     |
| **Failed Shuffles**    | 0      | 无Shuffle失败                   |

**溢写分析**：

- Map溢写：48,213条 → 说明内存缓冲区被写满至少1次
- Reduce溢写：48,213条 → Reduce端也发生了溢写
- 优化建议：增加`mapreduce.task.io.sort.mb`参数（默认100MB）

------

## 五、可以改进的方向

基于实际运行数据和性能分析，分析当前实现的不足并提出可行的改进方案。

### 5.1 添加Combiner减少Shuffle开销

#### 问题

从实际Counter数据发现：

```
Map output records: 48,213
Reduce input records: 48,213
Combine input records: 0        ← 未启用Combiner
Reduce shuffle bytes: 687,464   ← 所有数据通过网络传输
```

- 当前：48,213条记录全部传输，浪费带宽77.6%
- 实际需要：仅传输11,219个唯一词的统计结果即可

#### **改进方案**

**实现方式**（仅需1行代码）：

```java
public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Stock Word Count");
    
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);  // ← 添加此行
    job.setReducerClass(IntSumReducer.class);
    
    // ... 其他配置
}
```

------

### 5.2 优化Reducer内存使用

#### **问题**

当前实现在Reducer中使用HashMap存储全量词频：

```java
private Map<String, Integer> positiveWordCount = new HashMap<>();
private Map<String, Integer> negativeWordCount = new HashMap<>();
```

**内存占用分析**（基于实际数据）：

```
唯一词数: 11,219个
正面词汇: ~5,600个 × 50字节 ≈ 280 KB
负面词汇: ~5,600个 × 50字节 ≈ 280 KB
总计: ~560 KB (当前数据量完全可接受)
```

**扩展性问题**：

| 数据规模  | 唯一词数 | 内存占用 | 风险评估       |
| --------- | -------- | -------- | -------------- |
| 1x (当前) | 11K      | 0.5 MB   | 安全           |
| 10x       | 50K      | 2.5 MB   | 安全           |
| 100x      | 200K     | 10 MB    | 可接受但需监控 |
| 1000x     | 800K     | 40 MB    | 接近危险阈值   |
| 10000x    | 3M       | 150 MB   | 可能OOM        |

#### **改进方案：Top K堆算法**

**核心思想**：只维护100个最大值，而不是全量存储

**实现代码**：

```java
public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    // 使用最小堆，堆顶是第100大的元素
    private PriorityQueue<WordCount> positiveTopK;
    private PriorityQueue<WordCount> negativeTopK;
    
    @Override
    protected void setup(Context context) {
        // 堆大小固定为100，比较器按词频升序（最小堆）
        positiveTopK = new PriorityQueue<>(100, 
            Comparator.comparingInt(wc -> wc.count));
        negativeTopK = new PriorityQueue<>(100, 
            Comparator.comparingInt(wc -> wc.count));
    }
    
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) {
        String keyStr = key.toString();
        String[] parts = keyStr.split("_", 2);
        int sentiment = Integer.parseInt(parts[0]);
        String word = parts[1];
        
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        
        PriorityQueue<WordCount> targetHeap = 
            (sentiment == 1) ? positiveTopK : negativeTopK;
        
        // Top K维护逻辑
        if (targetHeap.size() < 100) {
            targetHeap.offer(new WordCount(word, sum));
        } else if (sum > targetHeap.peek().count) {
            targetHeap.poll();  // 移除最小值
            targetHeap.offer(new WordCount(word, sum));
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 从小到大取出，再反转得到降序
        List<WordCount> positiveSorted = new ArrayList<>(positiveTopK);
        positiveSorted.sort((a, b) -> Integer.compare(b.count, a.count));
        
        context.write(new Text("========== POSITIVE NEWS TOP 100 WORDS =========="), 
                      new IntWritable(0));
        for (WordCount wc : positiveSorted) {
            context.write(new Text(wc.word), new IntWritable(wc.count));
        }
        
        // 负面新闻同理
        List<WordCount> negativeSorted = new ArrayList<>(negativeTopK);
        negativeSorted.sort((a, b) -> Integer.compare(b.count, a.count));
        
        context.write(new Text(""), new IntWritable(0));
        context.write(new Text("========== NEGATIVE NEWS TOP 100 WORDS =========="), 
                      new IntWritable(0));
        for (WordCount wc : negativeSorted) {
            context.write(new Text(wc.word), new IntWritable(wc.count));
        }
    }
    
    // 辅助类
    private static class WordCount {
        String word;
        int count;
        WordCount(String word, int count) {
            this.word = word;
            this.count = count;
        }
    }
}
```

**性能对比**：

| 方案              | 内存复杂度 | 时间复杂度   | 100万词时内存 | 适用场景   |
| ----------------- | ---------- | ------------ | ------------- | ---------- |
| **HashMap全存储** | O(n)       | O(n log n)   | 50 MB         | 词数<50万  |
| **Top K堆**       | O(100)     | O(n log 100) | **0.005 MB**  | 很大的规模 |

------

### 5.3 Map输出缓冲区调优

#### **问题**

从Counter发现Map阶段有溢写现象：

```
Spilled Records: 96,426
= Map spilled: 48,213 + Reduce spilled: 48,213
```

**溢写触发原因**：

- Map输出缓冲区默认100MB（`mapreduce.task.io.sort.mb`）
- 当前Map输出687KB序列化后 + 元数据开销可能超过阈值（默认80%触发溢写）

#### **改进方案**

**配置调优**（在`mapred-site.xml`或代码中设置）：

```xml
<property>
    <name>mapreduce.task.io.sort.mb</name>
    <value>200</value>
    <description>Map输出缓冲区大小，默认100MB，增加到200MB</description>
</property>

<property>
    <name>mapreduce.map.sort.spill.percent</name>
    <value>0.90</value>
    <description>溢写阈值，默认0.8，提高到0.9</description>
</property>

<property>
    <name>mapreduce.task.io.sort.factor</name>
    <value>100</value>
    <description>合并因子，默认10，增加到100减少合并次数</description>
</property>
```

**代码方式配置**：

```java
Configuration conf = new Configuration();
conf.setInt("mapreduce.task.io.sort.mb", 200);
conf.setFloat("mapreduce.map.sort.spill.percent", 0.9f);
conf.setInt("mapreduce.task.io.sort.factor", 100);
```

------

### 5.4 优化停用词

#### 问题

**当前停用词表的不足**

从输出结果发现：

```
高频无意义词:
- t (403)        ← 单字符未过滤
- s (319)        ← 单字符未过滤
- https (282)    ← URL前缀未过滤
- aap (518)      ← 股票代码（领域特定噪音）
```

#### **改进方案**

**多次过滤**：

```java
public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    private Set<String> stopWords = new HashSet<>();
    private Set<String> stockSymbols = new HashSet<>();  // 新增
    private Pattern urlPattern;  // 新增
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 1. 加载通用停用词（现有逻辑）
        loadStopWords(context);
        
        // 2. 加载股票代码白名单/黑名单
        loadStockSymbols(context);
        
        // 3. 编译URL正则
        urlPattern = Pattern.compile("https?|www|com|org");
    }
    
    private boolean isValidWord(String word) {
        // 规则1: 长度过滤（单字符和过长词）
        if (word.length() < 2 || word.length() > 20) {
            return false;
        }
        
        // 规则2: 通用停用词
        if (stopWords.contains(word)) {
            return false;
        }
        
        // 规则3: URL
```





## 六、遇到的问题

### 问题1

```bash
faye@faye-virtual-machine:~/Desktop/bigDataProcess/hadoop_installs/hadoop-3.4.0$ $HADOOP_HOME/sbin/start-dfs.sh
Starting namenodes on [localhost]
Starting datanodes
Starting secondary namenodes [faye-virtual-machine]
faye@faye-virtual-machine:~/Desktop/bigDataProcess/hadoop_installs/hadoop-3.4.0$ jps
5761 SecondaryNameNode
5962 Jps
5374 NameNode
5519 DataNode
faye@faye-virtual-machine:~/Desktop/bigDataProcess/hadoop_installs/hadoop-3.4.0$ bin/hdfs dfs -mkdir -p /stock/input
mkdir: Cannot create directory /stock/input. Name node is in safe mode.
```

把 NameNode、DataNode、SecondaryNameNode 都拉起来了（`jps` 结果正常），现在的报错是**NameNode 处于 Safe Mode（安全模式）**。安全模式是 HDFS 启动时的只读态：NameNode 等待 DataNode 上报块信息，达到阈值后自动退出；在此期间包括 `mkdir` 在内的写操作会被拒绝。

解决方法：

解除安全模式

```bash
bin/hdfs dfsadmin -safemode leave
# 再试
bin/hdfs dfs -mkdir -p /stock/input
bin/hdfs dfs -ls /
```

