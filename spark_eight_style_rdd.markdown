#	spark八法之方圆:RDD

@时金魁(花名玄畅)   2015.3

##	Overview

方圆: 数据为阳，算子为阴；算子为方，数据为圆。阴阳应象，天人合一，再不可分。

RDD(Resilient Distributed Datasets)弹性分布式数据集, spark的核心理念, 对数据及其操作抽象表达。

Spark整体上略说可以分为两部分:RDD和运行时框架, RDD表达一种运行时数据分片分布、依赖关系、计算的接口规格以及进入Spark运行时框架执行运算的入口。

RDD规格标准：

1. **Spark运行期框架支持入口**    
	调用包含*runJob()*的函数, 开始执行任务。    
	运行期执行过程框架主要由SparkContext, DAGScheduler, TaskScheduler, CoarseGrainedSchedulerBackend, CoarseGrainedExecutorBackend, Executor提供框架支持
2. **partition**
3. **compute**
4. **dependency**
5. **transform: 算子**     
6. **其他: 惰性计算**    
	
图1: RDD概要图      
![overview](img/spark_8_overview.jpg)

上图简要描述RDD的生命周期, RDD由partition构成, partition通过存储系统拿到实际的数据, 应用定义的一组具有上下依赖关系的算子(RDD), 算子们运算完每个partition数据后得到transform结果, 再去执行聚合函数, 得到最终结果。

RDD只是定义了整个过程, Spark运行时框架(dag, schedule, executor等)保证任务分布式、弹性、并行执行。

RDD(弹性分布式数据集)去掉形容词，主体为：数据集。如果认为RDD就是数据集，那就有点理解错了。个人认为：RDD是定义对partition数据项转变的高阶函数，应用到输入源数据，输出转变后的数据，即：**RDD是一个数据集到另外一个数据集的映射，而不是数据本身。** 这个概念类似数学里的函数`f(x) = ax^2 + bx + c`。这个映射函数可以被序列化，所要被处理的数据被分区后分布在不同的机器上，应用一下这个映射函数，得出结果，聚合结果。

下面就细说RDD具体规格。

##	Spark运行期框架支持入口
一个RDD被运算离不开框架的支持。SparkContext会初始化运行时各个组件，封装和提交RDD计算任务, 相关函数为：
    
函数名字 | 描述
----------- | -----------
sc.clean()		|	清理算子中的闭包函数中的未引用到的变量及其他, 以便持久化
sc.persistRDD	|	注册持久化事件: 持久化当前RDD到存储系统中(内存或磁盘)
sc.unpersistRDD	 |	注册反持久化事件: 销毁当前RDD在存储系统的持久化副本
new RDD(rc, ...) | 构建新的RDD对象所需的参数
sc.runJob() | **开始提交运算任务, 入口**
sc.runApproximateJob() | **开始提交返回近似结果的任务, 入口**

`sc.runJob`和`sc.runApproximateJob`是提交运算任务的入口。往下就是DAG, Scheduler, Executor的菜了。

如下图所示：RDD及其依赖RDD被层层包装加工、分发、调度执行, partiton对应的实体数据应用到算子里制定的计算函数; 然后再原路返回, 这样RDD中每个partition对应一个计算结果, reduce这些结果为终极结果, application中直接得到这个最终运算结果。

图2: Spark运行时框架与RDD关系图       
![](img/saprk_8_framework.jpg)

##		**partition: 分区**

RDD, 名为弹性数据集, 大数据讲的是一个大字, 数据太大必然要切分为一个个partition分片, 这些partition分布在不同机器上。

怎么切分是`Partitioner`定义的, `Partitioner`有两个接口: `numPartitions`分区数, `getPartition(key: Any): Int`根据传入的参数确定分区号。实现了Partitioner的有：    
1. HashPartitioner
2. RangePartitioner
3. GridPartitioner
4. PythonPartitioner

一个RDD有了Partitioner, 就可以对当前RDD持有的数据进行划分, 通过`def getPartitions: Array[Partition]`获取所有的partition。在具体计算的partition的时候就可以通过数组下表确定partition。

根据dependency依赖关系, 可以拿到上一级RDD的partition数据, 如果上一级的RDD数据没有缓存没持久化, 那就根据RDD定义的算子函数计算出partition。以此类推, 一直追溯到没有依赖的root RDD(HadoopRDD), 这个没有依赖的RDD就是原始输入的数据源。拿到这个数据源, 再依次展开执行刚才的追溯。

图3: 在依赖链中计算出partition实际数据        
![](img/saprk_8_1_dep_data.jpg)       

用户的写的app代码中, 算子顺序调用, 最后一个算子的最后面的RDD, 持有这个RDD就可以向上追溯到源数据, 回来再一步步执行RDD的transform partition得到各个Partition的数据, 最终得到末尾的RDD数据。


##		**compute: 计算**
`def compute(split: Partition, context: TaskContext): Iterator[T]`
接口定义上看：就是计算分区, 返回数据集合。具体怎么个计算方法, 需要具体的RDD子类去定义。


##	**Dependency: 依赖**

图4: RDD的依赖关系         
![](img/saprk_8_1_dep.jpg)      

`Dependency`定义的抽象函数为: `def rdd: RDD[T]`, 表示这个依赖对应的父RDD对象。

依赖分为:    

1.	NarrowDependency 约束当前RDD的每个分区依赖父RDD的**少量**分区        
	*	OneToOneDependency		一对一关系
	*	RangeDependency			一对一关系, 父子RDD一定范围内的分区一一对应。
	*	PruneDependency
2.	ShuffleDependency
	洗牌。表示父子RDD之间的partition是混洗关系。即: 子RDD的每一个partition的数据由父RDD所有partiton的一部分组成。

NarrowDependency让数据从父RDD到子RDD数据条数减少, 最多保持相等; ShuffleDependency让数据从父RDD到子RDD数据条数保持不变, 顺序shuffle.

依赖用来表示父RDD依赖和当前RDD的关系, 子依赖持有父依赖的引用, 这样就能在当前RDD中根据依赖关系拿到父RDD对应的partition的数据。

图5: RDD调用链和依赖链         
![](img/saprk_8_1_dep_fn.jpg)      

依赖关系表示RDD之间的对应关系, 多个RDD之间链式调用算子, 最后的关系图为由内到外随着算子调用增多, 层级逐步向外扩展。即: 最外层的RDD能根据与父RDD的依赖关系逐层递进一直找到原点RDD(没有父RDD)。这样, 中间某个RDD的数据分区丢失数据, 就可以根据依赖关系溯源向上重新计算出数据。这就是所谓的弹性。

##		transform: 算子

>	狭义的算子是: 如同普通的运算符号作用于数后可以得到新的数那样，一个算子作用于一个函数后可以根据一定的规则生成一个新的函数。(来源于[百科](http://baike.baidu.com/view/53313.htm))

我个人的理解, 所谓算子就是单个输入单个输出的函数。而RDD中定义常用函数如map, filter等, 函数内部会产生不同的RDD, 就是transform的意思, 按照产生新的RDD个数和性质不同, 我分为了三类: 单算子、复合算子、action算子:

1.	**单算子**    
	顾名思义, 只会产生一个新的RDD, 即只产生一次数据transform

	名称 | RDD | 描述
	------------ | ------------ | ------------
	coalesce() | CoalescedRDD | 合并分区, 也可以做shuffle合并
	union(other: RDD)| PartitionerAwareUnionRDD或UnionRDD | 合并两个RDD
	cartesian(other: RDD)|CartesianRDD|计算两个RDD的笛卡尔积
	pipe|	PipedRDD|把partition中的数据作为输入, 执行管道命令
	mapPartitions	|	MapPartitionsRDD	|	遍历RDD的所有partition, 应用一个自定义函数到partition所有元素
	mapPartitionsWithIndex	|	MapPartitionsRDD	|	遍历RDD的所有partition, 应用一个带有*原partition下标*的自定义函数到partition所有元素
	zip|ZippedPartitionsRDD2|把两个相同partition数目, 相同partition中元素数的RDD对应成一个新的RDD, 元素为两个RDD对应tuple。拉链
	zipPartitions|zip多个RDD, 应用函数到zip的partition, 要求RDD的partition数相同, partition元素数可以不同
	zipWithIndex|ZippedWithIndexRDD|生成新的元素中包含所在partition索引位的RDD
	glom|MapPartitionsRDD|把所有Partition中的数据合并到一个数组中
	zipWithUniqueId|MapPartitionsRDD|

2. **复合算子**    
	会产生多个新的RDD, 即产生多次有次序的数据transform

	名称 | RDD | 描述
	------------ | ------------- | ------------
	groupBy|MapPartitionsRDD -> RDD | 按照key分组, 得到的是key -> Iterator[T]的映射
	sortBy | MapPartitionsRDD -> ShuffledRDD | 先对key进行变换, 再按照key排序
	intersection() | MapPartitionsRDD -> CoGroupedRDD -> MapPartitionsRDD -> MapPartitionsRDD | 两个RDD交集
	subtract|MapPartitionsRDD|SubtractedRDD|一个RDD减去另外一个RDD
	countByValue|MapPartitionsRDD -> MapPartitionsRDD -> MapPartitionsRDD/ShuffledRDD|计算

3. Action执行函数

	当前RDD为计算的末端, 进入提交RDD计算任务的入口。上面的所有内容都是吹吹牛不算数的, 调用了`runJob`就表示：我要开始兑现刚才吹的牛。
	      
	1.	runJob()	提交job任务 
	`  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {...}`    
		   
	2.	runApproximateJob()	提交返回近似值的job任务 
		  
	`def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      timeout: Long): PartialResult[R] = {`
      
   算子定义了对数据做变换的过程, 并未立即执行。执行的入口是`runJob`或`runApproximateJob`, 这两个多态函数有一个共同特点, 就是: **都包含参数`TaskContext, Iterator[T]`**，这个迭代器代表partition的数据, 真正执行的地方是Executor的线程池里获取一个线程, 执行Task, 在计算时需要TaskContext辅助。*后面详解* 

	用到这两个入口函数的, 就是计算入口, 如下列表: 
	
名称 | 应用的函数 | 描述
------------ | ------------- | ------------
foreach|f: T => Unit|遍历所有元素, 应用函数
foreachPartition| f: Iterator[T] => Unit|应用一个函数到所有的partition对象(不是partition中所有元素)
collect|-|收集RDD中所有的元素
reduce|f: (T, T) => T|对应于scala的reduceLeft函数, 应用f函数到partition, 再应用f到每个partition的结果
fold|fold(zeroValue: T)(op: (T, T) => T): T|对应于scala的fold函数. 折叠
aggregate|(zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U)|对应于scala的aggregate函数, 聚合partition使用一个函数, 聚合partition的结果使用另外一个函数
treeAggregate|-|-
count|-|计算所有元素总数
countApprox|timeout: Long|计算时允许超时, 得到一个可以忍受的近似结果
countByValueApprox|timeout:Long|类似, 允许超时, 所以会得到近似结果
countApproxDistinct|-|得到相同元素的近似数, 使用HyperLogLog算法
take|num:Int|对应于scala的take函数, 选择第num个值


每个变换过程有有两个计算函数: 1. 应用到每个partition的function, 得到result; 2. 应用到每个partition的result上的function。
这两个函数都是数据映射的定义，真正开始执行是右包含`runJob()`函数的action算子启动。


##		其他: 惰性计算


以比较简单ResultTask为例子: 

```
  override def runTask(context: TaskContext): U = {
    // Deserialize the RDD and the func using the broadcast variables.
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)

    metrics = Some(context.taskMetrics)
    func(context, rdd.iterator(partition, context))
  }

```

`func`就是算子里定义的映射函数, 序列化后经过DAG, TaskManager, Scheduler等Spark框架层层封装到在Task线程中执行实际的任务, 反序列化函数, 应用到partition数据集上。计算出结果。

在前面算子中, 函数的参数中一般都有个`Iterator[T]`形参, 就是表示partition数据。

`=>`这种形式的高阶函数作为参数，在函数被实际调用的时候才去求值, 这就是所谓的惰性计算。

高阶函数见: [scala Higher-order Function](http://docs.scala-lang.org/tutorials/tour/higher-order-functions.html)
 
##		总结

**RDD几个特点**:    
1.	数据: partition
2.	RDD之间的关系: Dependency
3.	惰性计算
4.	算子: 确定当前RDD分区和子RDD分区的映射关系, transform
5.	执行: 通过`runJob()`入口进入spark分布式框架流程, 进行任务的调度、执行、容灾

**一句话总结**:   
RDD是一个抽象的概念, 定义了数据transform映射关系, 数据链上下游关系, 组合了常用的transform形式, 提供了执行运算的入口。

----------------EOF---------------
