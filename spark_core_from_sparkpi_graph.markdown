#		从SparkPi图解Spark core
----------------
@玄畅    
2015.1.30


spark core是spark的核心库，执行最基础的RDD操作。通过本文，从万年pi入手，逐层分解spark core整个运行过程，一窥其貌。

文字是辅助阅读的面包屑。

先不管那么多名词，一路看下去，理脉络。
 
##		万年Pi

这个代码做了以下几件事：

1.	初始化配置`SparkConf`
2.	初始化上下文`SparkContext`
3.	准备数据，从1～N, 分slices段，用ParallelCollectionRDD表示
4.	map变换，使用大括号里的函数计算ParallelCollectionRDD中的每一个数据项，用MappedRDD表示
5.	reduce，合并数据，计算函数为: `_ + _`

```
object SparkPi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
}

```

##		启动 & 初始化配置和上下文

1.	入口spark-submit, SparkSubmit会调用`SparkPi`的入口函数`main`() Driver, client, master, worker的启动和交互关系另表, todo)
2.	初始化SparkConf    
	执行spark-submit时传入的vm参数，spark参数统统由这个对象表示
3.	初始化SparkContext
	初始化：UI，statusTracker, progressBar, jars, files, env, heartbeatReceiver, masterUrl, applicationId。。。
	
	关键的是：dagScheduler, taskScheduler, schedulerBackend, blockManager
	
	taskScheduler用于提交`TaskSet`即提交RDD    
	schedulerBackend用于接受任务，分配任务给worker去执行
	
	从createTaskScheduler函数下去，取得`spark-submit`的master url，根据master的scheme协议类型生成不同的`SchedulerBackend`、`TaskScheduler`, master的类型有：local、local-cluster、simr、spark、mesos、zk、yarn-standalone、yarn-cluster。
	相应的种类：LocalBackend, SparkDeploySchedulerBackend, SimrSchedulerBackend, CoarseGrainedSchedulerBackend, CoarseMesosSchedulerBackend, MesosSchedulerBackend

![init](img/spark_core_init.jpg)


##		提交job
上文，初始化完了sparkContext, 执行到下面的代码:

```
    val count = spark.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)

```

1.	步骤分解。这行代码，分为三步:  

	1.	`spark.parallelize`生成`ParallelCollectionRDD`
	2.	然后, `ParallelCollectionRDD`执行`map()`函数，生成`MapPartitionsRDD`
	3.	`MapPartitionsRDD`执行`reduce`函数，生成最终结果

	上面可以看出RDD之间是有依赖关系的，这个依赖关系怎么形成的？

2.	依赖关系    
	`new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.map(cleanF))`，这个`this`指向`ParallelCollectionRDD`, 这俩RDD都继承自`RDD`, `RDD`的一个构造函数`def this(@transient oneParent: RDD[_]) =
    this(oneParent.context , List(new OneToOneDependency(oneParent)))`会定义当前RDD的上一个RDD是谁，并用一个`deps: Seq[Dependency[_]]`对象表示所有的依赖对象。
    
	如此，每个RDD都会记录与上一个RDD的关系，是一对一，还是一对多.

3.	计算函数     
	计算函数是用户定义的`map()`中的代码块。    
	一个RDD包含多个分区partition，这个计算函数会应用到所有的partition中的所有数据。这就隐含着两个遍历操作：遍历RDD的partition；遍历partiton中的数据
	
	如此，整个RDD经过算子的运算，原始数据变成了想要的数据，即map过程，就像数学中的函数概念, `f(x) = 3x + 100`。但是，这里仅仅是表示这种计算关系，并没有马上计算出来结果。真正执行计算的时机由action函数触发，如下面的`reduce`
	
4.	合并     
	上面这些准备数据，演算过程，都是虚的，真正触发计算过程的时机在此。    
	既然是分布式计算，就需要把数据和算子分布到多台服务器上。    
	这里首先把map、reduce的用户定义的函数序列化，传输到不同的机器上。而partition实际的分片数据是可以根据`Partition`的信息定位到数据的位置的。
	
	`reduce(_ + _)`函数会把整个计算过程封装成一个`Job`

![init](img/spark_core_submitjob.jpg)


##		DAGScheduler构建final stage
`DAGScheduler`对象在`SparkContext`初始化的时候会实例化，这里开用了。

这里会对job做一些整理变换，以便符合适合下一阶段要求。在job中rdd的partition并不包含实际数据，只是partition的序号。

1.	提交Job
	`eventProcessActor ! JobSubmitted`  发射消息  
	这里开始使用akka， `eventProcessActor`使用`DAGSchedulerEventProcessActor`	来处理收到的消息。
	
	actor`DAGSchedulerActorSupervisor`收到提交的`JobSubmitted`。dag会执行`handleJobSubmitted`来处理提交的job。

2.	处理提交的job，生成finalStage     
	这里可以看到提交的是finalRDD，表示这是老末了，通过这位老末可以向上遍历找到所有的父RDD及其包含的相应的父partition。
	
	`handleJobSubmitted`就是根据RDD的依赖关系向上遍历父RDD，关系树中的RDD都封装成stage，结果就是finalRDD变成finalStage。
	
	这里遍历依赖关系使用先进后出的栈。先把自己finalRDD送进去，pop出来，找到所有的`dependencies`, 遍历依赖, 把`ShuffleDependency`类型的依赖封装成一个`Stage`对象添加到返回的列表中，非`ShuffleDependency`继续取出`dependencies`入栈。
	
	每个`Stage`对象中都包含当前job的jobId。
	
	这样，父依赖RDD和当前的finalRDD都转换成了`Stage`对象，即：finalStage包含了父stages
	
![](img/spark_core_dag_finalstage.jpg)
	
	
##		DAGScheduler提交stage（submitStage）

上一步对finalRDD进行了大清洗，把RDD及其父RDD洗白成`Stage`.

1.	检查block miss的stage     
	检查每个stage对应的存储块是否存在, 即：每个rdd的partition id封装成一个`BlockId`, 向master的`BlockManagerMasterActor`发消息(`GetLocationsMultipleBlockIds`)，收到消息后，根据本地的`blockLocations`取出`BlockManagerId`, 如果没有则返回空。
	
	这样确保所有的stage对应的block都是存在的，然后把stage存入`waitingStages`
	
	不得不说说`BlockManagerId`，这货有`executorId, host, port`，即它已经许身给了某个worker，有身份和地址的对象。
	
2.	正式提交stage
	遍历`waitingStages`的拷贝，`submitStage`提交每一个stage, 这样没有miss的stage，直接进入下一步：`submitMissingTasks`
	
	提交时，把`Stage`对象中每个分区和rdd序列化，把stage.rdd封装成`Broadcast`对象，默认为`TorrentBroadcast`。stage（RDD）的partition封装成task, 有两种类型的task：ResultTask、ShuffleMapTask。而`TaskSet`是tasks的持有者，`taskScheduler.submitTasks(...)`实际执行提交任务。    
	`taskScheduler`是在sparkContext初始化时生成的任务调度器。
	
##		任务调度器提交任务
入口：`TaskSchedulerImpl#submitTasks`

书接上文，DAG把finalRDD最后封装成`Task`对象，调用sparkContext里的`taskScheduler`。

`taskScheduler`中，加一个`maxTaskFailures`最大任务失败数，把TaskSet封装成`TaskSetManager`对象。提交到`Pool`中。这里的`Pool`即是`SparkContext`初始化时构建TaskSchedulerImpl时初始化的池化对象，把任务提交到`ConcurrentLinkedQueue`。

spark任务躺在linkedqueue中，`backend.reviveOffers()`触发下一步任务的执行。backend就是在初始化时根据不同的master类型确定的不同backend类型。

*模块之间通过队列解耦，数据在不同的模块中由不同的对象来封装和表达。*

在backend（CoarseGrainedSchedulerBackend）的reviveOffers中，`Driver`发送消息`ReviveOffers`。通过发消息的方式，任务的执行就不是顺序执行了，而是乱序并行执行。driverActor收到消息，从TaskSchedulerImpl中取出随机shuffle的task，发射task，把任务发送到各个`Executor(worker)`上执行，对象在网络上传播就需要序列化对象。发送消息对象`LaunchTask`到具体的任务执行者`executorActor`。


![](img/spark_core_taskscheduler.jpg)


##		Executor任务执行1

`CoarseGrainedExecutorBackend`收到`LaunchTask`消息，首先要做的就是反序列化task描述对象`TaskDescription`。下面的事情就是放到线程池中执行任务。

实例`TaskRunner`对象，放到队列中，线程池执行。

`TaskRunner`运行时，反序列化task为三个对象：taskFiles，taskJars， taskBytes。`taskFiles`，`taskJars`会加载到时机的文件，jar引入到当前类加载器中。

回顾前面application，`spark-submit`提交时会添加jar，各种参数。application执行时需要用户提交的外部依赖jar, 在每个线程中执行任务时就要把这些外部以来文件和依赖jar加载进来。

`taskBytes`反序列化为`Task`对象，运行的时候，首先实例化任务的上下文`context = new TaskContextImpl(stageId, partitionId, attemptId, runningLocally = false)`, context存入`ThreadLocal`。

现在万事具备，只需要执行Task了(application的代码), Task有两种：`ShuffleMapTask`、`ResultTask`,默认为`ResultTask`

![](img/spark_core_executor_before_run.jpg)

##		执行ResultTask（默认）

反序列化`taskBinary: Broadcast[Array[Byte]]`, 还原出rdd和计算函数。`Broadcast`是DAG中封装的对象。

计算需要两部分内容：计算函数，数据。
计算函数就是application中map函数体的内容。数据就需要从存储系统中获取。

执行函数和取数据的函数:   
`func(context, rdd.iterator(partition, context))`

而在提交job的代码如下：

```
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      resultHandler: (Int, U) => Unit)
  {
    val processFunc = (context: TaskContext, iter: Iterator[T]) => processPartition(iter)
    runJob[T, U](rdd, processFunc, 0 until rdd.partitions.size, false, resultHandler)
  }

```

processFunc的签名为:`func: (TaskContext, Iterator[T]) => U,`

跟ResutTask中调用时的一样, 这样前后就对上号了。

1.	取数据
	取数据从rdd的`iterator`函数进去，传入当前task所对应的partition和`TaskContext`。
	
	数据的标示：不论数据存储在哪个level上，都需要有个唯一的标示来表示实际存储的数据:`RDDBlockId`, blockId是用rdd id和partiton index组合成的字符串。
	
	数据序列化：不论保存在何种level上，都可以指定序列化，序列化以及压缩能够大幅节省内存，但是消耗CPU，空间与时间的平衡。
	
	数据位置氛围本地和远程，本地存储类别为：memoryStore, tachyonStore, diskStore。远程存储类型为：todo

	**本地存储:**
	
	memoryStore保存在heap中，`LinkedHashMap`持有。根据blockId取出数据，如果没有反序列化则反序列化成`Iterator[Any]`对象。
	
	tachyonStore保存在tackyon分布式内存存储中，根据blockId取出ByteBuffer数据，反序列化成`Iterator[Any]`对象。
	
	diskStore保存在本地磁盘中，根据blockId，从`DiskBlockManager`中拼装文件名、文件路径，返回File对象，通过`RandomAccessFile`随机访问文件对象读取文件的具体内容，返回`ByteBuffer`, 反序列化成`Iterator[Any]`对象。
	
	通过上述三种本地存储，取出`Iterator[Any]`数据对象，强制类型转换成`BlockResult`。如果取出的数据为空，则表示当前这个partition的数据没有分布在当前的`Executor`上，需要从远程读取数据。
	
	**远程存储：**
	
	本地没有当前task对应的partition数据，需要先问下`Master`这个数据分布在哪里`master.getLocations(blockId)`。master收到消息`GetLocations(blockId)`, `BlockManagerMasterActor`根据blockId在`blockLocations:JHashMap[BlockId, mutable.HashSet[BlockManagerId]]`中get出数据块表示对象`Seq[BlockManagerId]`
	
	从master咨询得到partiton的数据块表示`Seq[BlockManagerId]`, 随机混排一下，再把数据块远程下载下来`blockTransferService.fetchBlockSync(
        loc.host, loc.port, loc.executorId, blockId.toString).nioByteBuffer()`

	在fetchBlocks抓取数据块时有两种方式：`NettyBlockTransferService`和`NioBlockTransferService`。netty和spark写的nio。在`SparkEnv#create`中，用户传入的spark参数`spark.shuffle.blockTransferService`(默认**netty**)实例化数据块传输对象`NettyBlockTransferService`或`NioBlockTransferService`。
	
	以`NioBlockTransferService#fetchBlocks`为例，`SparkEnv`在初始化的时候确定了使用哪个传输服务(默认`NettyBlockTransferService`), `NettyBlockTransferService`对象在初始化的时候会启动Netty Server来监听网络端口，发送和接收数据。SparkEnv封装`NettyBlockTransferService`到对象`BlockManager`在sparkContext初始化的时候调用blockManager初始化方法`env.blockManager.initialize(applicationId)`。如此：数据块传输的netty server就启动了。
	
	
	数据块的传输处理handler为`TransportChannelHandler`, 处理过程为netty的pipeline。handler包含三个成员，responseHandler处理响应，requestHandler和client发送请求。
	
	responseHandler通过pipeline的解码，得到ResponseMessage，message有四种类型：ChunkFetchSuccess、ChunkFetchFailure、RpcResponse、RpcFailure，收到数据后，会直接从channel中取到byteBuffer，从`outstandingFetches`取出`ChunkReceivedCallback`，调用onSuccess回调函数，回调函数再调用`BlockFetchingListener`监听器，最终返回partition的完整数据块。
	
	小结：取partition数据，如果数据保存在本地，就从cache, tachyon, disk中读取；如果保存在远程，则通过netty或者NIO读取。最终返回的都是ByteBuffer, 反序列化成响应的对象。
   
   **重新计算**
   
   如果storageLevel不为空，但是存储系统中都没有数据，那么就需要计算出所需的数据。
   
   这时分三种情况：1. cache正在被加载，等待加载后直接返回；2. 从checkpoint读；2. 重新计算。
   
   不同的RDD类型有不同的compute实现。ParallelCollectionPartition是直接返回起iterator数据。这个就是最原始的数据了。
   
2.	应用计算函数
	
	现在数据有了，计算函数也有了，直接调用计算函数，传入数据，遍历partition，应用函数计算每一项数据。返回计算后的数据


![](img/spark_core_executor_resulttask.jpg)

##		执行ShuffleMapTask

todo ...
	


    
    


