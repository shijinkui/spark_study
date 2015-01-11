#	学习spark

##		分析spark从两个纬度
        
1.	 框架层面		框架焦点在于处理流程，系统交互，性能等   
2.	 数据流		数据流关注于数据的转换、合并、shuffle、结果

##		概念
1.	Master	中心节点，记录worker、Driver信息 
2.	Worker	任务执行的节点，取Master发送的Driver，运行任务
3.	Client	SparkSubmit启动，把用户的application(包含main的类)打包，分装成Driver, 提交给Master
4.	Driver	Client向Master注册，Master随机取非满负荷的Worker，绑定Worker和此Driver，向此Worker发送任务消息


Cluster Programming Models
	distributed storage abstraction


###	备忘：scala 解释器 interactive


To recap previous chapter, RDDs provide the following facilities:

*	Immutable storage of arbitrary records across a cluster (in Spark, the records are Java objects).
*	Control over data partitioning, using a key for each record.
*	Coarse-grained operators that take into account partitioning. • Low latency due to in-memory storage.


As long as each batched record fits in the CPU cache, this leads to fast transformations and conversions.


