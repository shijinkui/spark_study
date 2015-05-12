#		Spark八法之不攻: DAG、Lazy

>	不攻: 故用兵之法, 无恃其不来, 恃吾有以待也; 无恃其不攻, 恃吾有所不可攻也。
>	
>	以DAG和Lazy来应对数据计算的任务

##		综述overview

>	在图论中，如果一个有向图无法从任意顶点出发经过若干条边回到该点，则这个图是一个有向无环图（DAG图）。    
>	因为有向图中一个点经过两种路线到达另一个点未必形成环，因此有向无环图未必能转化成树，但任何有向树均为有向无环图。    
>	--[维基百科](http://zh.wikipedia.org/wiki/%E6%9C%89%E5%90%91%E6%97%A0%E7%8E%AF%E5%9B%BE)

####	几个概念 

*	Job: 用户提交的spark任务, 一个任务分为多个Stage
*	Stage: 阶段, 有两种Stage: ResultStage、ShuffleMapStage
*	DAG: Directed Acyclic Graph, 有向无环图。DAG用来表达整个Job。

####	Stage、Patition、Task
Spark中各个子系统之间边界清晰, 子系统内对相似的概念有不同的命名。
一个大块数据, 被分割成n块, 单个分块的数据在RDD中称为`Partition`, 在DAG中表达对分块的操作过程称为`Stage`, 在Executor中实际执行对分块数据的操作过程称为`Task`

[图1: DAG的概念图]    
![DAG Concept](img/spark_8_2_dag_concept.jpg)

[图1: Spark DAG的概念图]    
![Spark DAG](img/spark_8_2_dag_concept2.jpg)




##		特点feature
##		结构structure
##		细节detail
##		关系relation
##		总结summary