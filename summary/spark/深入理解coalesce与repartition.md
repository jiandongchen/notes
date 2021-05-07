# 深入理解coalesce与repartition

## 背景

在spark中，要改变一个rdd的partition数目，一般有两个方法1. repartition 2. coalesce。假如现在有一个场景，一个rdd在经过spark运算之后，持久化时，在我知道这个rdd落地会很小的情况下，我肯定会将这个rdd的所有partition进行合并再存储，因为小文件尽可能的少，可以减轻hdfs namenode的压力。那么问题来了，是应该使用repartition(1)比较好呢？还是coalesce(1)？

## 区别

如果你去简单的看一下repartition和coalesce的源码，你会发现coalesce就是shuffle默认为false的repartition。再深入的了解一下呢，你会得到如下信息：

先假设rdd原始分区数为m，使用rdd.coalesce(n)会发生两种情况（=情况不考虑，啥都不发生）

1.  n < m 

   原始rdd会在不发生shuffle的情况下从m个分区变成n个分区，很明显一个父rdd分区对应一个子rdd分区（窄依赖的概念）

2. n > m

   很明显这是一个父rdd分区对应多个子rdd分区的情况（宽依赖）要达成必须发生shuffle

那么我们之前提及的问题coalesce(1)对应的一定是第一种情况，那么表面看起来coalesce(1)是优于repatition(1)的，因为前者不发生shuffle，而后者一定，一般来说shuffle是集群中最耗时的操作。

那么事实真的是这样吗？

## 发现问题

图1：

![coalesce(1)](C:\Users\jiandong.chen\Desktop\coalesce(1).jpg)

图2：

![repartition(1)](C:\Users\jiandong.chen\Desktop\repartition(1).jpg)

运行同一个spark app两次，唯一的区别是在最后一步持久化tohive以前，图1使用了coalesce(1)来将分区合并，而图2使用了repartition。奇怪的问题出现了，抛开效率不谈，使用coalesce的情况下，任务甚至都失败了，而repartition可以顺利执行。那么问题点究竟出在了哪里呢？

## 解决过程

1. 解决的第一步肯定是分析两个app执行过程中的不同点，仔细观察图1与图2，很容易就能发现其中最重要的不同，图1比图2少了一个stage。

2. 那么为什么coalesce(1)会比repartition(1)少一个stage呢？

   图1：

   ![coalesce(1)-dag](C:\Users\jiandong.chen\Desktop\coalesce(1)-dag.jpg)

   图2：

   ![repartition(1)-dag](C:\Users\jiandong.chen\Desktop\repartition(1)-dag.png)

   观察两者的dag图可以很明显的看出，reparation(1)在快结束时有一步exchange操作，而coalesce没有。那么可以推断出多的那个stage就是exchange了。

3. 为了验证这个想法仔细观察两者的stage情况

   可以发现，repartition(1)在最后两个stage的task总数为400 + 1 = 401

   而coalesce(1)仅为1，并行度竟然只有1！就是这1的并行度导致了stage的失败，进而导致了app的失败。

## 结论

问题到了这里，coalesce和repartition的区别已经很清晰了。repartition的分区缩小效果是在之前stage的基础上添加了一层stage来完成的，不会影响到之前stage中的操作。而coalesce不同，它的分区缩小效果可以看作是当前stage中的最后一环操作，进而会影响到分布式执行那个stage的效果，特别是coalesce(n)中n特别小的情况下。

那么结论就是尽量使用repartition，而非coalesce，除非你真的很确定你写下的coalesce在干嘛。并且当n小于当前rdd分区数时coalesce效率反而不如repartition，因为stage并发度会受影响。