# 大数据分析大作业

## BackGround

### Content Addressable Network(CAN)

CAN是一种自组织性很强的P2P覆盖网络，一个$d$维的CAN的划分是将一个$d$维的坐标系划分到不同的节点上去。数据是通过一个$hash$函数来将数据的主键划分为$d$维的坐标，从而划分数据到不同的CAN节点上的。对于一个CAN节点$N_i$来说，它会保存所有和$N_i$对应的空间的邻接空间的ip地址，从而形成一个P2P网络。因为CAN网络的自组织性很强，所以一个节点可以很自由地加入或者离开一个CAN，就像P2P网络一样，只需要发送信息更改网络拓扑即可。

我们使用的RT-CAN索引的构建是基于一种CAN的变种，$C^2$来实现的。$C^2$除了有CAN的特征外，它还在划分坐标系的每个维度上添加了和弦邻居链接(Chord-like neighbor link)。具体而言，在每个维度上距离为$2^0,2^1,\cdots$的节点上添加链接。因为我们要实现的是分布式空间近似关键字查询，所以我们选择中2维的$C^2$来实现的RT-CAN索引。

我们要处理的是空间近似关键字查询以及$KNN$，$RkNN$近似关键字查询，所以我们的数据的key值就可以看作是2维的位置坐标，这样很自然的就可以想到对CAN的划分也是在2维坐标中而且可以和位置信息对应起来，即$hash$函数不对输入进行转换。

## 数据划分



## 第一层索引

我们使用RT-CAN索引作为我们的第一层索引，即寻找存储节点的索引。RT-CAN索引是一种建立在本地索引之上的索引[1]。它使用$C^2$网络作为底层存储节点分布，通过定义R树数据节点如何划分到各个存储节点来得到数据的组织方式，并且定义相关算法能对其中的数据进行查询。下面首先介绍RT-CAN节点的结构，然后介绍如何构造RT-CAN索引。

### 节点结构

RT-CAN索引是建立在一个shared-nothing的集群上的。如图1所示，集群中的每一个节点$N_i$都包含了两个部分：一个是存储节点$N_{si}$，另一个是覆盖节点$N_{oi}$。$N_{si}$表示的是分布式存储的特征，它存储着所有数据划分的一部分。为了满足空间近似查询，以及空间近似的$kNN$和$RkNN$查询，$N_{si}$使用了一种R树的变种来存储局部数据，从而满足我们的查询需求。$N_{oi}$是用来表示CAN结构化覆盖的部分，它负责的是CAN划分的一部分。对于CAN的网络通信来说，$N_{si}$会适应性的选择一部分局部R树的结点，然后通过$N_{oi}$来将这些节点信息发送到CAN网络中。发送的信息结果为一个二元组$(ip,mbr)$，其中ip是$N_i$节点的IP地址，$mbr$是这个R树结点的范围。当$N{si}$收到$N_{oi}$的发送请求时，$N{si}$就会选择相应的R树结点并将其map成为一个CAN节点，并通过CAN的路由协议将请求发出。$N_{oi}$维护着全局索引，当它收到一个广播请求，它就通过map方法判断这是否是它要接受的请求。如果时，它就保留一份广播的R树的结点当作索引并保存。这样就能做到用一些R树的结点来当作索引并将其分布在集群中。

![图1](figure1.png)

### 索引构造

基于我们的需求，我们使用的是二维的$C^2$来构建我们的RT-CAN索引。前面提到我们需要一个map方法将一个R树结点map为一个CAN节点，这样的map方法一般要以这个R树节点的中心和半径来确定。对一个二维的R树节点$n$，范围为$[l_1,u_1],[l_2,u_2]$，中心和半径分别表示为$c_n=(\frac{l_1+u_1}{2}),r_n=\frac{1}{2}\sqrt{(u_1-l_1)^2+(u_2-l_2)^2}$。首先，对于R树节点$n$，我们首先把它map到包含$n$的中心$c_n$的CAN节点$N_c$上，然后$N_c$会比较$n$半径和定义的一阈值参数$R_{max}$，如果$n$的半径小于$R_{max}$，就只需要map给这一个CAN节点，否则就需要将$n$发送给所有和$n$范围覆盖的所有CAN节点。这样可能会导致一些副本的出现，但同时也会提升查询的效率，因为只保存一个索引会导致所有相关的搜索都要在网络中查询这个索引，会降低查询效率。

然后，对于索引的构造，对于每个CAN节点，若假设其存储的R树是$L$层的，我们就选择将R树的$L-1$层的所有R树节点发送，因为它们不是经常被更新的，这样就减少了更新索引的次数。然后对每个CAN节点都执行发送的操作，然后就可以按照我们前面提到的map算法来判断如何构造我们的全局索引。

## 查询

对于RT-CAN索引来说，我们能得到下面两个定理。

**定理1**：对一个点查询$Q(key)$，如果我们查询了所有以$key$为圆心，$R_{max}$为半径的的圆覆盖的所有的CAN，那么我们就一能得到完整的结果。

**定理2**：对于一个范围查询$Q(range)$，如果我们查询了所有以$range$的中心为圆心，$R_{max}+range.radius$为半径的源覆盖的所有的CAN，那么我们就一定能得到完整的结果。

所以点查询和范围查询的区别就在查询的半径不同。以点查询为例，首先，我们需要将查询发给一个$N_{init}$，它的范围必须包含我们要查询的点$key$，然后查询$N_{init}$的全局索引，返回一个结果集合；这个CAN查询完后就开始将查询

## 参考文献

[1] Wang J, Wu S, Gao H, et al. Indexing multi-dimensional data in a cloud system[C]// Acm Sigmod International Conference on Management of Data. 2010.