# Lab2 聚类与分类
## 聚类算法
### K-Means
对于K-Means的要求，实验指导中参考了CMU 10-605 2015 Spring的Homework要求，见`kmeans.pdf`文件。

利用Hadoop实现K-Means的主要思想：

两类不同的Mapper和Reducer，其中第一类Mapper和Reducer用于在随机确定初始化中心点（在本实验中，取前K条记录作为初始化中心点亦可），Mapper取前K条元素并将其全部转移到同一Reducer，在Reducer中将其转为`(cluster_id, center_data)`；
第二类Mapper和Reducer主要用于迭代，具体的做法可以参考`kmeans.pdf`中的作业提示。
