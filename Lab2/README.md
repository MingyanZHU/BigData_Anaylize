# Lab2 聚类与分类
## 聚类算法
### K-Means
对于K-Means的要求，实验指导中参考了CMU 10-605 2015 Spring的Homework要求，见`kmeans.pdf`文件。

利用Hadoop实现K-Means的主要思想：

两类不同的Mapper和Reducer，其中第一类Mapper和Reducer用于在随机确定初始化中心点（在本实验中，取前K条记录作为初始化中心点亦可），Mapper取前K条元素并将其全部转移到同一Reducer，在Reducer中将其转为`(cluster_id, center_data)`；
第二类Mapper和Reducer主要用于迭代，具体的做法可以参考`kmeans.pdf`中的作业提示。


## 结果文件夹说明
- `KMenas_12_clusters` 是使用KMeans聚类算法，随机选择12个聚类中心，迭代两次优化函数的值变化小于1e-6时，得到的最终12个聚类中心的结果。同理，`KMeans_8_clusters`是8个聚类中心。
- `LR` 是Logistics Regression分类的结果，其中`lrOutput_m_*`为每次迭代得到的系数向量的每一维度的值；`lrTest_m_*`是在训练集上每次迭代后的结果，每一个类别正确分类数和错误分类数；`LR_m_*`为使用每次迭代的得到的系数向量在测试集上，得到的每一个类别正确分类数和错误分类数。