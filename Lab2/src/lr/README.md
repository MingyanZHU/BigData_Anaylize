# Logistic Regression MapReduce
- `LogisticRegression`是逻辑回归的主类
- `LogisticRegressionMapper`是逻辑回归的Mapper类，用于处理计算每一维度的这一次迭代变化的值。
- `LogisticRegressionReducer`是逻辑回归的Reducer类，用于更新w和b的每一维度。
- `LogisticRegressionTestMapper`是逻辑回归的测试用Mapper类，其中计算p1和p0的概率，返回较大的作为预测。
- `LogisticRegressionTestReducer`是逻辑回归测试用的Reducer类，此处使用与Naive Bayes使用相同的类进行测试即可。
- `LogisticRegressionTest`是用于测试LR的准确率的类，使用测试集。