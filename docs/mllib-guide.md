---
layout: global
title: "MLlib: RDD-based API"
displayTitle: "MLlib: RDD-based API"
---

This page documents sections of the MLlib guide for the RDD-based API (the `spark.mllib` package).
Please see the [MLlib Main Guide](ml-guide.html) for the DataFrame-based API (the `spark.ml` package),
which is now the primary API for MLlib.

* [Data types](mllib-data-types.html)
* [Basic statistics](mllib-statistics.html)
  * [summary statistics](mllib-statistics.html#summary-statistics)
  * [correlations](mllib-statistics.html#correlations)
  * [stratified sampling](mllib-statistics.html#stratified-sampling)
  * [hypothesis testing](mllib-statistics.html#hypothesis-testing)
  * [streaming significance testing](mllib-statistics.html#streaming-significance-testing)
  * [random data generation](mllib-statistics.html#random-data-generation)
* [Classification and regression](mllib-classification-regression.html)
  * [linear models (SVMs, logistic regression, linear regression)](mllib-linear-methods.html)
  * [naive Bayes](mllib-naive-bayes.html)
  * [decision trees](mllib-decision-tree.html)
  * [ensembles of trees (Random Forests and Gradient-Boosted Trees)](mllib-ensembles.html)
  * [isotonic regression](mllib-isotonic-regression.html)
* [Collaborative filtering](mllib-collaborative-filtering.html)
  * [alternating least squares (ALS)](mllib-collaborative-filtering.html#collaborative-filtering)
* [Clustering](mllib-clustering.html)
  * [k-means](mllib-clustering.html#k-means)
  * [Gaussian mixture](mllib-clustering.html#gaussian-mixture)
  * [power iteration clustering (PIC)](mllib-clustering.html#power-iteration-clustering-pic)
  * [latent Dirichlet allocation (LDA)](mllib-clustering.html#latent-dirichlet-allocation-lda)
  * [bisecting k-means](mllib-clustering.html#bisecting-kmeans)
  * [streaming k-means](mllib-clustering.html#streaming-k-means)
* [Dimensionality reduction](mllib-dimensionality-reduction.html)
  * [singular value decomposition (SVD)](mllib-dimensionality-reduction.html#singular-value-decomposition-svd)
  * [principal component analysis (PCA)](mllib-dimensionality-reduction.html#principal-component-analysis-pca)
* [Feature extraction and transformation](mllib-feature-extraction.html)
* [Frequent pattern mining](mllib-frequent-pattern-mining.html)
  * [FP-growth](mllib-frequent-pattern-mining.html#fp-growth)
  * [association rules](mllib-frequent-pattern-mining.html#association-rules)
  * [PrefixSpan](mllib-frequent-pattern-mining.html#prefix-span)
* [Evaluation metrics](mllib-evaluation-metrics.html)
* [PMML model export](mllib-pmml-model-export.html)
* [Optimization (developer)](mllib-optimization.html)
  * [stochastic gradient descent](mllib-optimization.html#stochastic-gradient-descent-sgd)
  * [limited-memory BFGS (L-BFGS)](mllib-optimization.html#limited-memory-bfgs-l-bfgs)

<<<<<<< HEAD
=======
# spark.ml: high-level APIs for ML pipelines

* [Overview: estimators, transformers and pipelines](ml-guide.html)
* [Extracting, transforming and selecting features](ml-features.html)
* [Classification and regression](ml-classification-regression.html)
* [Clustering](ml-clustering.html)
* [Advanced topics](ml-advanced.html)

Some techniques are not available yet in spark.ml, most notably dimensionality reduction 
Users can seamlessly combine the implementation of these techniques found in `spark.mllib` with the rest of the algorithms found in `spark.ml`.

# Dependencies

MLlib uses the linear algebra package [Breeze](http://www.scalanlp.org/), which depends on
[netlib-java](https://github.com/fommil/netlib-java) for optimised numerical processing.
If natives libraries[^1] are not available at runtime, you will see a warning message and a pure JVM
implementation will be used instead.

Due to licensing issues with runtime proprietary binaries, we do not include `netlib-java`'s native
proxies by default.
To configure `netlib-java` / Breeze to use system optimised binaries, include
`com.github.fommil.netlib:all:1.1.2` (or build Spark with `-Pnetlib-lgpl`) as a dependency of your
project and read the [netlib-java](https://github.com/fommil/netlib-java) documentation for your
platform's additional installation instructions.

To use MLlib in Python, you will need [NumPy](http://www.numpy.org) version 1.4 or newer.

[^1]: To learn more about the benefits and background of system optimised natives, you may wish to
    watch Sam Halliday's ScalaX talk on [High Performance Linear Algebra in Scala](http://fommil.github.io/scalax14/#/).

# Migration guide

MLlib is under active development.
The APIs marked `Experimental`/`DeveloperApi` may change in future releases,
and the migration guide below will explain all changes between releases.

## From 1.5 to 1.6

There are no breaking API changes in the `spark.mllib` or `spark.ml` packages, but there are
deprecations and changes of behavior.

Deprecations:

* [SPARK-11358](https://issues.apache.org/jira/browse/SPARK-11358):
 In `spark.mllib.clustering.KMeans`, the `runs` parameter has been deprecated.
* [SPARK-10592](https://issues.apache.org/jira/browse/SPARK-10592):
 In `spark.ml.classification.LogisticRegressionModel` and
 `spark.ml.regression.LinearRegressionModel`, the `weights` field has been deprecated in favor of
 the new name `coefficients`.  This helps disambiguate from instance (row) "weights" given to
 algorithms.

Changes of behavior:

* [SPARK-7770](https://issues.apache.org/jira/browse/SPARK-7770):
 `spark.mllib.tree.GradientBoostedTrees`: `validationTol` has changed semantics in 1.6.
 Previously, it was a threshold for absolute change in error. Now, it resembles the behavior of
 `GradientDescent`'s `convergenceTol`: For large errors, it uses relative error (relative to the
 previous error); for small errors (`< 0.01`), it uses absolute error.
* [SPARK-11069](https://issues.apache.org/jira/browse/SPARK-11069):
 `spark.ml.feature.RegexTokenizer`: Previously, it did not convert strings to lowercase before
 tokenizing. Now, it converts to lowercase by default, with an option not to. This matches the
 behavior of the simpler `Tokenizer` transformer.

## Previous Spark versions

Earlier migration guides are archived [on this page](mllib-migration-guides.html).

---
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
