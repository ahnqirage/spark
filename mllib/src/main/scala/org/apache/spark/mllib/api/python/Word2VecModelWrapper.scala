/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.api.python

<<<<<<< HEAD
import java.util.{List => JList, Map => JMap}

=======
import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
import scala.collection.JavaConverters._

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
<<<<<<< HEAD
 * Wrapper around Word2VecModel to provide helper methods in Python
 */
=======
  * Wrapper around Word2VecModel to provide helper methods in Python
  */
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
private[python] class Word2VecModelWrapper(model: Word2VecModel) {
  def transform(word: String): Vector = {
    model.transform(word)
  }

  /**
   * Transforms an RDD of words to its vector representation
   * @param rdd an RDD of words
   * @return an RDD of vector representations of words
   */
  def transform(rdd: JavaRDD[String]): JavaRDD[Vector] = {
    rdd.rdd.map(model.transform)
  }

<<<<<<< HEAD
  /**
   * Finds synonyms of a word; do not include the word itself in results.
   * @param word a word
   * @param num number of synonyms to find
   * @return a list consisting of a list of words and a vector of cosine similarities
   */
  def findSynonyms(word: String, num: Int): JList[Object] = {
    prepareResult(model.findSynonyms(word, num))
  }

  /**
   * Finds words similar to the vector representation of a word without
   * filtering results.
   * @param vector a vector
   * @param num number of synonyms to find
   * @return a list consisting of a list of words and a vector of cosine similarities
   */
  def findSynonyms(vector: Vector, num: Int): JList[Object] = {
    prepareResult(model.findSynonyms(vector, num))
  }

  private def prepareResult(result: Array[(String, Double)]) = {
=======
  def findSynonyms(word: String, num: Int): JList[Object] = {
    val vec = transform(word)
    findSynonyms(vec, num)
  }

  def findSynonyms(vector: Vector, num: Int): JList[Object] = {
    val result = model.findSynonyms(vector, num)
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    val similarity = Vectors.dense(result.map(_._2))
    val words = result.map(_._1)
    List(words, similarity).map(_.asInstanceOf[Object]).asJava
  }

<<<<<<< HEAD

  def getVectors: JMap[String, JList[Float]] = {
    model.getVectors.map { case (k, v) =>
      (k, v.toList.asJava)
    }.asJava
=======
  def getVectors: JMap[String, JList[Float]] = {
    model.getVectors.map({case (k, v) => (k, v.toList.asJava)}).asJava
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }

  def save(sc: SparkContext, path: String): Unit = model.save(sc, path)
}
