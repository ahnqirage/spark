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

package org.apache.spark.sql

<<<<<<< HEAD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.storage.StorageLevel
=======
import scala.language.postfixOps

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284


class DatasetCacheSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

<<<<<<< HEAD
  test("get storage level") {
    val ds1 = Seq("1", "2").toDS().as("a")
    val ds2 = Seq(2, 3).toDS().as("b")

    // default storage level
    ds1.persist()
    ds2.cache()
    assert(ds1.storageLevel == StorageLevel.MEMORY_AND_DISK)
    assert(ds2.storageLevel == StorageLevel.MEMORY_AND_DISK)
    // unpersist
    ds1.unpersist()
    assert(ds1.storageLevel == StorageLevel.NONE)
    // non-default storage level
    ds1.persist(StorageLevel.MEMORY_ONLY_2)
    assert(ds1.storageLevel == StorageLevel.MEMORY_ONLY_2)
    // joined Dataset should not be persisted
    val joined = ds1.joinWith(ds2, $"a.value" === $"b.value")
    assert(joined.storageLevel == StorageLevel.NONE)
  }

  test("persist and unpersist") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS().select(expr("_2 + 1").as[Int])
=======
  test("persist and unpersist") {
    val ds = Seq(("a", 1) , ("b", 2), ("c", 3)).toDS().select(expr("_2 + 1").as[Int])
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    val cached = ds.cache()
    // count triggers the caching action. It should not throw.
    cached.count()
    // Make sure, the Dataset is indeed cached.
    assertCached(cached)
    // Check result.
<<<<<<< HEAD
    checkDataset(
=======
    checkAnswer(
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
      cached,
      2, 3, 4)
    // Drop the cache.
    cached.unpersist()
<<<<<<< HEAD
    assert(cached.storageLevel == StorageLevel.NONE, "The Dataset should not be cached.")
=======
    assert(!sqlContext.isCached(cached), "The Dataset should not be cached.")
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }

  test("persist and then rebind right encoder when join 2 datasets") {
    val ds1 = Seq("1", "2").toDS().as("a")
    val ds2 = Seq(2, 3).toDS().as("b")

    ds1.persist()
    assertCached(ds1)
    ds2.persist()
    assertCached(ds2)

    val joined = ds1.joinWith(ds2, $"a.value" === $"b.value")
<<<<<<< HEAD
    checkDataset(joined, ("2", 2))
    assertCached(joined, 2)

    ds1.unpersist()
    assert(ds1.storageLevel == StorageLevel.NONE, "The Dataset ds1 should not be cached.")
    ds2.unpersist()
    assert(ds2.storageLevel == StorageLevel.NONE, "The Dataset ds2 should not be cached.")
=======
    checkAnswer(joined, ("2", 2))
    assertCached(joined, 2)

    ds1.unpersist()
    assert(!sqlContext.isCached(ds1), "The Dataset ds1 should not be cached.")
    ds2.unpersist()
    assert(!sqlContext.isCached(ds2), "The Dataset ds2 should not be cached.")
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }

  test("persist and then groupBy columns asKey, map") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
<<<<<<< HEAD
    val grouped = ds.groupByKey(_._1)
    val agged = grouped.mapGroups { case (g, iter) => (g, iter.map(_._2).sum) }
    agged.persist()

    checkDataset(
=======
    val grouped = ds.groupBy($"_1").keyAs[String]
    val agged = grouped.mapGroups { case (g, iter) => (g, iter.map(_._2).sum) }
    agged.persist()

    checkAnswer(
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
      agged.filter(_._1 == "b"),
      ("b", 3))
    assertCached(agged.filter(_._1 == "b"))

    ds.unpersist()
<<<<<<< HEAD
    assert(ds.storageLevel == StorageLevel.NONE, "The Dataset ds should not be cached.")
    agged.unpersist()
    assert(agged.storageLevel == StorageLevel.NONE, "The Dataset agged should not be cached.")
=======
    assert(!sqlContext.isCached(ds), "The Dataset ds should not be cached.")
    agged.unpersist()
    assert(!sqlContext.isCached(agged), "The Dataset agged should not be cached.")
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }
}
