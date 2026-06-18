package DistributedFramework

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.unsafe.types.UTF8String

class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[scala.collection.mutable.HashMap[_, _]])
    kryo.register(classOf[scala.collection.mutable.HashSet[_]])
    kryo.register(classOf[scala.collection.mutable.ArrayBuffer[_]])
    kryo.register(classOf[scala.Tuple2[_, _]])
    kryo.register(classOf[scala.Tuple3[_, _, _]])
    kryo.register(classOf[scala.Tuple4[_, _, _, _]])
    kryo.register(classOf[scala.Tuple5[_, _, _, _, _]])
    kryo.register(classOf[Array[scala.Tuple2[_, _]]])
    kryo.register(classOf[Array[scala.Tuple3[_, _, _]]])
    kryo.register(classOf[Array[scala.Tuple4[_, _, _, _]]])
    kryo.register(classOf[Array[scala.Tuple5[_, _, _, _, _]]])

    kryo.register(classOf[Array[AnyRef]])
    kryo.register(classOf[Array[Int]])
    kryo.register(classOf[Array[Array[Int]]])

    kryo.register(classOf[OpenHashSet[_]])

    // ===== fastutil：我们后续会用到的容器 =====
    kryo.register(classOf[it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap[_]])
    kryo.register(classOf[it.unimi.dsi.fastutil.longs.LongOpenHashSet])
    kryo.register(classOf[it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap])
    kryo.register(classOf[it.unimi.dsi.fastutil.ints.IntArrayList])
    kryo.register(classOf[it.unimi.dsi.fastutil.ints.IntOpenHashSet])

    // immutable Map 常见实现（更稳）
    kryo.register(classOf[scala.collection.immutable.HashMap[_, _]])
    registerImmutableMapClasses(kryo)

    // ---- 关键补充：SQL Schema 相关（就是你报错的）----
    kryo.register(classOf[StructType])
    kryo.register(classOf[Array[StructType]])     // <<< 这行替代你之前的反射写法
    kryo.register(classOf[StructField])
    kryo.register(classOf[Array[StructField]])    // schema 里大量出现

    // 注册 Catalyst 内部类型
    kryo.register(classOf[InternalRow])
    kryo.register(classOf[Array[InternalRow]])
    kryo.register(classOf[UnsafeRow])

    // 注册底层数据类型
    kryo.register(classOf[UTF8String])
    kryo.register(classOf[Array[UTF8String]])

    // 注册元数据相关
    kryo.register(classOf[Metadata])
    kryo.register(Class.forName("org.apache.spark.sql.types.Metadata$"))

    // 注册Catalyst内部类型
    kryo.register(Class.forName("org.apache.spark.sql.catalyst.InternalRow"))
    kryo.register(Class.forName("[Lorg.apache.spark.sql.catalyst.InternalRow;"))
    kryo.register(Class.forName("org.apache.spark.sql.catalyst.expressions.SpecificInternalRow"))

    // 注册底层数据类型
    kryo.register(Class.forName("org.apache.spark.unsafe.types.UTF8String"))
    kryo.register(Class.forName("[Lorg.apache.spark.unsafe.types.UTF8String;"))

    // 其他常用基础
    kryo.register(classOf[Array[Double]])
    kryo.register(classOf[Array[Byte]])
    kryo.register(classOf[scala.collection.immutable.$colon$colon[_]]) // ::
    kryo.register(Class.forName("scala.collection.immutable.Nil$"))

    // 注册基础数据类型
    kryo.register(classOf[StringType])
    kryo.register(classOf[IntegerType])
    kryo.register(classOf[LongType])
    kryo.register(classOf[DoubleType])
    kryo.register(classOf[BooleanType])
    kryo.register(classOf[TimestampType])
    kryo.register(classOf[DateType])

    // Register the problematic class
    kryo.register(Class.forName("org.apache.spark.sql.execution.datasources.InMemoryFileIndex$SerializableFileStatus"))

    // You might need to register other classes that could cause similar issues
    kryo.register(Class.forName("scala.collection.mutable.ArrayBuffer"))

    // 除了SerializableFileStatus，还需要注册其他相关类
    kryo.register(Class.forName("org.apache.hadoop.fs.Path"))
    kryo.register(Class.forName("org.apache.hadoop.fs.LocatedFileStatus"))
    kryo.register(Class.forName("org.apache.hadoop.fs.FileStatus"))
    kryo.register(Class.forName("scala.collection.mutable.ArrayBuffer"))
    kryo.register(Class.forName("scala.collection.mutable.WrappedArray$ofRef"))

    // 注册 SerializableBlockLocation 类
    val blockLocationClass = Class.forName("org.apache.spark.sql.execution.datasources.InMemoryFileIndex$SerializableBlockLocation")
    kryo.register(blockLocationClass)


    // 注册 Hadoop 相关类
    kryo.register(Class.forName("org.apache.hadoop.fs.BlockLocation"))


    // ====== 新增数据源相关注册 ======
    // 1. 数据源写入核心类
    registerClassIfExists(kryo, "org.apache.spark.sql.execution.datasources.WriteTaskResult")
    registerClassIfExists(kryo, "[Lorg.apache.spark.sql.execution.datasources.WriteTaskResult;")

    // 2. 写入任务元数据
    registerClassIfExists(kryo, "org.apache.spark.sql.execution.datasources.ExecutedWriteSummary")
    registerClassIfExists(kryo, "org.apache.spark.sql.execution.datasources.WriteJobDescription")

    // 3. SQL 指标系统
    registerClassIfExists(kryo, "org.apache.spark.sql.execution.metric.SQLMetric")
    registerClassIfExists(kryo, "org.apache.spark.sql.execution.metric.SQLMetricInfo")
    registerClassIfExists(kryo, "org.apache.spark.sql.execution.metric.SQLMetricValue")

    // 4. 文件输出相关
    registerClassIfExists(kryo, "org.apache.spark.sql.execution.datasources.OutputWriter")
    registerClassIfExists(kryo, "org.apache.spark.sql.execution.datasources.OutputWriterFactory")
    registerClassIfExists(kryo, "org.apache.spark.sql.execution.datasources.FileFormat")

    // 5. 分区相关
    registerClassIfExists(kryo, "org.apache.spark.sql.catalyst.catalog.BucketSpec")
    registerClassIfExists(kryo, "org.apache.spark.sql.catalyst.expressions.Expression")
    registerClassIfExists(kryo, "org.apache.spark.sql.catalyst.expressions.NamedExpression")

    // ====== 写出 / 提交（commit）相关，统一补齐 ======

    // 0) Option/None（很多内部结构带 Option）
    kryo.register(classOf[scala.Some[_]])
    registerClassIfExists(kryo, "scala.None$")

    // 1) FileCommit 协议消息（你当前的报错就是这个）
    registerClassIfExists(kryo, "org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage")
    registerClassIfExists(kryo, "[Lorg.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage;")

    // 2) Write 结果与统计
    registerClassIfExists(kryo, "org.apache.spark.sql.execution.datasources.WriteTaskResult")
    registerClassIfExists(kryo, "[Lorg.apache.spark.sql.execution.datasources.WriteTaskResult;")
    registerClassIfExists(kryo, "org.apache.spark.sql.execution.datasources.WriteTaskStats")
    registerClassIfExists(kryo, "[Lorg.apache.spark.sql.execution.datasources.WriteTaskStats;")
    registerClassIfExists(kryo, "org.apache.spark.sql.execution.datasources.BasicWriteTaskStats")
    registerClassIfExists(kryo, "[Lorg.apache.spark.sql.execution.datasources.BasicWriteTaskStats;")

    // 3) Hadoop MapReduce Commit 协议在 Spark SQL 下的具体消息载荷
    registerClassIfExists(kryo, "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol$TaskCommitMessage")
    registerClassIfExists(kryo, "[Lorg.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol$TaskCommitMessage;")

    // 4) 写出的文件状态（写入总结里会带）
    registerClassIfExists(kryo, "org.apache.spark.sql.execution.datasources.SinkFileStatus")
    registerClassIfExists(kryo, "[Lorg.apache.spark.sql.execution.datasources.SinkFileStatus;")

    // 5) 常见 Hadoop 类型（路径等）
    registerClassIfExists(kryo, "org.apache.hadoop.fs.Path")
    registerClassIfExists(kryo, "[Lorg.apache.hadoop.fs.Path;")

    // 6) 可能出现的 WrappedArray 实现（有时出现在 schema/表达式里）
    registerClassIfExists(kryo, "scala.collection.mutable.WrappedArray$ofRef")
    registerClassIfExists(kryo, "scala.collection.mutable.WrappedArray$ofInt")

    // 7) 其他可能参与 commit 的辅助类（不同 Spark 版本可能存在/不存在——用 ifExists 安全注册）
    registerClassIfExists(kryo, "org.apache.spark.sql.execution.datasources.FileFormatWriter$TaskCommitMessage")
    registerClassIfExists(kryo, "[Lorg.apache.spark.sql.execution.datasources.FileFormatWriter$TaskCommitMessage;")
    registerClassIfExists(kryo, "org.apache.spark.sql.execution.datasources.FileFormatWriter$ExecutedWriteTaskStats")
    registerClassIfExists(kryo, "[Lorg.apache.spark.sql.execution.datasources.FileFormatWriter$ExecutedWriteTaskStats;")


    // ==== 关键：注册 Spark SQL DataType 单例对象（带 $ 的 module class）====
    @inline def regObj(obj: AnyRef): Unit = kryo.register(obj.getClass)

    // 常见标量类型（建议一次性注册完）
    regObj(StringType)
    regObj(BooleanType)
    regObj(ByteType)
    regObj(ShortType)
    regObj(IntegerType)    // 就是这行修掉你当前的 IntegerType$ 报错
    regObj(LongType)
    regObj(FloatType)
    regObj(DoubleType)
    regObj(BinaryType)
    regObj(DateType)
    regObj(TimestampType)
    regObj(NullType)
    // 如你用到小数，补这两行
    kryo.register(classOf[DecimalType])   // 非单例 case class
    // regObj(DecimalType)  //（可选）注册 object 本身

    // 其他常用基础（你已有）
    kryo.register(classOf[Array[Double]])
    kryo.register(classOf[Array[Byte]])
    kryo.register(classOf[scala.collection.immutable.$colon$colon[_]])
    kryo.register(Class.forName("scala.collection.immutable.Nil$"))

    // 不可变 Map 的实现（你已有）
    registerImmutableMapClasses(kryo)

  }

  private def registerClassIfExists(kryo: Kryo, className: String): Unit = {
    try {
      val cls = Class.forName(className)
      kryo.register(cls)
      println(s"成功注册类: $className")
    } catch {
      case _: ClassNotFoundException =>
        println(s"警告: 未找到类 $className，跳过注册")
    }
  }

  private def registerImmutableMapClasses(kryo: Kryo): Unit = {
    def reg(n: String): Unit = try kryo.register(Class.forName(n)) catch { case _: ClassNotFoundException => () }
    reg("scala.collection.immutable.Map$EmptyMap$")
    reg("scala.collection.immutable.Map$Map1")
    reg("scala.collection.immutable.Map$Map2")
    reg("scala.collection.immutable.Map$Map3")
    reg("scala.collection.immutable.Map$Map4")
  }
}
