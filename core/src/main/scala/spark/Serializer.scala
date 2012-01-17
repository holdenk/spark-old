package spark

import java.io.{InputStream, OutputStream}

/**
 * A serializer. Because some serialization libraries are not thread safe,
 * this class is used to create SerializerInstances that do the actual
 * serialization.
 */
trait Serializer {
  def newInstance(): SerializerInstance
}

/**
 * An instance of the serializer, for use by one thread at a time.
 */
trait SerializerInstance {
  def serialize[T](t: T): Array[Byte] =
    if (SparkEnv.get.eventReporter.measurePerformance) {
      val start = System.currentTimeMillis
      val result = serializeImpl(t)
      val end = System.currentTimeMillis
      SparkEnv.get.eventReporter.reportSerialization(end - start)
      result
    } else {
      serializeImpl(t)
    }

  def deserialize[T](bytes: Array[Byte]): T =
    if (SparkEnv.get.eventReporter.measurePerformance) {
      val start = System.currentTimeMillis
      val result = deserializeImpl(bytes)
      val end = System.currentTimeMillis
      SparkEnv.get.eventReporter.reportSerialization(end - start)
      result
    } else {
      deserializeImpl(bytes)
    }

  protected def serializeImpl[T](t: T): Array[Byte]
  protected def deserializeImpl[T](bytes: Array[Byte]): T

  def outputStream(s: OutputStream): SerializationStream
  def inputStream(s: InputStream): DeserializationStream
}

/**
 * A stream for writing serialized objects.
 */
trait SerializationStream {
  var totalSerializationTime = 0L

  def writeObject[T](t: T) {
    if (SparkEnv.get.eventReporter.measurePerformance) {
      val start = System.currentTimeMillis
      writeObjectImpl(t)
      val end = System.currentTimeMillis
      totalSerializationTime += end - start
    } else {
      writeObjectImpl(t)
    }
  }

  def close() {
    if (SparkEnv.get.eventReporter.measurePerformance)
      SparkEnv.get.eventReporter.reportSerialization(totalSerializationTime)
    closeImpl()
  }

  def writeObjectImpl[T](t: T): Unit
  def flush(): Unit
  def closeImpl(): Unit
}

/**
 * A stream for reading serialized objects.
 */
trait DeserializationStream {
  var totalSerializationTime = 0L

  def readObject[T](): T =
    if (SparkEnv.get.eventReporter.measurePerformance) {
      val start = System.currentTimeMillis
      val result = readObjectImpl[T]()
      val end = System.currentTimeMillis
      totalSerializationTime += end - start
      result
    } else {
      readObjectImpl[T]()
    }

  def close() {
    if (SparkEnv.get.eventReporter.measurePerformance)
      SparkEnv.get.eventReporter.reportSerialization(totalSerializationTime)
    closeImpl()
  }

  def readObjectImpl[T](): T
  def closeImpl(): Unit
}
