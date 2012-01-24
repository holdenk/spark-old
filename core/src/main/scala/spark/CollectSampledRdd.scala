package spark

import java.util.Random

class CollectSampledRDDSplit(val prev: Split, val seed: Int) extends Split with Serializable {
  override val index = prev.index
}

class CollectSampledRDD[T: ClassManifest](
  prev: RDD[T], withReplacement: Boolean, sampleSize: Int, seed: Int)
extends RDD[T](prev.context) {

  @transient val splits_ = { val rg = new Random(seed); prev.splits.map(x => new SampledRDDSplit(x, rg.nextInt)) }

  override def splits = splits_.asInstanceOf[Array[Split]]

  override val dependencies = List(new OneToOneDependency(prev))
  
  override def preferredLocations(split: Split) = prev.preferredLocations(split.asInstanceOf[SampledRDDSplit].prev)

  override def compute(splitIn: Split) = {
    val split = splitIn.asInstanceOf[SampledRDDSplit]
    val rg = new Random(split.seed);
    // Sampling with replacement (TODO: use reservoir sampling to make this more efficient?)
    if (withReplacement) {
      val oldData = prev.iterator(split.prev).toArray
      val sampledData = for (i <- 1 to sampleSize) yield oldData(rg.nextInt(oldData.size)) // all of oldData's indices are candidates, even if sampleSize < oldData.size
      sampledData.iterator
    }
    // Sampling without replacement
    else {
      val oldData = prev.iterator(split.prev).toArray
      //Note if sampleSize > oldData.size this is a problem
      if (sampleSize > oldData.size) {
      }
      val sampleData = new Array[T](sampleSize)
      for (i <- 1 to sampleSize) sampleData.update(i,oldData(i))
      for (i <- sampleSize to oldData.size) {
      	  val j = rg.nextInt(i)
	  if (j <= sampleSize) {
	     sampleData.update(j,oldData(i)) 
	  }
      }
      sampleData.iterator
    }
  }
}
