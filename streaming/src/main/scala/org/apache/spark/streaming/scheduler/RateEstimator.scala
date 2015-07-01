package org.apache.spark.streaming.scheduler

import org.apache.spark.streaming.Duration

/**
 *  A strategy to estimate the maximum rate at which InputDStreams should
 *  operate. The estimator should maximize rate while keeping processing time
 *  below the batch interval.
 */
trait RateEstimator {

  /**
   *  Estimate new input rates based on the given batch scheduling info.
   *
   *  @return A map from each DStream ID to a maximum rate (records/second)
   */
  def estimateNewRates(batchInfo: BatchInfo): Map[Int, Long]
}

/**
 * A PI-based estimator (PID without the Derived component).
 *
 * @see https://en.wikipedia.org/wiki/PID_controller
 *
 * @param proporionalK the proportional constant of the PID controller. A value close to 1.0
 *        is a good default.
 * @param integralK the integral constant of the PID. A value close to 0.0 is a good default.
 * @param batchDuration the duration of the batch interval.
 */
class PIDRateEstimator(proportionalK: Double, integralK: Double, batchDuration: Duration)
    extends RateEstimator {

  override def estimateNewRates(batchInfo: BatchInfo): Map[Int, Long] = {
    val workDelay = batchInfo.processingDelay
    val waitDelay = batchInfo.schedulingDelay

    val elements = batchInfo.streamIdToNumRecords
    // Percentage of a batch interval this job computed for
    val workRatio = workDelay.get.toDouble / batchDuration.milliseconds
    // Ratio of wait to work time
    val waitRatio = waitDelay.getOrElse(0L).toDouble / workDelay.get.toDouble

    elements.mapValues { elements =>
      // Based on this batch's size, how many elements this setup
      // can process given one batch interval of computation
      val elementsThisBatch = elements / workRatio

      // This computes how many elements could have been processed
      // at that speed, if the wait time had been compute time
      // (i.e. given an amount of time equal to the scheduling delay),
      // Because the scheduling delay increases synchronously for
      // every job, this represents the aggregation of the error
      // signal between elements flowing in and out - the integral
      // component of a PID.
      val waitElementsThisBatch = elements * waitRatio

      val batchElements = proportionalK * elementsThisBatch + integralK * waitElementsThisBatch

      // transform the number of batchElements into a rate (elements / second)
      val rate = 1000 * batchElements / batchDuration.milliseconds
      math.round(rate)
    }
  }
}