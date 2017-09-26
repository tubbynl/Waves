package scorex.transaction

import com.wavesplatform.features.{BlockchainFeatures, FeatureProvider}
import com.wavesplatform.settings.FunctionalitySettings
import com.wavesplatform.state2.reader.StateReader
import scorex.account.{Address, PublicKeyAccount}
import scorex.block.Block
import scorex.consensus.nxt.NxtLikeConsensusBlockData
import scorex.crypto.hash.FastCryptographicHash
import scorex.crypto.hash.FastCryptographicHash.hash
import scorex.utils.ScorexLogging

import scala.concurrent.duration.FiniteDuration

object PoSCalc extends ScorexLogging {

  val MinimalEffectiveBalanceForGenerator1: Long = 1000000000000L
  val MinimalEffectiveBalanceForGenerator2: Long = 100000000000L
  val AvgBlockTimeDepth: Int = 3

  def calcTarget(prevBlock: Block, timestamp: Long, balance: Long): BigInt = {
    val eta = (timestamp - prevBlock.timestamp) / 1000
    BigInt(prevBlock.consensusData.baseTarget) * eta * balance
  }

  def calcHit(lastBlockData: NxtLikeConsensusBlockData, generator: PublicKeyAccount): BigInt =
    BigInt(1, calcGeneratorSignature(lastBlockData, generator).take(8).reverse)

  def calcGeneratorSignature(lastBlockData: NxtLikeConsensusBlockData, generator: PublicKeyAccount): FastCryptographicHash.Digest =
    hash(lastBlockData.generationSignature.arr ++ generator.publicKey)

  def calcBaseTarget(avgBlockDelay: FiniteDuration, parentHeight: Int, parent: Block, greatGrandParent: Option[Block], timestamp: Long): Long = {
    val avgDelayInSeconds = avgBlockDelay.toSeconds

    def normalize(value: Long): Double = value * avgDelayInSeconds / (60: Double)

    val prevBaseTarget = parent.consensusData.baseTarget
    if (parentHeight % 2 == 0) {
      val blocktimeAverage = greatGrandParent.fold(timestamp - parent.timestamp) {
        b => (timestamp - b.timestamp) / AvgBlockTimeDepth
      } / 1000

      val minBlocktimeLimit = normalize(53)
      val maxBlocktimeLimit = normalize(67)
      val baseTargetGamma = normalize(64)
      val maxBaseTarget = Long.MaxValue / avgDelayInSeconds

      val baseTarget = (if (blocktimeAverage > avgDelayInSeconds) {
        prevBaseTarget * Math.min(blocktimeAverage, maxBlocktimeLimit) / avgDelayInSeconds
      } else {
        prevBaseTarget - prevBaseTarget * baseTargetGamma *
          (avgDelayInSeconds - Math.max(blocktimeAverage, minBlocktimeLimit)) / (avgDelayInSeconds * 100)
      }).toLong

      scala.math.min(baseTarget, maxBaseTarget)
    } else {
      prevBaseTarget
    }
  }

  def generatingBalance(state: StateReader, fs: FunctionalitySettings, account: Address, atHeight: Int): Long = {
    val generatingBalanceDepth = if (atHeight >= fs.generationBalanceDepthFrom50To1000AfterHeight) 1000 else 50
    state.effectiveBalanceAtHeightWithConfirmations(account, atHeight, generatingBalanceDepth)
  }

  def nextBlockGenerationTime(height: Int, state: StateReader, fs: FunctionalitySettings,
                              block: Block, account: PublicKeyAccount, featureProvider: FeatureProvider): Either[String, Long] = {
    val balance =generatingBalance(state, fs, account, height)
       Either.cond((!featureProvider.activated(BlockchainFeatures.SmallerMinimalGeneratingBalance) &&balance >= MinimalEffectiveBalanceForGenerator1) ||
          (featureProvider.activated(BlockchainFeatures.SmallerMinimalGeneratingBalance) && balance >= MinimalEffectiveBalanceForGenerator2),
      balance,    s"Balance $balance of ${account.address} is lower than required for generation")
       .flatMap { _ =>
         val cData = block.consensusData
         val hit = calcHit(cData, account)
         val t = cData.baseTarget
         val calculatedTs = (hit * 1000) / (BigInt(t) * balance) + block.timestamp
         if (0 < calculatedTs && calculatedTs < Long.MaxValue) {
           Right(calculatedTs.toLong)
         } else {
           Left( s"Invalid next block generation time: $calculatedTs")
         }
    }
  }
}
