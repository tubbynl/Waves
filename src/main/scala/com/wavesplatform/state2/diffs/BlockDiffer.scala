package com.wavesplatform.state2.diffs

import cats.Monoid
import cats.implicits._
import cats.kernel.Semigroup
import com.wavesplatform.settings.FunctionalitySettings
import com.wavesplatform.state2._
import com.wavesplatform.state2.patch.LeasePatch
import com.wavesplatform.state2.reader.{CompositeStateReader, StateReader}
import scorex.account.Address
import scorex.block.{Block, MicroBlock}
import scorex.transaction.{Signed, Transaction, ValidationError}
import scorex.utils.ScorexLogging

object BlockDiffer extends ScorexLogging {

  def right(diff: Diff): Either[ValidationError, Diff] = Right(diff)

  def fromBlock(settings: FunctionalitySettings, s: StateReader, maybePrevBlock: Option[Block], block: Block): Either[ValidationError, BlockDiff] = {
    val blockSigner = block.signerData.generator.toAddress
    val stateHeight = s.height

    lazy val prevBlockFeeDistr: Option[Diff] =
      if (stateHeight > settings.ng4060switchHeight)
        maybePrevBlock
          .map(prevBlock => Diff.empty.copy(
            portfolios = Map(blockSigner -> prevBlock.prevBlockFeePart)))
      else None

    lazy val currentBlockFeeDistr =
      if (stateHeight < settings.ng4060switchHeight)
        Some(Diff.empty.copy(portfolios = Map(blockSigner -> block.feesDistribution)))
      else
        None

    val prevBlockTimestamp = maybePrevBlock.map(_.timestamp)
    for {
      _ <- Signed.validateSignatures(block)
      r <- apply(settings, s, prevBlockTimestamp)(block.signerData.generator, prevBlockFeeDistr, currentBlockFeeDistr, block.timestamp, block.transactionData, 1)
    } yield r
  }

  def fromMicroBlock(settings: FunctionalitySettings, s: StateReader, pervBlockTimestamp: Option[Long], micro: MicroBlock, timestamp: Long): Either[ValidationError, BlockDiff] = {
    for {
      _ <- Signed.validateSignatures(micro)
      r <- apply(settings, s, pervBlockTimestamp)(micro.generator, None, None, timestamp, micro.transactionData, 0)
    } yield r
  }

  def unsafeDiffMany(settings: FunctionalitySettings, s: StateReader, prevBlock: Option[Block])(blocks: Seq[Block]): BlockDiff =
    blocks.foldLeft((Monoid[BlockDiff].empty, prevBlock)) { case ((diff, prev), block) =>
      val blockDiff = fromBlock(settings, new CompositeStateReader(s, diff), prev, block).explicitGet()
      (Monoid[BlockDiff].combine(diff, blockDiff), Some(block))
    }._1

  private def apply(settings: FunctionalitySettings, s: StateReader, pervBlockTimestamp: Option[Long])
                   (blockGenerator: Address, prevBlockFeeDistr: Option[Diff], maybeFeesDistr: Option[Diff], timestamp: Long, txs: Seq[Transaction], heightDiff: Int): Either[ValidationError, BlockDiff] = {
    val currentBlockHeight = s.height + heightDiff
    val txDiffer = TransactionDiffer(settings, pervBlockTimestamp, timestamp, currentBlockHeight) _

    val txsDiffEi = maybeFeesDistr match {
      case Some(feedistr) =>
        txs.foldLeft(right(Monoid.combine(prevBlockFeeDistr.orEmpty, feedistr))) { case (ei, tx) => ei.flatMap(diff =>
          txDiffer(new CompositeStateReader(s, diff.asBlockDiff), tx)
            .map(newDiff => diff.combine(newDiff)))
        }
      case None =>
        txs.foldLeft(right(prevBlockFeeDistr.orEmpty)) { case (ei, tx) => ei.flatMap(diff =>
          txDiffer(new CompositeStateReader(s, diff.asBlockDiff), tx)
            .map(newDiff => diff.combine(newDiff.copy(portfolios = newDiff.portfolios.combine(Map(blockGenerator -> tx.feeDiff()).mapValues(_.multiply(Block.CurrentBlockFeePart)))))))
        }
    }

    txsDiffEi.map { d =>
      val diff = if (currentBlockHeight == settings.resetEffectiveBalancesAtHeight)
        Monoid.combine(d, LeasePatch(new CompositeStateReader(s, d.asBlockDiff)))
      else d

      implicit val g: Semigroup[Map[ByteStr, Long]] = (x: Map[ByteStr, Long], y: Map[ByteStr, Long]) =>
        x.keySet.map { k =>
          val sum = safeSum(x.getOrElse(k, 0L), y.getOrElse(k, 0L))
          require(sum >= 0, s"Negative balance $sum for asset X'${BigInt(k.arr).toString(16)}', available: ${y.getOrElse(k, 0L)}")
          k -> sum
        }.toMap

      val newSnapshots = diff.portfolios
        .map { case (acc, portfolioDiff) =>
          val oldPortfolio = s.wavesBalance(acc)
          val newWavesBalance = if (portfolioDiff.balance != 0 || portfolioDiff.effectiveBalance != 0)
            Some(WavesBalance(oldPortfolio.regularBalance + portfolioDiff.balance,
              oldPortfolio.effectiveBalance + portfolioDiff.effectiveBalance))
            else None
          val assetBalances: Map[ByteStr, Long] = if (portfolioDiff.assets.isEmpty) Map.empty else {
            Semigroup.combine(portfolioDiff.assets, s.assetBalance(acc))(g)
          }
          acc -> Snapshot(newWavesBalance, assetBalances = assetBalances)
        }
      BlockDiff(diff, heightDiff, newSnapshots)
    }
  }
}
