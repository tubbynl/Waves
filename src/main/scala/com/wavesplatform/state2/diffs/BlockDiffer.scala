package com.wavesplatform.state2.diffs

import cats.Monoid
import cats.implicits._
import com.wavesplatform.settings.FunctionalitySettings
import com.wavesplatform.state2._
import com.wavesplatform.state2.patch.LeasePatch
import com.wavesplatform.state2.reader.{CompositeStateReader, StateReader}
import scorex.account.Address
import scorex.block.{Block, MicroBlock}
import scorex.transaction.{Signed, Transaction, ValidationError}
import scorex.utils.ScorexLogging

import scala.collection.SortedMap

object BlockDiffer extends ScorexLogging with Instrumented {

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
        Some(Diff.empty.copy(portfolios = Map(blockSigner -> block.feesPortfolio)))
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
      val newSnapshots = diff.portfolios
        .collect { case (acc, portfolioDiff) if portfolioDiff.balance != 0 || portfolioDiff.effectiveBalance != 0 =>
          val oldPortfolio = s.wavesBalance(acc)
          acc -> SortedMap(currentBlockHeight -> Snapshot(
            prevHeight = s.lastUpdateHeight(acc).getOrElse(0),
            balance = oldPortfolio.balance + portfolioDiff.balance,
            effectiveBalance = oldPortfolio.effectiveBalance + portfolioDiff.effectiveBalance))
        }
      BlockDiff(diff, heightDiff, newSnapshots)
    }
  }
}
