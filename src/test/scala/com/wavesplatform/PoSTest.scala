package com.wavesplatform

import java.io.File
import java.util
import java.util.concurrent.locks.ReentrantReadWriteLock

import cats.kernel.Monoid
import com.wavesplatform.settings.{FunctionalitySettings, GenesisSettings, GenesisTransactionSettings}
import com.wavesplatform.state2.reader.StateReader
import com.wavesplatform.state2.{AssetInfo, ByteStr, LeaseInfo, OrderFillInfo, Portfolio, Snapshot}
import scorex.account.{Address, Alias, PrivateKeyAccount}
import scorex.block.Block
import scorex.consensus.nxt.NxtLikeConsensusBlockData
import scorex.transaction.PoSCalc.{calcBaseTarget, calcGeneratorSignature}
import scorex.transaction.lease.LeaseTransaction
import scorex.transaction.{PoSCalc, Transaction}

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.Random

object PoSTest {
  val balances = Seq.empty[Long]

  class TestState(
       val accounts: Map[PrivateKeyAccount, Long],
       val height: Int,
       previousBlocks: Seq[Block] = Seq.empty) extends StateReader {
    def appendBlock(b: Block) = new TestState(accounts, height + 1, b +: previousBlocks.take(1))
    val greatGrandParent = if (previousBlocks.size < 2) None else previousBlocks.lastOption

    override val accountPortfolios: Map[Address, Portfolio] = accounts.map {
      case (pk, balance) => pk.toAddress -> Portfolio(balance, LeaseInfo.empty, Map.empty)
    }

    override def accountPortfolio(a: Address): Portfolio = accountPortfolios.getOrElse(a, Monoid.empty[Portfolio])
    override def transactionInfo(id: ByteStr): Option[(Int, Transaction)] = None
    override def containsTransaction(id: ByteStr): Boolean = false
    override def assetInfo(id: ByteStr): Option[AssetInfo] = None
    override def accountTransactionIds(a: Address, limit: Int): Seq[ByteStr] = Seq.empty
    override def paymentTransactionIdByHash(hash: ByteStr): Option[ByteStr] = None
    override def aliasesOfAddress(a: Address): Seq[Alias] = Seq.empty
    override def resolveAlias(a: Alias): Option[Address] = None
    override def isLeaseActive(leaseTx: LeaseTransaction): Boolean = false
    override def activeLeases(): Seq[ByteStr] = Seq.empty
    override def lastUpdateHeight(acc: Address): Option[Int] = Some(1)
    override def snapshotAtHeight(acc: Address, h: Int): Option[Snapshot] =
      Some(Snapshot(0, accountPortfolio(acc).balance, accountPortfolio(acc).balance))

    override def filledVolumeAndFee(orderId: ByteStr): OrderFillInfo = ???
    override def synchronizationToken: ReentrantReadWriteLock = new ReentrantReadWriteLock(true)
  }

  def mkAddress: PrivateKeyAccount = {
    val bytes = new Array[Byte](32)
    Random.nextBytes(bytes)
    PrivateKeyAccount(bytes)
  }

  def nextBlock(state: TestState, previousBlock: Block): (TestState, Block) = {
    val zz = state.accounts.keys.flatMap { account =>
      PoSCalc.nextBlockGenerationTime(state.height, state, FunctionalitySettings.MAINNET, previousBlock, account)
        .toOption
        .map(ts => account -> math.ceil(ts / 1e3).toLong * 1000)
    }

    val (_, nextTs) = zz.minBy(_._2)
    val possibleGenerators = zz.filter(_._2 == nextTs)
    val nextGenerator = Random.shuffle(possibleGenerators).head._1

    val btg = calcBaseTarget(GenesisSettings.MAINNET.averageBlockDelay, state.height, previousBlock, state.greatGrandParent, nextTs)
    val gs = calcGeneratorSignature(previousBlock.consensusData, nextGenerator)
    val consensusData = NxtLikeConsensusBlockData(btg, gs)
    val newBlock = Block.buildAndSign(2, nextTs, previousBlock.uniqueId, consensusData, Seq.empty, nextGenerator)
    val newState = state.appendBlock(newBlock)

    newState -> newBlock
  }

  def main(args: Array[String]): Unit = {
    val totalBlocks = args(0).toInt

    val balances = Source
      .fromFile(new File(args(1)))
      .getLines()
      .map(_.trim.replace(".", ""))
      .filterNot(_.isEmpty)
      .map(_.toLong)
      .toSeq

    require(balances.nonEmpty)

    val accounts = balances.map(b => mkAddress -> b)
    val genesisSettings = GenesisSettings.MAINNET.copy(
      blockTimestamp = System.currentTimeMillis() / 1000 * 1000,
      timestamp = System.currentTimeMillis() / 1000 * 1000,
      initialBalance = balances.sum,
      signature = None,
      transactions = accounts.map {
        case (pk, balance) => GenesisTransactionSettings(pk.address, balance)
      }
    )

    val genesisBlock = Block.genesis(genesisSettings)
    val initialState = new TestState(accounts.toMap, 1)

    val generatorStats = new util.HashMap[Address, Long]()

    (1 to totalBlocks).foldLeft((initialState, genesisBlock.right.get)) {
      case ((s, b), i) =>
        val (ns, nb) = nextBlock(s, b)

        if (i > 1000) {
          generatorStats.compute(nb.signerData.generator.toAddress, (_, count) => count + 1)
        }

        (ns, nb)
    }

    val accountMap = accounts.map {
      case (a, b) => a.toAddress -> BigDecimal(b) / genesisSettings.initialBalance
    }.toMap

    println("blockCount,blockShare,balanceShare,performance")
    println(generatorStats.asScala.toSeq.sortBy(_._2).map {
      case (address, blockCount) =>
        val blockShare = BigDecimal(blockCount) / totalBlocks
        val balanceShare = accountMap.getOrElse(address, BigDecimal(0))
        f"$blockCount,$blockShare%.6f,$balanceShare%.6f,${if (balanceShare != 0) blockShare / balanceShare else BigDecimal(0)}%.6f"
    }.reverse.mkString("\n"))
  }
}
