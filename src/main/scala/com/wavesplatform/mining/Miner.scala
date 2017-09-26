package com.wavesplatform.mining

import java.util.concurrent.atomic.AtomicBoolean

import com.wavesplatform.features.{FeatureProvider, FeatureStatus}
import com.wavesplatform.metrics.BlockStats
import com.wavesplatform.network._
import com.wavesplatform.settings.WavesSettings
import com.wavesplatform.state2._
import com.wavesplatform.state2.reader.StateReader
import com.wavesplatform.{Coordinator, UtxPool}
import io.netty.channel.group.ChannelGroup
import kamon.Kamon
import kamon.metric.instrument
import monix.eval.Task
import monix.execution._
import monix.execution.cancelables.{CompositeCancelable, SerialCancelable}
import monix.execution.schedulers.SchedulerService
import scorex.account.{Address, PrivateKeyAccount}
import scorex.block.Block._
import scorex.block.{Block, MicroBlock}
import scorex.consensus.nxt.NxtLikeConsensusBlockData
import scorex.transaction.PoSCalc._
import scorex.transaction._
import scorex.utils.{ScorexLogging, Time}
import scorex.wallet.Wallet

import scala.collection.mutable.{Map => MMap}
import scala.concurrent.Await
import scala.concurrent.duration._

class Miner(
             allChannels: ChannelGroup,
             blockchainReadiness: AtomicBoolean,
             blockchainUpdater: BlockchainUpdater,
             checkpoint: CheckpointService,
             history: NgHistory,
             featureProvider: FeatureProvider,
             stateReader: StateReader,
             settings: WavesSettings,
             timeService: Time,
             utx: UtxPool,
             wallet: Wallet) extends MinerDebugInfo with ScorexLogging with Instrumented {

  import Miner._

  private implicit val scheduler: SchedulerService = Scheduler.fixedPool(name = "miner-pool", poolSize = 2)

  private lazy val minerSettings = settings.minerSettings
  private lazy val minMicroBlockDurationMills = minerSettings.minMicroBlockAge.toMillis
  private lazy val blockchainSettings = settings.blockchainSettings
  private lazy val processBlock = Coordinator.processSingleBlock(checkpoint, history, blockchainUpdater, timeService, stateReader, utx, blockchainReadiness, settings, featureProvider) _

  private val scheduledAttempts = SerialCancelable()
  private val microBlockAttempt = SerialCancelable()

  private val blockBuildTimeStats = Kamon.metrics.histogram("pack-and-forge-block-time", instrument.Time.Milliseconds)
  private val microBlockBuildTimeStats = Kamon.metrics.histogram("forge-microblock-time", instrument.Time.Milliseconds)

  private val nextBlockGenerationTimes: MMap[Address, Long] = MMap.empty

  def collectNextBlockGenerationTimes: List[(Address, Long)] = Await.result(Task.now(nextBlockGenerationTimes.toList).runAsync, Duration.Inf)

  private def checkAge(parentHeight: Int, parentTimestamp: Long): Either[String, Unit] =
    Either.cond(parentHeight == 1, (), (timeService.correctedTime() - parentTimestamp).millis)
      .left.flatMap(blockAge => Either.cond(blockAge <= minerSettings.intervalAfterLastBlockThenGenerationIsAllowed, (),
      s"BlockChain is too old (last block timestamp is $parentTimestamp generated $blockAge ago)"
    ))

  private def generateOneBlockTask(version: Int, account: PrivateKeyAccount, parentHeight: Int,
                                   greatGrandParent: Option[Block], balance: Long)(delay: FiniteDuration): Task[Either[String, Block]] = Task {
    // should take last block right at the time of mining since microblocks might have been added
    // the rest doesn't change
    val parent = history.bestLastBlock(System.currentTimeMillis() - minMicroBlockDurationMills).get
    val pc = allChannels.size()
    lazy val lastBlockKernelData = parent.consensusData
    lazy val currentTime = timeService.correctedTime()
    lazy val h = calcHit(lastBlockKernelData, account)
    lazy val t = calcTarget(parent, currentTime, balance)
    measureSuccessful(blockBuildTimeStats, for {
      _ <- Either.cond(pc >= minerSettings.quorum, (), s"Quorum not available ($pc/${minerSettings.quorum}, not forging block with ${account.address}")
      _ <- Either.cond(h < t, (), s"${System.currentTimeMillis()}: Hit $h was NOT less than target $t, not forging block with ${account.address}")
      _ = log.debug(s"Forging with ${account.address}, H $h < T $t, balance $balance, prev block ${parent.uniqueId}")
      _ = log.debug(s"Previous block ID ${parent.uniqueId} at $parentHeight with target ${lastBlockKernelData.baseTarget}")
      block <- {
        val avgBlockDelay = blockchainSettings.genesisSettings.averageBlockDelay
        val btg = calcBaseTarget(avgBlockDelay, parentHeight, parent, greatGrandParent, currentTime)
        val gs = calcGeneratorSignature(lastBlockKernelData, account)
        val consensusData = NxtLikeConsensusBlockData(btg, ByteStr(gs))
        val sortInBlock = history.height() <= blockchainSettings.functionalitySettings.dontRequireSortedTransactionsAfter
        val unconfirmed = utx.packUnconfirmed(minerSettings.maxTransactionsInKeyBlock, sortInBlock)
        val features = settings.featuresSettings.supported
          .filter(featureProvider.status(_) == FeatureStatus.Defined).toSet
        log.debug(s"Adding ${unconfirmed.size} unconfirmed transaction(s) to new block")
        Block.buildAndSign(version.toByte, currentTime, parent.uniqueId, consensusData, unconfirmed, account, features)
          .left.map(l => l.err)
      }
    } yield block)
  }.delayExecution(delay)


  private def generateOneMicroBlockTask(account: PrivateKeyAccount, accumulatedBlock: Block): Task[Either[ValidationError, Option[Block]]] = Task {
    log.trace(s"Generating microblock for $account")
    val pc = allChannels.size()
    lazy val unconfirmed = measureLog("packing unconfirmed transactions for microblock") {
      utx.packUnconfirmed(settings.minerSettings.maxTransactionsInMicroBlock, sortInBlock = false)
    }
    if (pc < minerSettings.quorum) {
      log.trace(s"Quorum not available ($pc/${minerSettings.quorum}, not forging microblock with ${account.address}")
      Right(None)
    }
    else if (unconfirmed.isEmpty) {
      log.trace("skipping microBlock because no txs in utx pool")
      Right(None)
    }
    else {
      log.trace(s"Accumulated ${unconfirmed.size} txs for microblock")
      val start = System.currentTimeMillis()
      val block = for {
        signedBlock <- Block.buildAndSign(
          version = 3,
          timestamp = accumulatedBlock.timestamp,
          reference = accumulatedBlock.reference,
          consensusData = accumulatedBlock.consensusData,
          transactionData = accumulatedBlock.transactionData ++ unconfirmed,
          signer = account
        )
        microBlock <- MicroBlock.buildAndSign(account, unconfirmed, accumulatedBlock.signerData.signature, signedBlock.signerData.signature)
        _ = microBlockBuildTimeStats.safeRecord(System.currentTimeMillis() - start)
        _ <- Coordinator.processMicroBlock(checkpoint, history, blockchainUpdater, utx)(microBlock)
      } yield {
        BlockStats.mined(microBlock)
        log.trace(s"MicroBlock(id=${trim(microBlock.uniqueId)}) has been mined for $account}")
        allChannels.broadcast(MicroBlockInv(microBlock.totalResBlockSig, microBlock.prevResBlockSig))
        Some(signedBlock)
      }
      block.left.map { err =>
        log.trace(s"MicroBlock has NOT been mined for $account} because $err")
        err
      }
    }
  }.delayExecution(minerSettings.microBlockInterval)

  private def generateMicroBlockSequence(account: PrivateKeyAccount, accumulatedBlock: Block): Task[Unit] =
    generateOneMicroBlockTask(account, accumulatedBlock).flatMap {
      case Left(err) => Task(log.warn("Error mining MicroBlock: " + err.toString))
      case Right(maybeNewTotal) => generateMicroBlockSequence(account, maybeNewTotal.getOrElse(accumulatedBlock))
    }

  private def generateBlockTask(account: PrivateKeyAccount): Task[Unit] = {
    val height = history.height()
    val lastBlock = history.lastBlock.get
    val grandParent = history.parent(lastBlock, 2)
    (for {
      _ <- checkAge(height, history.lastBlockTimestamp().get)
      ts <- nextBlockGenerationTime(height, stateReader, blockchainSettings.functionalitySettings, lastBlock, account, featureProvider)
      offset = calcOffset(timeService, ts, minerSettings.minimalBlockGenerationOffset)
      balance = generatingBalance(stateReader, blockchainSettings.functionalitySettings, account, height)
    } yield (offset, balance)) match {
      case Right((offset, balance)) =>
        log.debug(s"Next attempt for acc=$account in $offset")
        val microBlocksEnabled = history.height() > blockchainSettings.functionalitySettings.enableMicroblocksAfterHeight
        val version = if (height <= blockchainSettings.functionalitySettings.blockVersion3After) PlainBlockVersion else NgBlockVersion
        nextBlockGenerationTimes += account.toAddress -> (System.currentTimeMillis() + offset.toMillis)
        generateOneBlockTask(version, account, height, grandParent, balance)(offset).flatMap {
          case Right(block) => Task.now {
            processBlock(block, true) match {
              case Left(err) => log.warn("Error mining Block: " + err.toString)
              case Right(score) =>
                allChannels.broadcast(LocalScoreChanged(score))
                allChannels.broadcast(BlockForged(block))
                scheduleMining()
                if (microBlocksEnabled)
                  startMicroBlockMining(account, block)
            }
          }
          case Left(err) =>
            log.debug(s"No block generated because $err, retrying")
            generateBlockTask(account)
        }
      case Left(err) =>
        log.debug(s"Not scheduling block mining because $err")
        Task.unit
    }
  }

  def scheduleMining(): Unit = {
    Miner.blockMiningStarted.increment()
    scheduledAttempts := CompositeCancelable.fromSet(
      wallet.privateKeyAccounts().map(generateBlockTask).map(_.runAsync).toSet)
    microBlockAttempt := SerialCancelable()
    log.debug(s"Block mining scheduled")
  }

  def stopMicroblockMining(): Unit = {
    microBlockAttempt := SerialCancelable()
    log.debug(s"Microblock mining was stopped")
  }

  private def startMicroBlockMining(account: PrivateKeyAccount, lastBlock: Block): Unit = {
    Miner.microMiningStarted.increment()
    microBlockAttempt := generateMicroBlockSequence(account, lastBlock).runAsync
    log.trace(s"MicroBlock mining scheduled for $account")
  }
}

object Miner extends ScorexLogging {

  val MaxTransactionsPerMicroblock: Int = 5000

  def calcOffset(timeService: Time, calculatedTimestamp: Long, minimalBlockGenerationOffset: FiniteDuration): FiniteDuration = {
    val calculatedGenerationTimestamp = (Math.ceil(calculatedTimestamp / 1000.0) * 1000).toLong
    val calculatedOffset = calculatedGenerationTimestamp - timeService.correctedTime()
    Math.max(minimalBlockGenerationOffset.toMillis, calculatedOffset).millis
  }

  private val blockMiningStarted = Kamon.metrics.counter("block-mining-started")
  private val microMiningStarted = Kamon.metrics.counter("micro-mining-started")
}

trait MinerDebugInfo {
  def collectNextBlockGenerationTimes: List[(Address, Long)]
}



