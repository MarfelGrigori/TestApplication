package com.example.testapplication.p2p

import android.util.Log
import io.ipfs.cid.Cid
import io.ipfs.multiaddr.MultiAddress
import io.libp2p.core.PeerId
import io.libp2p.core.StreamPromise
import io.libp2p.core.multiformats.Multiaddr
import io.libp2p.protocol.Ping
import io.libp2p.protocol.PingController
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.peergos.BlockRequestAuthoriser
import org.peergos.EmbeddedIpfs
import org.peergos.HashedBlock
import org.peergos.HostBuilder
import org.peergos.Want
import org.peergos.blockstore.RamBlockstore
import org.peergos.config.IdentitySection
import org.peergos.protocol.dht.RamRecordStore
import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class IpfsNodeService(
    private val nodeMultiaddress: String,
    private val pingIntervalMs: Long = 2000L,
    private val timeoutMessage: String? = null
) {
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private var pingJob: Job? = null
    private var ipfs: EmbeddedIpfs? = null
    private val _latencyMs = MutableStateFlow<Long?>(null)
    val latencyMs: StateFlow<Long?> = _latencyMs.asStateFlow()
    private val _lastError = MutableStateFlow<String?>(null)
    val lastError: StateFlow<String?> = _lastError.asStateFlow()
    private val _isRunning = MutableStateFlow(false)
    val isRunning: StateFlow<Boolean> = _isRunning.asStateFlow()
    private val pingRunning = AtomicBoolean(false)

    private val peerId: PeerId by lazy {
        val p2pPart = nodeMultiaddress.substringAfterLast("/p2p/")
        PeerId.fromBase58(p2pPart)
    }

    suspend fun start(): Result<Unit> = withContext(Dispatchers.IO) {
        try {
            Log.d(TAG, "start: bootstrap=$nodeMultiaddress swarm=/ip4/0.0.0.0/tcp/0")
            _lastError.value = null
            ipfs = buildAndStartIpfs()
            Log.d(TAG, "start: waiting ${BOOTSTRAP_WAIT_MS}ms for bootstrap")
            delay(BOOTSTRAP_WAIT_MS)
            _isRunning.value = true
            startPingLoop()
            Log.d(TAG, "start: success, peerId=${peerId.toBase58()}")
            Result.success(Unit)
        } catch (e: Exception) {
            Log.e(TAG, "start: failed", e)
            _lastError.value = e.message ?: "Start failed"
            Result.failure(e)
        }
    }

    private suspend fun buildAndStartIpfs(): EmbeddedIpfs = withContext(Dispatchers.IO) {
        val swarmAddresses = listOf(MultiAddress("/ip4/0.0.0.0/tcp/0"))
        val bootstrapAddresses = listOf(MultiAddress(nodeMultiaddress))
        val authoriser = BlockRequestAuthoriser { _, _, _ ->
            CompletableFuture.completedFuture(true)
        }
        val builder = HostBuilder().generateIdentity()
        val identity = IdentitySection(builder.privateKey.bytes(), builder.peerId)
        val records = RamRecordStore()
        val blocks = RamBlockstore()
        val node = EmbeddedIpfs.build(
            records,
            blocks,
            false,
            swarmAddresses,
            bootstrapAddresses,
            identity,
            authoriser,
            Optional.empty()
        )
        node.start()
        node
    }

    private suspend fun doReconnect() = withContext(Dispatchers.IO) {
        _isRunning.value = false
        _latencyMs.value = null
        _lastError.value = null
        Log.d(TAG, "ping: global reconnect — stopping node")
        try {
            ipfs?.stop()?.get(10, TimeUnit.SECONDS)
        } catch (_: Exception) { }
        ipfs = null
        Log.d(TAG, "ping: reconnect — building and starting new node")
        ipfs = buildAndStartIpfs()
        Log.d(TAG, "ping: reconnect — waiting ${BOOTSTRAP_WAIT_MS}ms for bootstrap")
        delay(BOOTSTRAP_WAIT_MS)
        _isRunning.value = true
        Log.d(TAG, "ping: reconnect done")
    }

    private fun startPingLoop() {
        if (!pingRunning.compareAndSet(false, true)) return
        pingJob = scope.launch {
            var streamPromise: StreamPromise<out PingController>? = null
            var controller: PingController? = null
            while (isActive && pingRunning.get() && ipfs != null) {
                try {
                    withTimeout(PING_ITERATION_TIMEOUT_MS) {
                        if (controller == null) {
                            val host = ipfs!!.node
                            val multiaddr = Multiaddr.fromString(nodeMultiaddress)
                            val ping = Ping()
                            streamPromise = ping.dial(host, multiaddr)
                            controller = streamPromise!!.controller.get(PING_DIAL_TIMEOUT_SEC, TimeUnit.SECONDS)
                            Log.d(TAG, "ping: connection opened (reuse)")
                        }
                        val rttMs = controller!!.ping().get(PING_TIMEOUT_SEC, TimeUnit.SECONDS)
                        _latencyMs.value = rttMs
                        _lastError.value = null
                        Log.d(TAG, "ping: ok latency=${rttMs}ms (libp2p Ping)")
                    }
                } catch (e: Exception) {
                    if (e is CancellationException) {
                        Log.w(TAG, "ping: iteration timeout or cancelled")
                    } else {
                        Log.w(TAG, "ping: fail peer=${peerId.toBase58()} error=${e.message}", e)
                    }
                    closeStream(streamPromise)
                    streamPromise = null
                    controller = null
                    val isRejected = e is RejectedExecutionException || e.cause is RejectedExecutionException
                    if (isRejected) {
                        runCatching { doReconnect() }.onFailure { Log.e(TAG, "ping: global reconnect failed", it) }
                    }
                    _lastError.value = null
                    delay(PING_RETRY_DELAY_MS)
                    continue
                }
                delay(pingIntervalMs)
            }
            closeStream(streamPromise)
            pingRunning.set(false)
            Log.d(TAG, "ping: loop stopped")
        }
    }

    private fun closeStream(streamPromise: StreamPromise<out PingController>?) {
        if (streamPromise == null) return
        try {
            streamPromise.stream.get(2, TimeUnit.SECONDS).close()
        } catch (_: Exception) { }
    }

    suspend fun fetchBlock(cidStr: String): Result<String> = withContext(Dispatchers.IO) {
        Log.d(TAG, "fetchBlock: request cid=$cidStr peer=${peerId.toBase58()} timeout=${FETCH_TIMEOUT_SEC}s")
        try {
            withTimeout(FETCH_TIMEOUT_MS) {
                val ipfsNode = ipfs ?: throw IllegalStateException("Node not started")
                val cid = Cid.decode(cidStr)
                val wants = listOf(Want(cid))
                val peers = setOf(peerId)
                Log.d(TAG, "fetchBlock: getBlocks(cid=$cidStr, addToLocal=true)")
                val blocks = ipfsNode.getBlocks(wants, peers, true)
                Log.d(TAG, "fetchBlock: getBlocks returned ${blocks.size} block(s)")
                if (blocks.isEmpty()) throw NoSuchElementException("Block not found: $cidStr")
                val block: HashedBlock = blocks[0]
                val raw = block.block.decodeToString()
                val content = raw.stripDisplayText()
                Log.d(TAG, "fetchBlock: success size=\"CID: ${block.hash}\\nSize: ${block.block.size} bytes\\n\\nContent (UTF-8):\\n$content\"")
                content
            }.let { Result.success(it) }
        } catch (e: CancellationException) {
            Log.w(TAG, "fetchBlock: timeout after ${FETCH_TIMEOUT_SEC}s cid=$cidStr")
            _lastError.value = timeoutMessage ?: "Таймаут запроса (${FETCH_TIMEOUT_SEC} с)"
            Result.failure(RuntimeException(timeoutMessage ?: "Таймаут запроса $FETCH_TIMEOUT_SEC с", e))
        } catch (e: Exception) {
            Log.w(TAG, "fetchBlock: error cid=$cidStr", e)
            _lastError.value = e.message
            Result.failure(e)
        }
    }

    suspend fun stop() = withContext(Dispatchers.IO) {
        Log.d(TAG, "stop: requested")
        pingRunning.set(false)
        pingJob?.cancel()
        pingJob?.join()
        try {
            ipfs?.stop()?.get(10, TimeUnit.SECONDS)
        } catch (_: Exception) { }
        ipfs = null
        _isRunning.value = false
        _latencyMs.value = null
        _lastError.value = null
        Log.d(TAG, "stop: done")
    }

    companion object {
        private const val TAG = "P2P"
        const val TEST_CID = "QmTBimFzPPP2QsB7TQGc2dr4BZD4i7Gm2X1mNtb6DqN9Dr"
        private const val FETCH_TIMEOUT_SEC = 30L
        private const val FETCH_TIMEOUT_MS = FETCH_TIMEOUT_SEC * 1000L
        private const val BOOTSTRAP_WAIT_MS = 5000L
        private const val PING_DIAL_TIMEOUT_SEC = 30L
        private const val PING_TIMEOUT_SEC = 10L
        private const val PING_RETRY_DELAY_MS = 500L
        private const val PING_ITERATION_TIMEOUT_MS = 15_000L
    }
}

private fun String.stripDisplayText(): String = this
    .removePrefix("\uFEFF")
    .filter { c -> !c.isControlOrReplacement() }

private fun Char.isControlOrReplacement(): Boolean {
    if (this == '\uFFFD') return true
    if (this == '\uFEFF') return true
    if (this in '\u0000'..'\u001F') return this != '\n' && this != '\r' && this != '\t'
    if (this in '\u007F'..'\u009F') return true
    return false
}
