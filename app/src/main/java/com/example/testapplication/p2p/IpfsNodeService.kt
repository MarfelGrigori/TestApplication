package com.example.testapplication.p2p

import android.util.Log
import io.ipfs.cid.Cid
import io.ipfs.multiaddr.MultiAddress
import io.libp2p.core.PeerId
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
            val swarmAddresses = listOf(MultiAddress("/ip4/0.0.0.0/tcp/0"))
            val bootstrapAddresses = listOf(MultiAddress(nodeMultiaddress))
            val authoriser = BlockRequestAuthoriser { _, _, _ ->
                CompletableFuture.completedFuture(true)
            }
            val builder = HostBuilder().generateIdentity()
            val identity = IdentitySection(builder.privateKey.bytes(), builder.peerId)
            val records = RamRecordStore()
            val blocks = RamBlockstore()
            ipfs = EmbeddedIpfs.build(
                records,
                blocks,
                false,
                swarmAddresses,
                bootstrapAddresses,
                identity,
                authoriser,
                Optional.empty()
            )
            Log.d(TAG, "start: EmbeddedIpfs.build ok, calling start()")
            ipfs!!.start()
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

    private fun startPingLoop() {
        if (!pingRunning.compareAndSet(false, true)) return
        pingJob = scope.launch {
            while (isActive && pingRunning.get() && ipfs != null) {
                try {
                    val start = System.currentTimeMillis()
                    val wants = listOf(Want(Cid.decode(TEST_CID)))
                    val peers = setOf(peerId)
                    Log.d(TAG, "ping: send want CID=$TEST_CID peer=${peerId.toBase58()}")
                    ipfs!!.getBlocks(wants, peers, false)
                    val elapsed = System.currentTimeMillis() - start
                    _latencyMs.value = elapsed
                    _lastError.value = null
                    Log.d(TAG, "ping: ok latency=${elapsed}ms")
                } catch (e: Exception) {
                    _latencyMs.value = null
                    _lastError.value = "Ping: ${e.message}"
                    Log.w(TAG, "ping: fail peer=${peerId.toBase58()} error=${e.message}", e)
                }
                delay(pingIntervalMs)
            }
            pingRunning.set(false)
            Log.d(TAG, "ping: loop stopped")
        }
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
            Result.failure(RuntimeException(timeoutMessage ?: "Таймаут запроса ${FETCH_TIMEOUT_SEC} с", e))
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
