package com.example.testapplication.p2p

import android.app.Application
import android.util.Log
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.example.testapplication.R
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch

private const val UI_TAG = "P2P_UI"
private const val NODE_MULTIADDR = "/dns4/ipfs.infra.cf.team/tcp/4001/p2p/12D3KooWKiqj21VphU2eE25438to5xeny6eP6d3PXT93ZczagPLT"
private const val PING_INTERVAL_MS = 2000L
private const val FETCH_TIMEOUT_SEC = 30

class P2PViewModel(
    private val application: Application
) : AndroidViewModel(application) {

    private val service = IpfsNodeService(
        NODE_MULTIADDR,
        PING_INTERVAL_MS,
        application.getString(R.string.error_timeout_request, FETCH_TIMEOUT_SEC)
    )

    private val _cidInput = MutableStateFlow(application.getString(R.string.p2p_cid_placeholder))
    val cidInput: StateFlow<String> = _cidInput.asStateFlow()

    private val _blockResult = MutableStateFlow<String?>(null)
    val blockResult: StateFlow<String?> = _blockResult.asStateFlow()

    private val _isLoading = MutableStateFlow(false)
    val isLoading: StateFlow<Boolean> = _isLoading.asStateFlow()

    val latencyMs: StateFlow<Long?> = service.latencyMs
    val lastError: StateFlow<String?> = service.lastError
    val isRunning: StateFlow<Boolean> = service.isRunning

    init {
        viewModelScope.launch {
            if (!service.isRunning.value) {
                Log.d(UI_TAG, "P2PViewModel: starting service")
                service.start().onFailure {
                    Log.e(UI_TAG, "P2PViewModel: start failed", it)
                    _blockResult.value = application.getString(R.string.error_start_failed, it.message ?: "")
                }
            }
        }
    }

    fun updateCidInput(value: String) {
        _cidInput.value = value
    }

    fun fetchBlock() {
        val cid = _cidInput.value.trim()
        Log.d(UI_TAG, "fetch clicked cid=$cid")
        _isLoading.value = true
        _blockResult.value = null
        viewModelScope.launch {
            try {
                service.fetchBlock(cid)
                    .onSuccess {
                        Log.d(UI_TAG, "fetch success length=${it.length}")
                        _blockResult.value = it
                    }
                    .onFailure {
                        Log.w(UI_TAG, "fetch failed: ${it.message}", it)
                        _blockResult.value = application.getString(R.string.error_fetch_failed, it.message ?: "")
                    }
            } finally {
                _isLoading.value = false
            }
        }
    }

    override fun onCleared() {
        super.onCleared()
        CoroutineScope(Dispatchers.IO).launch {
            service.stop()
        }
    }
}
