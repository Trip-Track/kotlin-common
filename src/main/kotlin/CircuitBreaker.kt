package swa.circuit_breaker

import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.java.Java
import io.ktor.client.plugins.HttpRequestTimeoutException
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.plugins.timeout
import io.ktor.client.request.*
import io.ktor.client.statement.HttpResponse
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.json
import kotlinx.serialization.json.Json
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class CircuitBreakerReject: RuntimeException("call rejected by Circuit Breaker")


sealed interface State {
    suspend fun processRequest(path: String, params: Parameters)

    suspend fun sendReq(httpClient: HttpClient, baseUrl: String, path: String, params: Parameters): HttpResponse {
        return httpClient.get("${baseUrl}$path") {
            url { parameters.appendAll(params) }
            timeout { requestTimeoutMillis = 1_500 }
        }
    }

    class Closed(private val cb: CircuitBreaker): State {
        private val failureCountExpiration = AtomicLong(System.currentTimeMillis() + cb.resetTimeoutMs)
        init {
            cb.failureCount.set(0)
        }

        override suspend fun processRequest(path: String, params: Parameters) {
            val rsp: HttpResponse? = try {
                sendReq(cb.http, cb.baseUrl, path, params)
            } catch (e: HttpRequestTimeoutException) { null }

            val now = System.currentTimeMillis()
            if ( now > failureCountExpiration.get() ) {
                cb.failureCount.set(0)
                failureCountExpiration.set(System.currentTimeMillis() + cb.resetTimeoutMs)
            }

            if (rsp != null && rsp.status.isSuccess()) return rsp.body()

            if (cb.failureCount.incrementAndGet() >= cb.failThreshold) cb.changeState(this,Open(cb))
            throw CircuitBreakerReject()
        }
    }


    class Open(private val cb: CircuitBreaker): State {
        val timerExpiration = System.currentTimeMillis() + cb.resetTimeoutMs

        override suspend fun processRequest(path: String, params: Parameters) {

            if ( System.currentTimeMillis() >= timerExpiration){
                val rsp: HttpResponse? = try {
                    sendReq(cb.http, cb.baseUrl, path, params)
                } catch (e: HttpRequestTimeoutException) { null }

                if (rsp != null && rsp.status.isSuccess()) {
                    cb.changeState(this, HalfOpen(cb))
                    return rsp.body()
                }
            }
            throw CircuitBreakerReject()
        }
    }


    class HalfOpen(private val cb: CircuitBreaker): State {
        init {
            cb.successCount.set(0)
        }
        override suspend fun processRequest(path: String, params: Parameters) {
            val rsp: HttpResponse? = try {
                sendReq(cb.http, cb.baseUrl, path, params)
            } catch (e: HttpRequestTimeoutException) { null }

            if (rsp != null && rsp.status.isSuccess()) {
                if(cb.successCount.incrementAndGet() >= cb.halfOpenSuccThreshold){
                    cb.changeState(this,Closed(cb))
                }
                return rsp.body()
            }
            cb.changeState(this, Open(cb))
            throw CircuitBreakerReject()
        }
    }
}


class CircuitBreaker(
    val baseUrl: String = "localhost:4444",
    val failThreshold: Int = 4,
    val resetTimeoutMs: Long = 30_000,
    val halfOpenSuccThreshold: Int = 5
) {

    var failureCount = AtomicInteger(0)
    var successCount = AtomicInteger(0)
    private var state = AtomicReference<State>(State.Closed(this))

    val http = HttpClient(Java) {
        install(ContentNegotiation) { json(Json) }
        install(HttpTimeout) {requestTimeoutMillis = 3_000}
        expectSuccess = false
    }

    suspend fun routeRequest(path: String, params: Parameters){
        while (true) {
            val current = state.get()
            try {
                return current.processRequest(path, params)
            } catch (x: StateChanged) {

            }
        }

    }

    fun changeState(expect: State, newState: State){
        if (state.compareAndSet(expect, newState)) {

        } else {
            throw StateChanged
        }
    }

    object StateChanged : Throwable()
}