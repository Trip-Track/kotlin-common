package swa.circuit_breaker

import io.ktor.client.*
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
import java.net.ConnectException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class CircuitBreakerReject: RuntimeException("call rejected by Circuit Breaker")


sealed interface State {
    suspend fun processRequest(path: String, params: Parameters): HttpResponse

    suspend fun sendReq(httpClient: HttpClient, baseUrl: String?, path: String, params: Parameters): HttpResponse? {
        return try {
            httpClient.get("${baseUrl}$path") {
                url { parameters.appendAll(params)
                    println("${baseUrl}$path")
                    println(url)
                }
                timeout { requestTimeoutMillis = 1_500 }
            }
        } catch (e: HttpRequestTimeoutException) {
            null
        }
        catch (e: ConnectException) { null }

    }

    class Closed(private val cb: CircuitBreaker): State {
        private val failureCountExpiration = AtomicLong(System.currentTimeMillis() + cb.resetTimeoutMs)
        init {
            cb.failureCount.set(0)
        }

        override suspend fun processRequest(path: String, params: Parameters): HttpResponse {
            val rsp: HttpResponse? = sendReq(cb.http, cb.baseUrl(), path, params)

            val now = System.currentTimeMillis()
            if ( now > failureCountExpiration.get() ) {
                cb.failureCount.set(0)
                failureCountExpiration.set(System.currentTimeMillis() + cb.resetTimeoutMs)
            }

            if (rsp != null && rsp.status.isSuccess()) return rsp

            if (cb.failureCount.incrementAndGet() >= cb.failThreshold) cb.changeState(this,Open(cb))
            throw CircuitBreakerReject()
        }
    }


    class Open(private val cb: CircuitBreaker): State {
        val timerExpiration = System.currentTimeMillis() + cb.resetTimeoutMs

        override suspend fun processRequest(path: String, params: Parameters): HttpResponse {

            if ( System.currentTimeMillis() >= timerExpiration){
                val rsp: HttpResponse? = sendReq(cb.http, cb.baseUrl(), path, params)

                if (rsp != null && rsp.status.isSuccess()) {
                    cb.changeState(this, HalfOpen(cb))
                    return rsp
                }
            }
            throw CircuitBreakerReject()
        }
    }


    class HalfOpen(private val cb: CircuitBreaker): State {
        init {
            cb.successCount.set(0)
        }
        override suspend fun processRequest(path: String, params: Parameters): HttpResponse {
            val rsp: HttpResponse? = sendReq(cb.http, cb.baseUrl(), path, params)

            if (rsp != null && rsp.status.isSuccess()) {
                if(cb.successCount.incrementAndGet() >= cb.halfOpenSuccThreshold){
                    cb.changeState(this,Closed(cb))
                }
                return rsp
            }
            cb.changeState(this, Open(cb))
            throw CircuitBreakerReject()
        }
    }
}


class CircuitBreaker(
    val failThreshold: Int = 2,
    val resetTimeoutMs: Long = 10_000,
    val halfOpenSuccThreshold: Int = 2,
    val baseUrl: suspend () -> String?
) {
    val logger: Logger = Logger(this::class.simpleName!!)

    var failureCount = AtomicInteger(0)
    var successCount = AtomicInteger(0)
    private var state = AtomicReference<State>(State.Closed(this))

    val http = HttpClient(Java) {
        install(ContentNegotiation) { json(Json) }
        install(HttpTimeout) {requestTimeoutMillis = 1_500}
        expectSuccess = false
    }

    suspend fun routeRequest(path: String, params: Parameters): HttpResponse{
        while (true) {
            logger.log.info("Routing request to $path")
            val current = state.get()
            try {
                return current.processRequest(path, params)
            } catch (x: StateChanged) {
                logger.log.info("Route request to $path failed: $x")
            }
        }

    }

    fun changeState(expect: State, newState: State){
        if (state.compareAndSet(expect, newState)) {
            logger.log.info("Changed state from $expect to $newState")
        } else {
            logger.log.warn("Lost state change from $expect to $newState")
            throw StateChanged
        }
    }

    object StateChanged : Throwable()
}