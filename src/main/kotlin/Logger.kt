package swa.circuit_breaker

import io.ktor.util.logging.*


class Logger(name: String) {
    val log = KtorSimpleLogger(name)
}