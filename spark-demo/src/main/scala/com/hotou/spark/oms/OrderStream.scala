package com.hotou.spark.oms

import java.io.PrintWriter
import java.net.ServerSocket

object OrderStream {

  def main(args : Array[String]) {
    val port = 22222
    val sleepDelayMs = 100

    val listener = new ServerSocket(port)
    println("Listening on port: " + port)

    val generator = new OrderGenerator()

    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run(): Unit = {
          println("Got client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)

          while (true) {
            Thread.sleep(sleepDelayMs)
            out.println(generator.getNextMessage)
            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }
}
