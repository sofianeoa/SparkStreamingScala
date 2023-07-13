package org.example

object Main {
  def main(args: Array[String]) {
    val thread1 = new Thread {
      override def run(): Unit = Move.main(args)
    }

    val thread2 = new Thread {
      override def run(): Unit = SparkStreamingJson.main(args)
    }

    val thread3 = new Thread {
      override def run(): Unit = SparkStreamingGroupe.main(args)
    }

    // Lancer les threads
    thread1.start()
    thread2.start()
    thread3.start()

    // Attendre que tous les threads se terminent
    thread1.join()
    thread2.join()
    thread3.join()
  }
}
