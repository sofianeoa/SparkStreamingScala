package org.example

object Move extends App {

  def test(): Unit = {
    val heartSourceDirectory = os.pwd / "src" / "main" / "Ressources" / "activities_heart"
    val heartDestinationDirectory = os.pwd / "src" / "main" / "Ressources" / "activities_heart_streaming"
    val logSourceDirectory = os.pwd / "src" / "main" / "Ressources" / "activity_log"
    val logDestinationDirectory = os.pwd / "src" / "main" / "Ressources" / "activity_log_streaming"

    val heartFiles = os.list(heartSourceDirectory)
    val logFiles = os.list(logSourceDirectory)

    heartFiles.foreach { heartFile =>
      val destinationHeartFile = heartDestinationDirectory / heartFile.last
      val matchingLogFiles = logFiles.filter(_.last.startsWith(heartFile.last.take(10)))

      os.copy(heartFile, destinationHeartFile, replaceExisting = true)
      println(s"Le fichier ${heartFile.last} a été copié à ${destinationHeartFile.toString()} avec succès.")

      matchingLogFiles.foreach { logFile =>
        val destinationLogFile = logDestinationDirectory / logFile.last
        os.copy(logFile, destinationLogFile, replaceExisting = true)
        println(s"Le fichier ${logFile.last} a été copié à ${destinationLogFile.toString()} avec succès.")
      }

      Thread.sleep(10000)
    }
  }

  test()
}