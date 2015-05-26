package kafka.security

import java.io.{FileWriter, BufferedWriter, File}
import java.net.URL
import javax.security.auth.login.Configuration

import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.hadoop.minikdc.MiniKdc

trait SecureTestHarness extends ZooKeeperTestHarness {
  val WorkDir = new File(System.getProperty("test.dir", "target"));
  val KdcConf = MiniKdc.createConf();
  val Kdc: MiniKdc = new MiniKdc(KdcConf, WorkDir);

  override def setUp() {
    val keytabFile: File = TestUtils.tempFile()
    val jaasFile: File = TestUtils.tempFile()

    val writer: BufferedWriter = new BufferedWriter(new FileWriter(jaasFile))

    val path: String = Thread.currentThread().getContextClassLoader.getResource("kafka_jaas.conf").getPath
    for(line <- io.Source.fromFile(path).getLines()) {
      val s: String = "\\$keytab-location"
      val replaced = line.replaceAll("\\$keytab-location",keytabFile.getAbsolutePath)
      writer.write(replaced)
      writer.newLine()
    }
    writer.close()

    Kdc.start()
    Kdc.createPrincipal(keytabFile, "client", "kafka/localhost")

    System.setProperty("java.security.auth.login.config", jaasFile.getAbsolutePath)

    super.setUp

//    System.setProperty("java.security.auth.login.config", Thread.currentThread().getContextClassLoader.getResource("kafka.jaas").getPath)
//    val keytabFile: File = new File("/tmp/kafka.keytab")
//    keytabFile.createNewFile()
//    keytabFile.deleteOnExit()
//    Kdc.start()
//    Kdc.createPrincipal(keytabFile, "client", "kafka/localhost")
//
//    super.setUp

  }

  override def tearDown() {
    super.tearDown
    Kdc.stop()
    System.clearProperty("java.security.auth.login.config")
    Configuration.setConfiguration(null)
  }
}
