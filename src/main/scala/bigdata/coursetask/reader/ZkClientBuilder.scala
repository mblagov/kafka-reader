package bigdata.coursetask.reader

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}

object ZkClientBuilder {
  // Пусть будет захардкожено
  private val sleepMsBetweenRetries = 100
  private val maxRetries = 3

  def build(host: String): CuratorFramework = {
    import org.apache.curator.retry.RetryNTimes

    val retryPolicy = new RetryNTimes(maxRetries, sleepMsBetweenRetries)

    CuratorFrameworkFactory.newClient(host, retryPolicy)
  }
}
