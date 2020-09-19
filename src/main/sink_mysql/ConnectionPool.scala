import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
import com.mysql.jdbc.Connection


/**
  * * @Author wang guo
  * * @Create Date 2020/9/19 1:14
  */
object ConnectionPool  {



  private val pool={
    try{
      Class.forName("com.mysql.jdbc.Driver")
      //  DriverManager.getConnection("jdbc:mysql://192.168.137.130:3306/rzdb?useSSL=false","root","syncdb123!")
      val config = new BoneCPConfig()
      config.setUsername("root")
      config.setPassword("123")
      config.setJdbcUrl("jdbc:mysql://192.168.0.2:3306/test?useSSL=false")
      config.setMinConnectionsPerPartition(1) //最小连接数
      config.setMaxConnectionsPerPartition(5) //最大连接数
      config.setCloseConnectionWatch(true)  //关闭的时候要不要监控

      Some(new BoneCP(config))

    }catch {
      case e:Exception=>{
        e.printStackTrace()
        None
      }
    }


  }

     //:Option[Connection]====connection:Connection
  def getConnection()={
    pool match {
      case  Some(pool)=> Some(pool.getConnection)
      case None=>None

    }

  }

  def  returnConnection(connection:Connection)={
    if(null != connection){
      connection.close() //这个地方不能关闭，应该要返回到池里面去才行
    }

    def closeConnection(connection:Connection): Unit = {
      if (!connection.isClosed) {
        connection.close()

      }
    }

  }


}
