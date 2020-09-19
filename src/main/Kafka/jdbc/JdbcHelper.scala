package temputer

import java.sql.{CallableStatement, Connection, PreparedStatement, ResultSet, SQLException, Statement, Types}

import scala.collection.mutable.ListBuffer

object JdbcHelper {
  private var conn: Connection = null
  private var preparedStatement: PreparedStatement = null
  private var callableStatement: CallableStatement = null
  /**
    * 建立数据库连接
    *
    * @return
    * @throws SQLException
    */
  @throws[SQLException]
  private def getConnection: Connection = {
    conn = DBConnectionPool.getConn()
    conn
  }
  /**
    * 释放连接
    *
    * @param conn
    */
  private def freeConnection(conn: Connection) = {
    try
      DBConnectionPool.releaseCon(conn)
    catch {
      case e: SQLException =>
        e.printStackTrace()
    }
  }
  /**
    * 释放statement
    *
    * @param statement
    */
  private def freeStatement(statement: Statement) = {
    try
      statement.close()
    catch {
      case e: SQLException =>
        e.printStackTrace()
    }
  }
  /**
    * 释放resultset
    *
    * @param rs
    */
  private def freeResultSet(rs: ResultSet) = {
    try
      rs.close()
    catch {
      case e: SQLException =>
        e.printStackTrace()
    }
  }
  /**
    * 释放资源
    *
    * @param conn
    * @param statement
    * @param rs
    */
  def free(conn: Connection, statement: Statement, rs: ResultSet): Unit = {
    if (rs != null) freeResultSet(rs)
    if (statement != null) freeStatement(statement)
    if (conn != null) freeConnection(conn)
  }

  /////////////////////////////////////////////////////////

  /**
    * 获取PreparedStatement
    *
    * @param sql
    * @throws SQLException
    */
  @throws[SQLException]
  private def getPreparedStatement(sql: String) = {
    conn = getConnection
    preparedStatement = conn.prepareStatement(sql)
  }

  /**
    * 用于查询，返回结果集
    *
    * @param sql
    * sql语句
    * @return 结果集
    * @throws SQLException
    */
  @throws[SQLException]
  def query(sql: String): List[Map[String, Object]] = {
    var rs: ResultSet = null
    try {
      getPreparedStatement(sql)
      rs = preparedStatement.executeQuery
      ResultToListMap(rs)
    } catch {
      case e: SQLException =>
        throw new SQLException(e)
    } finally free(conn, callableStatement, rs)
  }
  /**
    * 用于带参数的查询，返回结果集
    *
    * @param sql
    * sql语句
    * @param paramters
    * 参数集合
    * @return 结果集
    * @throws SQLException
    */
  @throws[SQLException]
  def query(sql: String, paramters: Any*): List[Map[String, Object]] = {
    var rs: ResultSet = null
    try {
      getPreparedStatement(sql)
      var i = 0
      while ( {
        i < paramters.length
      }) {
        preparedStatement.setObject(i + 1, paramters(i))

        {
          i += 1;
          i - 1
        }
      }
      rs = preparedStatement.executeQuery
      ResultToListMap(rs)
    } catch {
      case e: SQLException =>
        throw new SQLException(e)
    } finally free(conn, callableStatement, rs)
  }
  /**
    * 返回单个结果的值，如count\min\max等等
    *
    * @param sql
    * sql语句
    * @return 结果集
    * @throws SQLException
    */
  @throws[SQLException]
  def getSingle(sql: String): Any = {
    var result: Any = null
    var rs: ResultSet = null
    try {
      getPreparedStatement(sql)
      rs = preparedStatement.executeQuery
      if (rs.next) result = rs.getObject(1)
      result
    } catch {
      case e: SQLException =>
        throw new SQLException(e)
    } finally free(conn, callableStatement, rs)
  }

  /**
    * 返回单个结果值，如count\min\max等
    *
    * @param sql
    * sql语句
    * @param paramters
    * 参数列表
    * @return 结果
    * @throws SQLException
    */
  @throws[SQLException]
  def getSingle(sql: String, paramters: Any*): Any = {
    var result: Any = null
    var rs: ResultSet = null
    try {
      getPreparedStatement(sql)
      var i = 0
      while ( {
        i < paramters.length
      }) {
        preparedStatement.setObject(i + 1, paramters(i))

        {
          i += 1;
          i - 1
        }
      }
      rs = preparedStatement.executeQuery
      if (rs.next) result = rs.getObject(1)
      result
    } catch {
      case e: SQLException =>
        throw new SQLException(e)
    } finally free(conn, callableStatement, rs)
  }

  /**
    * 用于增删改
    *
    * @param sql
    * sql语句
    * @return 影响行数
    * @throws SQLException
    */
  @throws[SQLException]
  def update(sql: String): Int = try {
    getPreparedStatement(sql)
    preparedStatement.executeUpdate
  } catch {
    case e: SQLException =>
      throw new SQLException(e)
  } finally free(conn, callableStatement, null)

  /**
    * 用于增删改（带参数）
    *
    * @param sql
    * sql语句
    * @param paramters
    * sql语句
    * @return 影响行数
    * @throws SQLException
    */
  @throws[SQLException]
  def update(sql: String, paramters: Any*): Int = try {
    getPreparedStatement(sql)
    var i = 0
    while ( {
      i < paramters.length
    }) {
      preparedStatement.setObject(i + 1, paramters(i))

      {
        i += 1;
        i - 1
      }
    }
    preparedStatement.executeUpdate
  } catch {
    case e: SQLException =>
      throw new SQLException(e)
  } finally free(conn, callableStatement, null)

  /**
    * 插入值后返回主键值
    *
    * @param sql
    * 插入sql语句
    * @return 返回结果
    * @throws Exception
    */
  @throws[SQLException]
  def insertWithReturnPrimeKey(sql: String): Any = {
    var rs: ResultSet = null
    var result: Object = null
    try {
      conn = getConnection
      preparedStatement = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
      preparedStatement.execute
      rs = preparedStatement.getGeneratedKeys
      if (rs.next) result = rs.getObject(1)
      result
    } catch {
      case e: SQLException =>
        throw new SQLException(e)
    }
  }

  /**
    * 插入值后返回主键值
    *
    * @param sql
    * 插入sql语句
    * @param paramters
    * 参数列表
    * @return 返回结果
    * @throws SQLException
    */
  @throws[SQLException]
  def insertWithReturnPrimeKey(sql: String, paramters: Any*): Any = {
    var rs: ResultSet = null
    var result: Object = null
    try {
      conn = getConnection
      preparedStatement = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
      var i = 0
      while ( {
        i < paramters.length
      }) {
        preparedStatement.setObject(i + 1, paramters(i))

        {
          i += 1;
          i - 1
        }
      }
      preparedStatement.execute
      rs = preparedStatement.getGeneratedKeys
      if (rs.next) result = rs.getObject(1)
      result
    } catch {
      case e: SQLException =>
        throw new SQLException(e)
    }
  }

  //////////////////////////////////////////////////////////////////

  /**
    * 获取CallableStatement
    *
    * @param procedureSql
    * @throws SQLException
    */
  @throws[SQLException]
  private def getCallableStatement(procedureSql: String) = {
    conn = getConnection
    conn.prepareCall(procedureSql)
  }


  /**
    * 存储过程查询
    * 注意outNames和outOracleTypes的顺序要对应 顺序按存储过程的参数顺序排列
    *
    * @param procedureSql
    * @param ins            输入参数数组
    * @param outNames       输出参数名称
    * @param outOracleTypes 输出参数类型
    * @return
    *
    */
  @throws[SQLException]
  def callableQuery(procedureSql: String, ins: Array[Object], outNames: Array[String], outOracleTypes: Array[Int]): Map[String, Object] = {

    val listBuffer = new ListBuffer[Object]

    try {
      callableStatement = getCallableStatement(procedureSql)

      var count = 0

      for (i <- 0 until ins.length) {
        count = count + 1
        callableStatement.setObject(count, ins(i))
      }

      for (j <- 0 until outOracleTypes.length) {
        count = count + 1
        callableStatement.registerOutParameter(count, outOracleTypes(j))
      }

      callableStatement.execute()

      count = count - outOracleTypes.length
      for (i <- 0 until outOracleTypes.length) {
        count = count + 1
        listBuffer.append(callableStatement.getObject(count))
      }
      outNames.zip(listBuffer.toList).toMap
    } catch {
      case e: SQLException =>
        throw new SQLException(e)
    }
    finally free(conn, callableStatement, null)
  }


  /**
    * 调用存储过程，查询单个值
    *
    * @param procedureSql
    * @return
    * @throws SQLException
    */
  @throws[SQLException]
  def callableGetSingle(procedureSql: String): Any = {
    var result: Any = null
    var rs: ResultSet = null
    try {
      rs = getCallableStatement(procedureSql).executeQuery
      while ( {
        rs.next
      }) result = rs.getObject(1)
      result
    } catch {
      case e: SQLException =>
        throw new SQLException(e)
    } finally free(conn, callableStatement, rs)
  }

  /**
    * 调用存储过程(带参数)，查询单个值
    *
    * @param procedureSql
    * @return
    * @throws SQLException
    */
  @throws[SQLException]
  def callableGetSingle(procedureSql: String, paramters: Any*): Any = {
    var result: Any = null
    var rs: ResultSet = null
    try {
      callableStatement = getCallableStatement(procedureSql)
      var i = 0
      while ( {
        i < paramters.length
      }) {
        callableStatement.setObject(i + 1, paramters(i))

        {
          i += 1;
          i - 1
        }
      }
      rs = callableStatement.executeQuery
      while ( {
        rs.next
      }) result = rs.getObject(1)
      result
    } catch {
      case e: SQLException =>
        throw new SQLException(e)
    } finally free(conn, callableStatement, rs)
  }

  @throws[SQLException]
  def callableWithParamters(procedureSql: String): Any = try {
    callableStatement = getCallableStatement(procedureSql)
    callableStatement.registerOutParameter(0, Types.OTHER)
    callableStatement.execute
    callableStatement.getObject(0)
  } catch {
    case e: SQLException =>
      throw new SQLException(e)
  } finally free(conn, callableStatement, null)

  /**
    * 调用存储过程，执行增删改
    *
    * @param procedureSql
    * 存储过程
    * @return 影响行数
    * @throws SQLException
    */
  @throws[SQLException]
  def callableUpdate(procedureSql: String): Int = try {
    callableStatement = getCallableStatement(procedureSql)
    callableStatement.executeUpdate
  } catch {
    case e: SQLException =>
      throw new SQLException(e)
  } finally free(conn, callableStatement, null)

  /**
    * 调用存储过程（带参数），执行增删改
    *
    * @param procedureSql
    * 存储过程
    * @param parameters
    * @return 影响行数
    * @throws SQLException
    */
  @throws[SQLException]
  def callableUpdate(procedureSql: String, parameters: Any*): Int = try {
    callableStatement = getCallableStatement(procedureSql)
    var i = 0
    while ( {
      i < parameters.length
    }) {
      callableStatement.setObject(i + 1, parameters(i))

      {
        i += 1;
        i - 1
      }
    }
    callableStatement.executeUpdate
  } catch {
    case e: SQLException =>
      throw new SQLException(e)
  } finally free(conn, callableStatement, null)


  @throws[SQLException]
  private def ResultToListMap(rs: ResultSet) = {

    val list = new ListBuffer[Map[String, Object]]

    while (rs.next) {
      val map = new scala.collection.mutable.HashMap[String, Object]
      val md = rs.getMetaData
      for (i <- 1 until md.getColumnCount) {
        map.put(md.getColumnLabel(i), rs.getObject(i))
      }
      list.append(map.toMap)
    }
    list.toList
  }


  def main(args: Array[String]): Unit = {
    //        val list = query("select * from POL_BEN_PLAN_CFG")
    //        for (e <- list) {
    //          println(e)
    //        }

    //    val startVersionMap = VersionManager.verStartIf("20181106", "spark_ifrs17_mp_ind_aio")
    //    println(startVersionMap.toString())
    //
    //    val endVersionMap = VersionManager.verEndIf(startVersionMap, JobConstants.P_RUN_FLAG_SUCCESS, "success")
    //    println(endVersionMap.toString())

  }

}