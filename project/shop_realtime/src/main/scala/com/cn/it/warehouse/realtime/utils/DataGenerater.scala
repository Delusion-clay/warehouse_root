package com.cn.it.warehouse.realtime.utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}
import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.lang3.RandomUtils

import scala.util.Random

/**
 * 订单数据生成器
 */
object DataGenerater {

  val formater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  // ==== 订单数据 ====
  val usernames = List("张三","李四","王五","赵六","田七","天下")
  val userAddrs = List("北京市朝阳区大屯路3号", "北京市海淀区中关村创业大街4M咖啡", "安徽省 芜湖市 芜湖县", "吉林省 白山市 抚松县", "广西壮族自治区 贺州地区 贺州市", "安徽省 马鞍山市 金家庄区", "广东省 惠州市 龙门县", "新疆维吾尔自治区 省直辖行政单位 石河子市", "黑龙江省 齐齐哈尔市 龙江县", "上海市 市辖区 静安区", "福建省 泉州市 惠安县", "江苏省 无锡市 北塘区", "江苏省 泰州市 兴化市", "四川省 成都市 锦江区", "福建省 泉州市 鲤城区", "上海市 市辖区 浦东新区")
  val userPhones = List("189****5225", "152****5535", "155****2375", "182****8771", "137****9662", "182****2232", "156****1679", "187****9264", "186****8800", "186****6506", "150****5112", "185****1531", "134****2220", "130****2806", "138****8179", "135****5372", "130****2238", "186****4005", "159****8894", "188****6010", "185****6931", "188****0070", "136****8632", "155****5533", "152****7176", "159****1050")
  val orderRemarks = List("加急，加急", "周末不送货", "任意时间配送", "仅工作时间配送")
  val searchKeys = List("节日礼品", "养生", "美白", "防晒", "iphone x", "华为", "AirPods")
  val orderSrcs = List(0, 1, 2, 3, 4)
  val orderStatus = List(-3, -2, -1, 0, 1, 2)
  val shopIds = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
  val userIds = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
  val areaIds = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36)

  //==== 日志数据 ====
  val os = List("Win32", "IOS", "Android")
  val types = List(1, 2, 3)
  val referers = List(
    "http://www.shop.com",
    "https://jiadian.shop.com",
    "https://list.shop.com/list.html?cat=",
    "https://baby.shop.com"
  )
  val urls = List(
    "http://item.shop.com/p/43891413.html",
    "http://item.shop.com/p/89243132.html",
    "http://item.shop.com/p/34345451.html",
    "http://cart.shop.com/addToCart.html?rcd=1&pid=100000679481&pc=1&eb=1&rid=1567598858113&em=",
    "http://cart.shop.com/addToCart.html?rcd=1&pid=100000679481&pc=1&eb=1&rid=1567598858113&em=",
    "http://cart.shop.com/addToCart.html?rcd=1&pid=100000679481&pc=1&eb=1&rid=1567598858113&em=",
    "http://cart.shop.com/addToCart.html?rcd=1&pid=100000679481&pc=1&eb=1&rid=1567598858113&em=",
    "https://buy.shop.com/shopping/order/getOrderInfo",
    "https://buy.shop.com/shopping/list",
    "https://buy.shop.com/shopping/goodsIds=2133434",
    "https://order.shop.com/center/list.action",
    "https://order.shop.com/center/list.action",
    "https://order.shop.com/normal/item.action?orderid=43891413&PassKey=C3F0B668AE4A82FB5C1D77D016090218"
  )
  val guids = List(
    DigestUtils.md5Hex("1"),
    DigestUtils.md5Hex("2"),
    DigestUtils.md5Hex("3"),
    DigestUtils.md5Hex("4"),
    DigestUtils.md5Hex("5"),
    DigestUtils.md5Hex("6"),
    DigestUtils.md5Hex("7"),
    DigestUtils.md5Hex("8"),
    DigestUtils.md5Hex("9"),
    DigestUtils.md5Hex("10"),
    DigestUtils.md5Hex("11"),
    DigestUtils.md5Hex("12")
  )

  def main(args: Array[String]): Unit = {
    Class.forName("com.mysql.jdbc.Driver")
    val connection = DriverManager.getConnection("jdbc:mysql://hadoop-101:3306/shop?useUnicode=true&characterEncoding=utf-8", "root", "_Qq3pw34w9bqa")

    var count = 1
    while (count < 10000) {
      try {
        //insertLogs(connection)
        info("==== 新增log数据：{}条。 ====", count)
        insertOrders(connection, count)
        info("==== 新增order数据：{}条。 ====", count)
        Thread.sleep(50)
      } catch {
        case e: InterruptedException => e.printStackTrace()
      }
      count = count + 1
    }


    connection.close()
  }

  /**
   * 生成log数据
   *
   * @param connection
   */
  def insertLogs(connection: Connection) {
    val sql = "INSERT INTO `" +
      "logs` (id,url,referer,type,guid,session_id,ip,track_time,province_id,platform) VALUES(?,?,?,?,?,?,?,?,?,?)"
    var ps: PreparedStatement = null
    try {
      ps = connection.prepareStatement(sql)
      ps.setString(1, getFixLenthString(17))
      ps.setString(2, urls((Math.random() * 12 + 1).toInt))
      ps.setString(3, referers((Math.random() * 3 + 1).toInt))
      ps.setString(4, types((Math.random() * 2 + 1).toInt) + "")
      ps.setString(5, guids((Math.random() * 11 + 1).toInt))
      ps.setString(6, guids((Math.random() * 11 + 1).toInt))
      ps.setString(7, randomIp())
      ps.setString(8, formater.format(new Date()))
      ps.setString(9, areaIds((Math.random() * 35 + 1).toInt) + "")
      ps.setString(10, os((Math.random() * 2 + 1).toInt))
      ps.executeUpdate()
    } catch {
      case e: SQLException => e.printStackTrace()
    } finally {
      try {
        if (null != ps) {
          ps.close()
        }
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }
  }

  /**
   * 生成order数据
   *
   * @param connection
   */
  def insertOrders(connection: Connection, incr: Long) = {
    var maxId: Long = 0
    var ps: PreparedStatement = null
    var rs: ResultSet = null
    val sql =
      """
        |INSERT INTO `orders` (
        |  `orderId`,
        |  `orderNo`,
        |  `shopId`,
        |  `userId`,
        |  `orderStatus`,
        |  `goodsMoney`,
        |  `deliverType`,
        |  `deliverMoney`,
        |  `totalMoney`,
        |  `realTotalMoney`,
        |  `payType`,
        |  `isPay`,
        |  `areaId`,
        |  `areaIdPath`,
        |  `userName`,
        |  `userAddress`,
        |  `userPhone`,
        |  `orderScore`,
        |  `isInvoice`,
        |  `invoiceClient`,
        |  `orderRemarks`,
        |  `orderSrc`,
        |  `needPay`,
        |  `payRand`,
        |  `orderType`,
        |  `isRefund`,
        |  `isAppraise`,
        |  `cancelReason`,
        |  `rejectReason`,
        |  `rejectOtherReason`,
        |  `isClosed`,
        |  `goodsSearchKeys`,
        |  `orderunique`,
        |  `receiveTime`,
        |  `deliveryTime`,
        |  `tradeNo`,
        |  `dataFlag`,
        |  `createTime`,
        |  `settlementId`,
        |  `commissionFee`,
        |  `scoreMoney`,
        |  `useScore`,
        |  `orderCode`,
        |  `extraJson`,
        |  `orderCodeTargetId`,
        |  `noticeDeliver`,
        |  `invoiceJson`,
        |  `lockCashMoney`,
        |  `payTime`,
        |  `isBatch`,
        |  `totalPayFee`
        |)
        |VALUES
        |  (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      """.stripMargin
    try {
      ps = connection.prepareStatement("SELECT MAX(orderId) FROM orders")
      rs = ps.executeQuery()
      if (rs.next()) {
        maxId = rs.getLong(1)
      }
      if (maxId > 0) {
        ps.clearWarnings()
        ps = connection.prepareStatement(sql)
        ps.setLong(1, maxId + incr)
        ps.setString(2, (maxId + incr) + getFixLenthString(5).toString)
        ps.setLong(3, shopIds((Math.random() * 11 + 1).toInt))
        ps.setLong(4, userIds((Math.random() * 11 + 1).toInt))
        ps.setInt(5, orderStatus((Math.random() * 5 + 1).toInt))
        val money: Double = Math.random() * 999999
        ps.setDouble(6, money)
        ps.setInt(7, if (incr / 2 == 0) 0 else 1)
        ps.setDouble(8, money)
        ps.setDouble(9, money)
        ps.setDouble(10, money)
        val pay = userIds((Math.random() * 5 + 1).toInt)
        ps.setInt(11, pay)
        //        ps.setString(12, pay + "")
        ps.setInt(12, if (incr / 2 == 0) 0 else 1)
        val areaId = areaIds((Math.random() * 34 + 1).toInt)
        ps.setInt(13, areaId)
        ps.setString(14, areaId + "")
        ps.setString(15, usernames((Math.random() * 19 + 1).toInt % (usernames.size - 1)))
        ps.setString(16, userAddrs((Math.random() * 14 + 1).toInt % (userAddrs.size - 1)))
        ps.setString(17, userPhones((Math.random() * 24 + 1).toInt))
        ps.setInt(18, (100 - 0) * new Random().nextInt())
        ps.setInt(19, if (incr / 2 == 0) 0 else 1)
        ps.setString(20, null)
        ps.setString(21, orderRemarks((Math.random() * 3 + 1).toInt % (orderRemarks.size - 1)))
        ps.setInt(22, orderSrcs((Math.random() * 4 + 1).toInt))
        ps.setDouble(23, Math.floor(Math.random() + 1d))
        ps.setInt(24, if (incr / 2 == 0) 0 else 1)
        ps.setInt(25, 0)
        ps.setInt(26, if (incr / 2 == 0) 0 else 1)
        ps.setInt(27, if (incr / 2 == 0) 0 else 1)
        ps.setInt(28, 0)
        ps.setInt(29, 0)
        ps.setString(30, null)
        ps.setInt(31, 0)
        ps.setString(32, searchKeys((Math.random() * 6 + 1).toInt))
        val uuid = UUID.randomUUID()
        ps.setString(33, uuid.toString())
        val currentTimeMillis = System.currentTimeMillis()
        ps.setString(34, formater.format(new Date(currentTimeMillis)))
        ps.setString(35, formater.format(new Date(currentTimeMillis + 1000)))
        ps.setString(36, uuid.toString())
        ps.setInt(37, 1)
        ps.setString(38, formater.format(new Date(currentTimeMillis + 30000)))
        ps.setInt(39, if (incr / 2 == 0) 0 else 1)
        ps.setDouble(40, new Random().nextInt(20) % (20 - 2 + 1) + 2)
        ps.setDouble(41, 0.00)
        ps.setInt(42, 0)
        ps.setString(43, "order")
        ps.setString(44, null)
        ps.setInt(45, 0)
        ps.setInt(46, 0)
        ps.setString(47, null)
        ps.setDouble(48, 0.00)
        ps.setString(49, new Date().getTime + "")
        ps.setInt(50, 0)
        ps.setInt(51, 1)
        ps.executeUpdate()

        val resultset = ps.executeQuery("SELECT LAST_INSERT_ID() as orderId")
        if(resultset.next()) {
          val orderId = resultset.getLong("orderId")
          insertOrderGoods(connection, maxId + incr)
        }
      }
    } catch {
      case e: SQLException => e.printStackTrace()
    } finally {
      try {
        if (null != ps) {
          ps.close()
        }
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }
  }

  def insertOrderGoods(connection: Connection, orderId:Long) = {
    var maxId: Long = 0
    var ps: PreparedStatement = null
    var rs: ResultSet = null
    val sql =
      """
        |insert into order_goods (
        |  `ogId`,
        |  `orderId`,
        |  `goodsId`,
        |  `goodsNum`,
        |  `goodsPrice`,
        |  `goodsName`,
        |  `goodsImg`,
        |  `goodsType`
        |)
        |values
        |  (
        |    null,?,?,?,?,'','',0
        |  )
      """.stripMargin

    ps = connection.prepareStatement(sql)
    ps.setLong(1, orderId)
    ps.setLong(2, RandomUtils.nextInt(100101, 100228))
    ps.setInt(3,RandomUtils.nextInt(1,30))
    ps.setDouble(4, RandomUtils.nextDouble(101.0, 10000.9))

    ps.executeUpdate()
  }

  /**
   * 获取固定的长度的随机数
   *
   * @param strLength
   * @return
   */
  def getFixLenthString(strLength: Int) = {
    val rm = new Random()
    // 获得随机数
    val pross: Double = (1 + rm.nextDouble()) * Math.pow(10, strLength)
    // 将获得的获得随机数转化为字符串
    val fixLenthString = String.valueOf(pross)
    var result = ""

    // 返回固定的长度的随机数
    if (strLength + 1 > fixLenthString.length()) {
      fixLenthString.substring(1, strLength).replace(".", "").toString
    } else {
      fixLenthString.substring(1, strLength + 1).replace(".", "").toString
    }
  }


  /**
   * 生成IP
   *
   * @return
   */
  def randomIp() = {
    // 需要排除监控的ip范围
    val range = Array[Array[Int]](Array[Int](607649792, 608174079), // 36.56.0.0-36.63.255.255
      Array[Int](1038614528, 1039007743), // 61.232.0.0-61.237.255.255
      Array[Int](1783627776, 1784676351), // 106.80.0.0-106.95.255.255
      Array[Int](2035023872, 2035154943), // 121.76.0.0-121.77.255.255
      Array[Int](2078801920, 2079064063), // 123.232.0.0-123.235.255.255
      Array[Int](-1950089216, -1948778497), // 139.196.0.0-139.215.255.255
      Array[Int](-1425539072, -1425014785), // 171.8.0.0-171.15.255.255
      Array[Int](-1236271104, -1235419137), // 182.80.0.0-182.92.255.255
      Array[Int](-770113536, -768606209), // 210.25.0.0-210.47.255.255
      Array[Int](-569376768, -564133889) // 222.16.0.0-222.95.255.255
    )
    val rdint = new Random()
    val index = rdint.nextInt(10)
    val ip = num2ip(range(index)(0) + new Random().nextInt(range(index)(1) - range(index)(0)))
    ip
  }

  /**
   * 将十进制转换成IP地址
   *
   * @param ip
   * @return
   */
  def num2ip(ip: Int) = {
    var b = new Array[Int](4)
    var x = ""
    b(0) = ((ip >> 24) & 0xff).toInt
    b(1) = ((ip >> 16) & 0xff).toInt
    b(2) = ((ip >> 8) & 0xff).toInt
    b(3) = (ip & 0xff).toInt
    x = Integer.toString(b(0)) + "." + Integer.toString(b(1)) + "." +
      Integer.toString(b(2)) + "." + Integer.toString(b(3))
    x
  }

  def info(log: Any*): Unit = {
    for (i <- log) {
      print(log + " ")
    }
    println()
  }
}
