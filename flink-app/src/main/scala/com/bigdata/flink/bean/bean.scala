package com.bigdata.flink

package object bean {
  // 用户行为数据对象
  case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

  // 商品点击数量对象
  case class ItemClickCount(itemId: Long, count: Long, windowEndTime: Long)

  // 服务器日志数据对象
  case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

  // URL点击数量对象
  case class URLClickCount(url: String, count: Long, windowEndTime: Long)

  // 应用市场推广数据
  case class AppMarketingData(marketing: String, behavior: String, timestamp: Long)

  case class AppMarketingCount(marketing: String, behavior: String, count: Long)

  // 广告点击日志数据
  case class AdvClickData(userId: Long, advId: Long, province: String, city: String, timestamp: Long)

  case class AdvClickCount(userId: Long, advId: Long, count: Long)

  // 登陆日志数据
  case class LoginLogData(userId: Long, ip: String, status: String, timestamp: Long)

  // 订单日志数据
  case class OrderLogData(orderId: Long, status: String, txId: String, timestamp: Long)

  // 交易日志数据
  case class TXLogData(txId: String, channel: String, timestamp: Long)

}
