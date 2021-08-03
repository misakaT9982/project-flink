package com.bigdata.education

/**
 * Ciel
 * project-flink: com.bigdata.education.model
 * 2020-08-24 19:17:39
 */
package object model {

    case class BaseAd(adid: Int, adname: String, dn: String)

    case class BaseViplevel(vip_id: Int, vip_level: String, start_time: String,
                            end_time: String, last_modify_time: String, max_free: String,
                            min_free: String, next_level: String, operator: String, dn: String)

    case class BaseWebSite(siteid: Int, sitename: String, siteurl: String,
                           delete: String, createtime: String, creator: String, dn: String)

    case class DwdMember(uid: Int, ad_id: Int, birthday: String, email: String, fullname: String, iconurl: String,
                         lastlogin: String, mailaddr: String, memberlevel: String, password: String, paymoney: String, phone: String,
                         qq: String, register: String, regupdatetime: String, unitname: String, userip: String, zipcode: String, dt: String,
                         dn: String)

    case class DwdMemberPayMoney(uid: Int, var paymoney: String, siteid: Int, vip_id: Int, createtime: String, dt: String, dn: String)

    case class DwdMemberRegtype(uid: Int, appkey: String, appregurl: String, bdp_uuid: String, createtime: String,
                                isranreg: String, regsource: String, websiteid: String, dt: String, dn: String)


    case class ResultMode(uid: Int, ad_id: Int, birthday: String, email: String, fullname: String, iconurl: String,
                          lastlogin: String, mailaddr: String, memberlevel: String, password: String,
                          phone: String, qq: String, register: String, regupdatetime: String, unitname: String, userip: String,
                          zipcode: String, appkey: String, appregurl: String, bdp_uuid: String, regtype_createtime: String,
                          isranreg: String, regsource: String, websiteid: String, adname: String, siteid: String, sitename: String,
                          siteurl: String, site_createtime: String, paymoney: String, vip_id: String, vip_level: String,
                          vip_start_time: String, vip_end_time: String, last_modify_time: String, max_free: String, min_free: String,
                          next_level: String, operator: String, dn: String, dt: String)

    case class TopicAndValue(topic:String,value:String)

}
