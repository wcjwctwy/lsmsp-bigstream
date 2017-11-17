package cn.lsmsp.sparkframe.transform.decode.adaptor

import cn.lsmsp.sparkframe.common.util.DateUtils
import cn.lsmsp.sparkframe.transform.decode.EventMeta


/**
  * Created by wangcongjun on 2017/7/7.
  */
class FlumeEventDecoder extends DecoderAdaptor[Map[String, String]] {
  override def adaptorDecoder(array: Array[Byte]): Map[String, String] = {
    val msg = new String(array).split("\t")
    val schema = "sid:cusid:collectname:eventtype:analyzerid:isanalyzerevent:analyzereventidlist:eventname:eventmessage:eventcount:eventlevel:eventresult:rawevent:collectidlist:extendid:issourceattacker:eventcategory:eventcategorybehavior:eventcategorytechnique:categorydevice:attackfashionlevel:eventfeature:eventcategoryobject:bytesin:bytesout:devicereceipttime:collectreceipttime:socreceipttime:eventstarttime:eventendtime:srcaddress:srctype:srcdnsdomain:srchostname:srcmac:srcnatdomain:srcport:srcprocessname:srcservicename:srcinterfaceisspoofed:srcinterface:srctransaddress:srctransport:srctranszone:srcuserid:srcusername:srcuserprivileges:srcusertype:srczone:taraddress:tartype:tardnsdomain:tarhostname:tarmac:tarnatdomain:tarport:tarprocessname:tarservicename:tarinterfaceisspoofed:tarinterface:tartransaddress:tartransport:tartranszone:taruserid:tarusername:taruserprivileges:tarusertype:tarzone:appprotocol:transprotocol:filepath:filename:filecreatetime:filemodifytime:fileaccresstime:filesize:disksize:filesystemtype:fileid:filehash:filetype:fileaccressprivileges:oldfilepath:oldfilename:oldfilecreatetime:oldfilemodifytime:oldfileaccresstime:oldfilesize:olddisksize:oldfilesystemtype:oldfileid:oldfilehash:oldfiletype:oldfileaccressprivileges:deviceaddress:devicetransaddress:deviceaction:devicednsdomain:devicedomain:devicehostname:devicemac:deviceeventlevel:deviceinboundinterface:deviceoutboundinterface:devicetransdomain:deviceprocessname:deviceproduct:devicetimezone:devicezone:devicetranszone:reqclientapp:httpreqcontext:httpcookie:reqmethod:repcode:apppath:appparam:appvariable:reqname:requrl:reqaction:referenceorigin:referencemeaning:referencename:referenceurl:entid".split(":")
    schema.zip(msg).map(x => {
      if (EventMeta.string2Long.contains(x._1)) {
        val date = DateUtils.converString2Date(x._2).getTime
        (x._1,date.toString)
      } else {
        x
      }

    }).toMap
  }
}
