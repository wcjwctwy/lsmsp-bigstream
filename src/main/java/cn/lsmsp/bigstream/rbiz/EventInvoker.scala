package cn.lsmsp.bigstream.rbiz

import cn.lsmsp.sparkframe.realtime.RealtimeBusi

/**
  * Created by wangcongjun on 2017/6/14.
  */
object EventInvoker extends RealtimeBusi{
  def main(args: Array[String]): Unit = {
    var jobName:String="default"
    if(args.length>0){
      jobName = args(0)
    }
    start()
    //    RestUtils.post("http://10.20.1.32/bigdata/saveevent","data=9527")
//    val name = "SID::CusID::CollectName::EventType::IsAnalyzerEvent::AnalyzerID::AnalyzerEventIDList::EventName::EventMessage::EventCount::EventLevel::EventResult::RawEvent::CollectIDList::ExtendID::IsSourceAttacker::EventCategory::EventCategoryBehavior::EventCategoryTechnique::CategoryDevice::AttackFashionLevel::EventFeature::EventCategoryObject::BytesIn::BytesOut::DeviceReceiptTime::CollectReceiptTime::SOCReceiptTime::EventStartTime::EventEndTime::SrcAddress::SrcType::SrcDNSDomain::SrcHostName::SrcMAC::SrcNATDomain::SrcPort::SrcProcessName::SrcServiceName::SrcInterfaceIsSpoofed::SrcInterface::SrcTransAddress::SrcTransPort::SrcTransZone::SrcUserID::SrcUserName::SrcUserPrivileges::SrcUserType::SrcZone::TarAddress::TarType::TarDNSDomain::TarHostName::TarMAC::TarNATDomain::TarPort::TarProcessName::TarServiceName::TarInterfaceIsSpoofed::TarInterface::TarTransAddress::TarTransPort::TarTransZone::TarUserID::TarUserName::TarUserPrivileges::TarUserType::TarZone::AppProtocol::TransProtocol::FilePath::FileName::FileCreateTime::FileModifyTime::FileAccressTime::FileSize::DiskSize::FileSystemType::FileID::FileHash::FileType::FileAccressPrivileges::OldFilePath::OldFileName::OldFileCreateTime::OldFileModifyTime::OldFileAccressTime::OldFileSize::OldDiskSize::OldFileSystemType::OldFileID::OldFileHash::OldFileType::OldFileAccressPrivileges::DeviceAddress::DeviceTransAddress::DeviceAction::DeviceDNSDomain::DeviceDomain::DeviceHostName::DeviceMAC::DeviceEventLevel::DeviceInBoundInterface::DeviceOutBoundInterface::DeviceTransDomain::DeviceProcessName::DeviceProduct::DeviceTimeZone::DeviceZone::DeviceTransZone::ReqClientApp::HttpReqContext::HttpCookie::ReqMethod::RepCode::AppPath::AppParam::AppVariable::ReqName::ReqURL::ReqAction::ReferenceOrigin::ReferenceMeaning::ReferenceName::ReferenceURL"
//    val value = "e2348aeb-fabf-4414-8b3a-66eef4e38a4a\t6390\tlsmsp_collecter\t1\t0\t1\t\tUnrecognized Log\tUnrecognized Log,please provide the regex config!\t1\tinfo\tunknown\t<166>2017-06-08T12:02:53.831Z liancheng01 Hostd: [623E2B70 warning ‘Hostsvc.VFlashManager‘ opID=hostd-7f00 user=root] GetVFlashResourceRuntimeInfo: vFlash is not licensed, not supported\t\t\ttrue\t/Others\t/Others\t/Others\t010601\tfalse\t0\t/Others\t0\t0\t2017-06-08 20:02:57\t2017-06-08 20:02:57\t2017-06-08 20:02:57\t20170608200257\t20170608200257\t\t\t\t\t\t\t\t\t\tfalse\t\t\t0\t\t0\t\t\t\t\t\t\t\t\t\t\t\t\t\tfalse\t\t\t0\t\t0\t\t\t\t\t\t\t\t\t2017-06-08 20:02:57\t2017-06-08 20:02:57\t2017-06-08 20:02:57\t0.0\t0.0\t\t\t\t\t\t\t\t2017-06-08 20:02:57\t2017-06-08 20:02:57\t2017-06-08 20:02:57\t0.0\t0.0\t\t\t\t\t\t10.20.4.231\t\t\t1\t67\tvm231\t\t\t\t\t\t173\t\t\t244\t345\t\t\t\t\t\t\t\t\t\t\t\t\t\t1\t\t1"
//    println(name.split("::").zip(value.split("\t")).toMap.toString().replace(",","\n"))
  }
}
