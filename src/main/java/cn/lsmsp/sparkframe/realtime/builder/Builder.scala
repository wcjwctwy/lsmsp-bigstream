package cn.lsmsp.sparkframe.realtime.builder

import cn.lsmsp.sparkframe.common.log.Logging
import cn.lsmsp.sparkframe.realtime.entity.{AppContext, JobContext}


/**
  * Created by wangcongjun on 2017/6/26.
  */
trait Builder extends Logging{
    def create():Any={}
    def create(appContext: AppContext, jobContext: JobContext):Any={}
    def create( jobContext: JobContext):Any={}
}
