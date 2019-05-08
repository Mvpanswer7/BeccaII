package xmatrix.core

/**
  * Created by iodone on {18-5-10}.
  */

import xmatrix.core.platform.PlatformManager
import xmatrix.common.ParamsUtil


object XmatrixApp {

  def main(args: Array[String]): Unit = {
    val params = new ParamsUtil(args)
    require(params.hasParam("xmatrix.name"), "Application name should be set")
    PlatformManager.getOrCreate.run(params)
  }

}

class XmatrixApp
