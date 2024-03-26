package com.superior.datatunnel.api;

import com.gitee.melin.bee.core.extension.SPI;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author melin 2021/7/27 10:47 上午
 */
@SPI
public interface DistCpAction extends Serializable {

    void run(DistCpContext context) throws IOException;

}
