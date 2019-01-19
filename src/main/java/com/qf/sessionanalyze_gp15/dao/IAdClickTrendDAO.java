package com.qf.sessionanalyze_gp15.dao;

import com.qf.sessionanalyze_gp15.domain.AdClickTrend;

import java.util.List;

/**
 * 广告点击趋势DAO接口
 * @author Administrator
 *
 */
public interface IAdClickTrendDAO {

	void updateBatch(List<AdClickTrend> adClickTrends);
	
}
