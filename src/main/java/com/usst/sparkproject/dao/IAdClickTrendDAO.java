package com.usst.sparkproject.dao;

import java.util.List;

import com.usst.sparkproject.domain.AdClickTrend;

public interface IAdClickTrendDAO {
	
	void updateBatch(List<AdClickTrend> adClickTrends);

}
