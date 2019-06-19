package com.usst.sparkproject.dao.impl;

import com.usst.sparkproject.dao.IPageSplitConvertRateDAO;
import com.usst.sparkproject.domain.PageSplitConvertRate;
import com.usst.sparkproject.jdbc.JDBCHelper;

public class PageSplitConvertRateDAOImpl implements IPageSplitConvertRateDAO {
	
	@Override
	public void insert(PageSplitConvertRate pageSplitConvertRate) {
		String sql = "insert into page_split_convert_rate values(?,?)";  
		Object[] params = new Object[]{pageSplitConvertRate.getTaskid(), 
				pageSplitConvertRate.getConvertRate()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}


}
