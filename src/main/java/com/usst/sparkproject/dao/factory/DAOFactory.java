package com.usst.sparkproject.dao.factory;

import com.usst.sparkproject.dao.IAdBlacklistDAO;
import com.usst.sparkproject.dao.IAdClickTrendDAO;
import com.usst.sparkproject.dao.IAdProvinceTop3DAO;
import com.usst.sparkproject.dao.IAdStatDAO;
import com.usst.sparkproject.dao.IAdUserClickCountDAO;
import com.usst.sparkproject.dao.IPageSplitConvertRateDAO;
import com.usst.sparkproject.dao.ISessionAggrStatDAO;
import com.usst.sparkproject.dao.ISessionDetailDAO;
import com.usst.sparkproject.dao.ISessionRandomExtractDAO;
import com.usst.sparkproject.dao.ITaskDAO;
import com.usst.sparkproject.dao.ITop10CategoryDAO;
import com.usst.sparkproject.dao.impl.AdBlacklistDAOImpl;
import com.usst.sparkproject.dao.impl.AdClickTrendDAOImpl;
import com.usst.sparkproject.dao.impl.AdProvinceTop3DAOImpl;
import com.usst.sparkproject.dao.impl.AdStatDAOImpl;
import com.usst.sparkproject.dao.impl.AdUserClickCountDAOImpl;
import com.usst.sparkproject.dao.impl.ISessionAggrStatDAOImpl;
import com.usst.sparkproject.dao.impl.ITop10CategoryDAOImpl;
import com.usst.sparkproject.dao.impl.PageSplitConvertRateDAOImpl;
import com.usst.sparkproject.dao.impl.SessionDetailDAOImpl;
import com.usst.sparkproject.dao.impl.SessionRandomExtractDAOImpl;
import com.usst.sparkproject.dao.impl.TaskDAOImpl;

/**
 * DAO工厂类
 * 
 * @author Administrator
 *
 */
public class DAOFactory {

	public static ITaskDAO getTaskDAO() {
		return new TaskDAOImpl();
	}

	public static ISessionAggrStatDAO getSessionAggrStatDAO() {
		return new ISessionAggrStatDAOImpl();
	}

	public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
		return new SessionRandomExtractDAOImpl();
	}

	public static ISessionDetailDAO getSessionDetailDAO() {
		return new SessionDetailDAOImpl();
	}

	public static ITop10CategoryDAO getTop10CategoryDAO() {
		return new ITop10CategoryDAOImpl();
	}

	public static IPageSplitConvertRateDAO getPageSplitConvertRateDAO() {
		return new PageSplitConvertRateDAOImpl();
	}

	public static IAdUserClickCountDAO getAdUserClickCountDAO() {
		return new AdUserClickCountDAOImpl();
	}

	public static IAdBlacklistDAO getAdBlacklistDAO() {
		return new AdBlacklistDAOImpl();
	}

	public static IAdStatDAO getAdStatDAO() {
		return new AdStatDAOImpl();
	}

	public static IAdProvinceTop3DAO getAdProvinceTop3DAO() {
		return new AdProvinceTop3DAOImpl();
	}

	public static IAdClickTrendDAO getAdClickTrendDAO() {
		return new AdClickTrendDAOImpl();
	}

}
