package com.usst.sparkproject.dao.factory;

import com.usst.sparkproject.dao.ISessionAggrStatDAO;
import com.usst.sparkproject.dao.ISessionDetailDAO;
import com.usst.sparkproject.dao.ISessionRandomExtractDAO;
import com.usst.sparkproject.dao.ITaskDAO;
import com.usst.sparkproject.dao.ITop10CategoryDAO;
import com.usst.sparkproject.dao.impl.ISessionAggrStatDAOImpl;
import com.usst.sparkproject.dao.impl.ITop10CategoryDAOImpl;
import com.usst.sparkproject.dao.impl.SessionDetailDAOImpl;
import com.usst.sparkproject.dao.impl.SessionRandomExtractDAOImpl;
import com.usst.sparkproject.dao.impl.TaskDAOImpl;

/**
 * DAO工厂类
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
}
