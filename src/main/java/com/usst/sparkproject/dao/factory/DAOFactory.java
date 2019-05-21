package com.usst.sparkproject.dao.factory;

import com.usst.sparkproject.dao.ITaskDAO;
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
}
