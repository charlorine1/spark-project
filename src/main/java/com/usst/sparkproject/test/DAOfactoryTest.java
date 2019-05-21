package com.usst.sparkproject.test;

import com.usst.sparkproject.dao.ITaskDAO;
import com.usst.sparkproject.dao.factory.DAOFactory;
import com.usst.sparkproject.domain.Task;

public class DAOfactoryTest {

	public static void main(String[] args) {
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task findById = taskDAO.findById(1);
		System.out.println(findById);

	}

}
