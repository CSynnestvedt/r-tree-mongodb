/**
*    Copyright (C) 2015 LIESMARS, Wuhan University.
*    Financially supported by Wuda Geoinfamatics Co. ,Ltd.
*    Author:  Xiang Longgang, Wang Dehao , Shao Xiaotian
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#pragma once

#include <iostream>
#include <vector>

#define RTREE_CURSOR_TEMPLATE template <typename Key>

namespace rtree_index
{
	RTREE_CURSOR_TEMPLATE
	class RTreeCursor
	{
	public:
		RTreeCursor(std::vector<Key> resultKeys, std::string DB_NAME, std::string STORAGE_NAME)
		{
			_DB_NAME = DB_NAME;
			_STORAGE_NAME = STORAGE_NAME;
			allKeys = resultKeys;
			currentNum = 0;
			MaxNum = allKeys.size();
		}
		int Count()
		{
			return MaxNum;
		}
		std::string _DB_NAME;
		std::string _STORAGE_NAME;
		int currentNum;
		int MaxNum;
		std::vector<Key> allKeys;
	};
}
