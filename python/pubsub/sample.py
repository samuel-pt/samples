# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START app]
import base64
import json
import logging
import os
import time
import datetime


# Global list to storage messages received by this instance.
def ymd_between_hql_clause(d1, n_days):
    """
        Accepts a d1::string (YYYY-MM-DD) and n_days::int

        Returns a year, month, day hql clause to pull
        data from d1 back n_days. Also returns YYYY-MM-DD
        representing to the date the cohort was created.

        Can only handle boundaries across two months

    """

    year, month, day = d1.split("-")
    finish_date = datetime.date(int(year), int(month), int(day))
    start_date = finish_date - datetime.timedelta(days=n_days)

    query_dates = [finish_date - datetime.timedelta(days=x)
                   for x in xrange(0, int(n_days))]

    print query_dates, start_date, finish_date

    # Check if dates go across months
    if query_dates[0].strftime('%m') == query_dates[-1].strftime('%m') and \
       query_dates[0].strftime('%Y') == query_dates[-1].strftime('%Y'):
       # Only need a single list to track all dates
       days = [x.strftime('%d') for x in query_dates]

       year_clause = "year = '{0}'".format(year)
       month_clause = "month = '{0}'".format(month)
       day_clause = "day in ({0})".format(",".join("'"+str(item)+"'" for item in days))

       ymd_clause = "({0} and {1} and {2})".format(year_clause
                                         , month_clause, day_clause)

    else:
       # Need to generate two lists split on where the month changes
       query_dates1 = [x for x in query_dates if x.strftime('%m')
                                == query_dates[0].strftime('%m')]
       query_dates2 = [x for x in query_dates if x.strftime('%m')
                                != query_dates[0].strftime('%m')]

       days1 = [x.strftime('%d') for x in query_dates1]
       year_clause1 = "year = '{0}'".format(year)
       month_clause1 = "month = '{0}'".format(month)
       day_clause1 = "day in ({0})".format(",".join("'"+str(item)+"'" for item in days1))
       ymd_clause1 = year_clause1 + " and " + month_clause1 + " and " + day_clause1

       days2 = [x.strftime('%d') for x in query_dates2]
       year_clause2 = "year = '{0}'".format(query_dates2[0].strftime('%Y'))
       month_clause2 = "month = '{0}'".format(query_dates2[0].strftime('%m'))
       day_clause2 = "day in ({0})".format(",".join("'"+str(item)+"'" for item in days2))
       ymd_clause2 = year_clause2 + " and " + month_clause2 + " and " + day_clause2

       ymd_clause = "((" + ymd_clause1 + ") OR (" + ymd_clause2 + "))"


    return ymd_clause, start_date.strftime('%Y-%m-%d')

if __name__ == '__main__':
    ret_ymd_clause, dt_cohort_created = ymd_between_hql_clause('2017-06-14', 1)
    print(ret_ymd_clause)
    print(dt_cohort_created)
    
# [END app]
