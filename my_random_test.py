import random
import datetime as dt

FROM_DATE = "2017-08-01T00:00:01"
TO_DATE = "2017-08-15T23:59:00"
DT_FORMAT = '%Y-%m-%dT%H:%M:%S'
HOUR = 3600

dtfrom = dt.datetime.strptime(FROM_DATE, DT_FORMAT)
dtto = dt.datetime.strptime(TO_DATE, DT_FORMAT)

start = dt.datetime.timestamp(dtfrom)
end = dt.datetime.timestamp(dtto)

qry_start = random.randint(start, end)
qry_end = qry_start + HOUR

qry_start_str = dt.datetime.fromtimestamp(qry_start).strftime(DT_FORMAT)
qry_end_str = dt.datetime.fromtimestamp(qry_end).strftime(DT_FORMAT)


print(start, end)
print(qry_start, qry_end)
print(qry_start_str, qry_end_str)

