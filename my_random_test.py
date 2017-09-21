import random

start = 1503187201
end = 1505951999
hour = 3600

qry_start = random.randint(start, end)
qry_end = qry_start + hour

print(start, end)
print(qry_start, qry_end)