
import pyspark
from pyspark import SparkContext
import os
import sys
from collections import defaultdict
from itertools import combinations
from math import sqrt, pow
import csv
import time

start_time = time.time()
sc = SparkContext('local','example')
fn1 = sys.argv[-1]
fn2 = sys.argv[-2]
if os.path.exists(fn1):
    rdd_tst = sc.textFile(fn1)
if os.path.exists(fn2):
    rdd_trn = sc.textFile(fn2)

rdd_trn = rdd_trn.mapPartitions(lambda x: csv.reader(x))
rdd_tst = rdd_tst.mapPartitions(lambda x: csv.reader(x))
def remove_header(itr_index, itr):
    return iter(list(itr)[1:]) if itr_index == 0 else itr
def train_el(items):
    return ((items[0],items[1]) ,float(items[2]))
def test_el(items):
    return ((items[0], items[1]), float(-1))
rdd_train_org = rdd_trn.mapPartitionsWithIndex(remove_header)
rdd_train_org = rdd_train_org.map(train_el)
rdd_test = rdd_tst.mapPartitionsWithIndex(remove_header)
rdd_test = rdd_test.map(test_el)

group_by = rdd_train_org.map(lambda x: (x[0][0], (x[0][1],x[1]))).groupByKey().map(lambda x : (x[0], list(x[1]))).collectAsMap()
for k, v in group_by.iteritems():
    group_by[k] = dict((x,y) for x, y in v)
compare_dct = rdd_train_org.collectAsMap()

def find_med(iterator):
    med_d = {}
    for x in iterator:
        s_lst = sorted (x[1])
        lng = len(s_lst)
        index = (lng - 1) // 2
        if (lng % 2):
            med_d[x[0]]= float(s_lst[index])
        else:
            med_d[x[0]] = float((s_lst[index] + s_lst[index + 1])/2.0)
    return med_d.items()

dctt_test= rdd_test.collectAsMap()
rdd_uu = rdd_train_org.subtractByKey(rdd_test).map(lambda x: (x[0][0], x[1]))
med_uu = rdd_uu.groupByKey().map(lambda x : (x[0], list(x[1]))).mapPartitions(find_med).collectAsMap()
aTuple = (0,0)
rdd_uu = rdd_uu.aggregateByKey(aTuple, lambda a,b: (a[0] + b, a[1] + 1), lambda a,b: (a[0] + b[0], a[1] + b[1]))
dct_uu = rdd_uu.mapValues(lambda v: float(v[0])/v[1]).collectAsMap()
dctt_trn_org = rdd_train_org.collectAsMap()
for k in iter(dctt_test.keys()):
    a=k[0]
    dctt_trn_org[k] = dct_uu[a]

ser = dctt_trn_org.items()
rdd_train = sc.parallelize(ser,10)

def make_avg_rdd(items):
    return items[0][1], float(items[1])

avg_rdd = rdd_train.map(make_avg_rdd)

aTuple = (0,0)
avg_rdd = avg_rdd.aggregateByKey(aTuple, lambda a,b: (a[0] + b, a[1] + 1), lambda a,b: (a[0] + b[0], a[1] + b[1]))

finalResult = avg_rdd.mapValues(lambda v: float(v[0])/v[1]).collectAsMap()

def mk_mv_key(items):
    return items[0][1], (items[0][0] , items[1])

def normz(groups):
    keys = groups[0]
    valuess = groups[1]
    sub_value = float(finalResult[keys])
    new_v = []
    for x in valuess:
        (a,b) = x
        b = float(b - sub_value)
        new_v.append((a,b))
    return keys, new_v
    
norm_rdd_train = rdd_train.map(mk_mv_key).groupByKey().map(lambda x: normz(x))

def ungroup(x):
    return x

norm_rdd_train = norm_rdd_train.flatMap(lambda data: [(data[0], x) for x in data[1]])

def findd(iterator):
    cor_dct = defaultdict(list)
    for k in iterator:
       
        (user_test,movie_test) = (k[0],k[1])
        candidate = [i for i in dct_group_by_item[user_test].keys() if i != movie_test]
        for c in candidate:
          
            sums = 0
            cand_denom = 0
            mv_denom = 0
            
            for com_user in dct_group_by_user[c].viewkeys() & dct_group_by_user[movie_test].viewkeys():
                if com_user == user_test:
                    continue
                sums += dct_group_by_user[c][com_user] * dct_group_by_user[movie_test][com_user]
                cand_denom += dct_group_by_user[c][com_user] ** 2
                mv_denom += dct_group_by_user[movie_test][com_user] ** 2
                                
            denom = (sqrt(cand_denom)) * (sqrt(mv_denom))
            if denom != 0:
                cor = float(sums) / denom
            else:
                cor = 0
            if cor > 0:
                cor_dct[movie_test].append((c, cor))
    return cor_dct.items()     

group_by_user = norm_rdd_train.groupByKey().map(lambda x : (x[0], list(x[1])))
dct_group_by_user = group_by_user.collectAsMap()
for k, v in dct_group_by_user.iteritems():
    dct_group_by_user[k] = dict((x,y) for x, y in v) 

group_by_item = norm_rdd_train.map(lambda x: (x[1][0],(x[0],x[1][1]))).groupByKey().map(lambda x : (x[0], list(x[1])))
dct_group_by_item = group_by_item.collectAsMap()
for k, v in dct_group_by_item.iteritems():
    dct_group_by_item[k] = dict((x,y) for x, y in v)

test_dct = rdd_test.map(lambda x: (x[0][0],x[0][1])).mapPartitions(findd)

cor_dct = test_dct.collectAsMap()
for k in cor_dct.iterkeys():
    cor_dct[k] = dict((x,y) for x, y in cor_dct[k]) 

def get_result(iterator):
    result_dct = {}
    for y in iterator:
        (user,movie) = (y[0],y[1])
        nome = 0
        denome = 0
        comp_lst = []
        if movie in cor_dct and user in group_by:
            for ke in cor_dct[movie].viewkeys() & group_by[user].viewkeys():
                if ke == movie:
                    continue
                comp_lst.append((ke, cor_dct[movie][ke]))
            comp_lst = sorted(comp_lst,key=lambda x: x[1], reverse=True)           
            cnt = 0
            for b in comp_lst:
                if b[1] >= 0.95:
                    cnt+= 1
                    nome += group_by[user][b[0]] * b[1]
                    denome += b[1]
                elif b[1] >= 0.9:
                    cnt+= 1
                    nome += group_by[user][b[0]] * b[1]
                    denome += b[1]
                elif b[1] >= 0.85:
                    cnt+= 1
                    nome += group_by[user][b[0]] * b[1]
                    denome += b[1]
                elif b[1] >= 0.8:
                    cnt+= 1
                    nome += group_by[user][b[0]] * b[1]
                    denome += b[1]
                elif b[1] >= 0.75:
                    cnt+= 1
                    nome += group_by[user][b[0]] * b[1]
                    denome += b[1]
                elif b[1] >= 0.7:
                    cnt+= 1
                    nome += group_by[user][b[0]] * b[1]
                    denome += b[1]
                elif b[1] >= 0.65:
                    cnt+= 1
                    nome += group_by[user][b[0]] * b[1]
                    denome += b[1]
                elif cnt < 4:
                    cnt+= 1
                    nome += group_by[user][b[0]] * b[1]
                    denome += b[1]
            if denome != 0:
                pred = float(nome) / denome
                if 0<=pred<=5:
                    result_dct[y] = pred
                else:
                    result_dct[y] = med_uu[user]
                #result_dct[y] = pred
            else:
                result_dct[y] = med_uu[user]
        else:
            result_dct[y] = med_uu[user]
            
    return result_dct.items()

result_dctt= rdd_test.map(lambda x: (x[0][0],x[0][1])).mapPartitions(get_result).collectAsMap()

summ = 0
nn = len(result_dctt.keys())
(g1,g2,g3,g4,g5)= (0,0,0,0,0)
for q in dctt_test.iterkeys():
    if q in result_dctt:
        dff = abs(result_dctt[q]- compare_dct[q])
        if 0<=dff<1:
            g1 += 1
        elif 1<=dff<2:
            g2 += 1
        elif 2<=dff<3:
            g3 += 1
        elif 3<=dff <4:
            g4 += 1
        else:
            g5 += 1
        summ += (dff ** 2)

rm = sqrt(float(summ) / nn)

pr_lst = []
for ky,vl in result_dctt.iteritems():
    pr_lst.append((int(ky[0]),int(ky[1]),vl))
pr_lst = sorted(pr_lst,key=lambda x: (x[0], x[1]))
pr_lst = [(str(i[0]),str(i[1]),str(i[2])) for i in pr_lst]

with open('Output.txt', 'w') as f:
    f.write("UserID, MovieID, Pred_rating\n")
    f.write('\n'.join('%s,%s,%s' %x for x in pr_lst))
print ">=0 and <1: %d" %g1
print ">=1 and <2: %d" %g2
print ">=2 and <3: %d" %g3
print ">=3 and <4: %d" %g4
print ">=4: %d" %g5
print "RMSE = %.16f" %rm
print("The total execution time taken is %s sec." % (time.time() - start_time))
