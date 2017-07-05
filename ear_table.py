from pyspark import SparkContext, SparkConf

conf = (SparkConf()
        .setMaster("local")
        .setAppName("My app")
        .set("spark.executor.memory", "1g"))
sc = SparkContext(conf=conf)

#pretotal_fee, start_dest_distance, time_cost, area, iscreate
def filter_feats(line):
    ret = True
    line = line.split('\t')
    line = [line[ind] for ind in [12, 11, 15, 2, 6]]
    try:
        [float(i) for i in line]
    except:
        print "cannot convert to float"
        ret = False
        pass
    return ret

def cross_feats(line):
    line = line.split('\t')
    line = [line[ind] for ind in [12, 11, 15, 2, 6]]
    return line

def xxx_fee(feat_nb, rdd_src):
    #rdd_src = sc.textFile('/user/bigdata_driver_ecosys_test/cgb/price_fe/bubble_basic_combinefe/2017/05/*')
    rdd_stat = rdd_src.filter(filter_feats)
    print "get the needed feats"
    rdd_stat = rdd_stat.map(cross_feats)
    rdd = rdd_stat.map(lambda line: [int(float(line[feat_nb])), int(float(line[0])), float(line[4])])
    rdd = rdd.map(lambda line: ((line[0], line[1]), (line[2], 1.0)))
    rdd = rdd.reduceByKey(lambda val1, val2: (val1[0]+val2[0], val1[1]+val2[1]))
    rdd = rdd.map(lambda line: (line[0][0], line[0][1], line[1][0]/line[1][1]))
    return rdd

if __name__=="__main__":
    rdd_stat = sc.textFile('/user/bigdata_driver_ecosys_test/cgb/price_fe/bubble_basic_combinefe/2017/05/*')
    rdd_dis_fee = xxx_fee(1, rdd_stat)
    rdd_time_fee = xxx_fee(2, rdd_stat)
    rdd_area_fee = xxx_fee(3, rdd_stat)
