#Script to set up MapReduce Job on Google Cloud DataProc Service
import os
import time

def setup():
    os.system('sudo chmod +x /home/warrenwilliams1996/mapper.py')
    os.system('sudo chmod +x /home/warrenwilliams1996/reducer.py')
    os.system('hdfs dfs -put /home/warrenwilliams1996/bankreviews.txt /user/hdfs')
    print('\nWaiting for MapReduce program to complete...')
    oldtime = time.time()
    os.system('hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -file ./mapper.py -mapper ./mapper.py -file ./reducer.py -reducer ./reducer.py -input /user/hdfs/bankreviews.txt -output /user/hdfs/results')
    newtime = time.time()
    print(f'Job Time: {int(newtime-oldtime)} Seconds')
    print('MapReduce Done')
    os.system('hdfs dfs -ls /user/hdfs/results')
    os.system('hdfs dfs -tail /user/hdfs/results/part-00000')
    os.system('hdfs dfs -cat /user/hdfs/results/part-00000 | sort -k2 -nr')


def main():
    print("Setting up MapReduce program\n")
    setup()
    print("\nCompleted")

if __name__ == '__main__':
    main()