#!/usr/bin/python

import argparse
import clickhouse_connect
import matplotlib.pyplot as plt
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.query import QueryResult


Q_PIPELINE_DETAIL = 'SELECT \
    thread_id,              \
    processor_name,         \
    processor_id,           \
    stage_type,             \
    start_ns - {start} AS start,  \
    end_ns - start_ns AS duration               \
    FROM pipeline_trace_log                     \
    WHERE (query_id = \'{id}\') AND             \
        ((start_ns != end_ns) OR (start_ns = {start}) OR (end_ns = {end}))    \
    ORDER BY start ASC'

Q_PIPELINE_SUMMARY = 'SELECT        \
    countDistinct(thread_id),       \
    countDistinct(processor_name),  \
    countDistinct(processor_id),    \
    countDistinct(stage_type),      \
    FROM pipeline_trace_log         \
    WHERE (query_id = \'{id}\') AND \
        ((start_ns != end_ns) OR (start_ns = {start}) OR (end_ns = {end}))'

Q_PIPELINE_START_END = 'SELECT \
    min(start_ns) AS start,    \
    max(end_ns) AS end         \
    FROM pipeline_trace_log    \
    WHERE query_id=\'{id}\''

class PipelineTraceData:
    threadNum:int       =0
    processorNum:int    =0
    processoridNum:int  =0
    stageNum:int        =0
    walltime:int        =0
    data:QueryResult    =None

    def __init__(self, threadNum, processorNum, processoridNum, stageNum, walltime, data):
        self.threadNum      = threadNum
        self.processorNum   = processorNum
        self.processoridNum = processoridNum
        self.stageNum       = stageNum
        self.walltime       = walltime
        self.data           = data
    

class GanttGraph:
    pipelineData:PipelineTraceData = None
    __ytickHight:int    = 3
    __taskAxis          = []
    __taskMap           = {}
    __colorMap          = {"r":{}, "g":{}, "b":{}}

    def __init__(self, data):
        self.pipelineData = data
        for i in range(self.pipelineData.threadNum):
            self.__taskAxis.append((self.__ytickHight*(i+1) - int(self.__ytickHight/2), self.__ytickHight-1))

    def __getColor(self, processor_name, processor_id, stage_type):
        if processor_name not in self.__colorMap["r"]:
            self.__colorMap["r"].update({processor_name: (len(self.__colorMap["r"])+1)*(1.0/(self.pipelineData.processorNum+1))})
        if str(processor_id) not in self.__colorMap["g"]:
            self.__colorMap["g"].update({str(processor_id): (len(self.__colorMap["g"])+1)*(1.0/(self.pipelineData.processoridNum+1))})
        if stage_type not in self.__colorMap["b"]:
            self.__colorMap["b"].update({stage_type: (len(self.__colorMap["b"])+1)*(1.0/(self.pipelineData.stageNum+1))})
        
        return (self.__colorMap["r"][processor_name], self.__colorMap["g"][str(processor_id)], self.__colorMap["b"][stage_type])

    def __updateAx(self, thread_id, processor_name, processor_id, stage_type, start, duration):
        if thread_id not in self.__taskMap:
            self.__taskMap[thread_id] = {
                "name": str(thread_id),
                "x":[], 
                "y":self.__taskAxis[len(self.__taskMap)],
                "color":[]}
        
        self.__taskMap[thread_id]["x"].append((start, duration))
        self.__taskMap[thread_id]["color"].append(self.__getColor(processor_name, processor_id, stage_type))

        return 

    def buildGraph(self):
        fig, ax = plt.subplots()
        
        fig.set_figheight(self.pipelineData.threadNum)
        fig.set_figwidth(80)

        for row in self.pipelineData.data:
            thread_id = row[0]
            processor_name = row[1]
            processor_id = row[2]
            stage_type = row[3]
            start= row[4]
            duration = row[5]

            axis = self.__updateAx(thread_id, processor_name, processor_id, stage_type, start, duration)
            
        for thread in self.__taskMap:
            value = self.__taskMap[thread]
            ax.broken_barh(value["x"], value["y"], facecolors=value["color"])

        ax.set_ylim(0, self.pipelineData.threadNum*self.__ytickHight+10)
        ax.set_xlim(0, self.pipelineData.walltime + 10)
        ax.set_yticks([self.__ytickHight*(i+1) for i in range(len(self.__taskMap))], labels=["thread id: " + str(id) for id in self.__taskMap])

        plt.legend()
        plt.savefig("gantt.svg")

def retrive_data(client: Client, query_id: str):
    qres = client.query(Q_PIPELINE_START_END.format(id=query_id))
    if qres.result_rows.count == 0:
        return
    
    start=qres.result_rows[0][0]
    end  =qres.result_rows[0][1]
    qres = client.query(Q_PIPELINE_SUMMARY.format(id=query_id, start=start, end=end))
    if qres.result_rows.count == 0:
        return

    threadNum       = qres.result_rows[0][0]
    processorNum    = qres.result_rows[0][1]
    processoridNum  = qres.result_rows[0][2]
    stageNum        = qres.result_rows[0][3]
    walltime        = end - start

    qres = client.query(Q_PIPELINE_DETAIL.format(id=query_id, start=start, end=end))    
    if qres.result_rows.count == 0:
        return
    
    pData = PipelineTraceData(threadNum, processorNum, processoridNum, stageNum, walltime, qres.result_rows)    

    return pData
    

def main():
    parser = argparse.ArgumentParser(description='clickhouse pipeline trace svg graph throgh the pipeline trace log')
    parser.add_argument('--ch-host', metavar='CLICKHOUSE_HOST', help='ClickHouse host', default='localhost')
    parser.add_argument('--ch-port', metavar='CLICKHOUSE_PORT', type=int, help='ClickHouse port', default=8123)
    parser.add_argument('--username', metavar='USERNAME', help='ClickHosue username', default='')
    parser.add_argument('--password', metavar='PASSWORD', help='ClickHosue password', default='')
    parser.add_argument('--query-id', metavar='QUERY_ID', help='The query id of the pipeline needed to be visualized')
    args = parser.parse_args()

    if not args.query_id:
        print("query id was required\n")
        parser.print_help()
        exit(1)

    client = clickhouse_connect.get_client(host=args.ch_host, port=args.ch_port, username=args.username, password=args.password, database='system')
    if not client:
        print("Can not connect to ClickHose DB")
        exit(1)

    pData = retrive_data(client, args.query_id)
    if not pData:
        print("Can not retrive data for query id: " + args.query_id)
        exit(1)

    gantt = GanttGraph(pData)
    gantt.buildGraph()

if __name__ == '__main__':
    main()
