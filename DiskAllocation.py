import numpy as np
import pandas as pd
from itertools import cycle

'''Memory Block structured as a nested list
[[process name, memory size, remaining time unit]]
Free or unused memory is named as "Free"
Free memories have 0 time unit
'''

#Combines Free memories adjacent to each other
#Returns the memoryBlock and the total unitTime used
def coalesce(memoryBlock):
    check = False
    i = 0
    totalUnitTime = 0
    memLen = len(memoryBlock)
    while i < memLen:
        if check == False and memoryBlock[i][0] == 'Free':
            check = True
            i += 1
            continue
        elif check == True and memoryBlock[i][0] == 'Free':
            memoryBlock[i-1][1] += memoryBlock[i][1]
            del memoryBlock[i]
            memLen = len(memoryBlock)
            totalUnitTime += 1
            continue
        check = False
        i += 1
    return {'memory': memoryBlock, 'unitTime': totalUnitTime}

#Sorts the memory where the first in memory has the highest memory usage
#Puts all free memory to the last segment of the memory then combines them together
def storageCompaction(memoryBlock):  
    df_mb = pd.DataFrame(memoryBlock, columns=['pname', 'memsize', 'timeunit'])
    totalJobs = df_mb.loc[df_mb['pname'] != 'Free'].count()['pname']
    i = 0
    if df_mb.groupby('pname').count()['memsize']['Free'] < 2:
        return {'memory': memoryBlock, 'unitTime': 0}
    for x in df_mb.index:
        if df_mb['pname'][x] != 'Free':
            i += 1
        else:
            break
    totalTimeUnit = totalJobs-i+1
    df_noFree = df_mb.loc[df_mb['pname'] != 'Free']
    totalFreeMemory = df_mb.loc[df_mb['pname'] == 'Free']['memsize'].sum()
    df_mb = df_noFree
    df_mb = df_mb.sort_values(['memsize', 'pname'], ascending=False)
    df_mb.loc[-1] = ['Free', totalFreeMemory, 0]
    memoryBlock = np.array(df_mb).tolist()
    return {'memory': memoryBlock, 'unitTime': totalTimeUnit}

#Check for coalesce and storage compaction
def checkSCnCH(currTimeUnit: int, memoryBlock: list, timeUnitTable: list, sc, ch):
    if currTimeUnit % sc == 0:
        newMemTU = storageCompaction(memoryBlock)
        memoryBlock = newMemTU['memory']
        currTimeUnit += newMemTU['unitTime']
        for i in range(newMemTU['unitTime']):
            timeUnitTable.append('SC')
    if currTimeUnit % ch == 0:
        newMemTU = coalesce(memoryBlock)
        memoryBlock = newMemTU['memory']
        currTimeUnit += newMemTU['unitTime']
        for i in range(newMemTU['unitTime']):
            timeUnitTable.append('CH')
    return [currTimeUnit, memoryBlock, timeUnitTable]


def removeExcess(timeUnitTable: list):
    while timeUnitTable[-1] in ['CH', 'SC']:
        timeUnitTable.pop(-1)
    return timeUnitTable
#Does the first fit algorithm
#processes must be made as such
#[process name, memory size, time unit]
def firstFit(processes: list, memorySize: int, sc: int, ch: int):
    currTimeUnit = 0
    memoryBlock = [['Free', memorySize, 0]]
    timeUnitTable = []
    #Order to do the jobs
    orderTable = []
    while processes or len(memoryBlock) != 1:
        i = 0
        currProcess = 0
        mbLen = len(memoryBlock)
        #Put processes into free memory
        while i < mbLen:
            if memoryBlock[i][0] == 'Free':
                while currProcess != len(processes):

                    if memoryBlock[i][1] > processes[currProcess][1]:
                        process = processes.pop(currProcess)
                        memoryBlock.insert(i+1, memoryBlock[i])
                        memoryBlock[i] = process
                        memoryBlock[i+1][1] -= process[1]
                        mbLen = len(memoryBlock)
                        currTimeUnit += 1
                        orderTable.append(process[0])
                        currTimeUnit, memoryBlock, timeUnitTable = checkSCnCH(currTimeUnit, memoryBlock,
                        timeUnitTable, sc, ch)
                        
                        timeUnitTable.append(process[0])
                        memoryBlock[i][2] -= 1
                        i += 1
                        continue
                    currProcess += 1
                currProcess = 0
            i += 1
        
        #Process jobs, no new memory available for all jobs
        jobTable = {}
        for i,memory in enumerate(memoryBlock):
            if memory[0] != 'Free':
                jobTable[memory[0]] = i
        #Processes job whilst no new memory is created
        orderTableLen = len(orderTable)
        while orderTableLen == len(orderTable):
            print(orderTable)
            currMemBlock = memoryBlock[jobTable[orderTable.pop(0)]]
            procName = currMemBlock[0]
            currMemBlock[2] -= 1
            currTimeUnit += 1
            timeUnitTable.append(procName)
            if currMemBlock[2] == 0:
                currMemBlock[0] = 'Free'
            else:
                orderTable.append(procName)

            lastTimeUnit = currTimeUnit
            #Check for coalesce and storage compaction
            currTimeUnit, memoryBlock, timeUnitTable = checkSCnCH(currTimeUnit, memoryBlock, 
            timeUnitTable, sc, ch)

            if lastTimeUnit != currTimeUnit:
                break
    
    return removeExcess(timeUnitTable)



processes = [['J1', 500, 3], ['J2', 250, 4], ['J3', 200, 5], ['J4', 350, 3], 
            ['J5', 60, 5], ['J6', 300, 3], ['J7', 400, 2]]

print(firstFit(processes, 1000, 20, 1))
