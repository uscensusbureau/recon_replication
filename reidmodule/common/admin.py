"""
.. module:: admin
    :platform: Unix
    :synopsis: miscelaneous data management functions

.. moduleauthor:: Goldschlag
"""

import csv
import subprocess
import threading
import logging
import pandas as pd

def startOutFile(filename,header,path): 
    """Starts outfile .csv, writing header row. 
        
    Args:
        filename (str): name of output file
        
        header (list): header row labels
        
        path (string): path of file
        
    """
    
    # Create output file and prepare writer method
    outFile = open(path+filename,'w')
    outWriter = csv.writer(outFile,delimiter=',')
    outWriter.writerow(header)
    outFile.close()


def emptyDoneQueue(statsOutFileName,inQ,stopDump,path):
    """Thread to continuously empty the doneQueue into the outfile.   
        
    Args:
        runner (Runner instance): reference to instance of Runner class 
        
        statsOutFileName (str): name of output file
        
        stopDump (threading.Event()): event to notify the thread to stop dumping
        
        path (str): path of statsOutFileName
        
    """
    
    # Empty doneQueue into csv file 
    while inQ.empty()==False:
        outData = inQ.get()
        outData.to_csv(path+statsOutFileName, mode='a', header=False, index=False)
    # Repeat until stop set
    if not stopDump.is_set():
        threading.Timer(60, emptyDoneQueue, 
                        [statsOutFileName, inQ, stopDump,path]).start()

def writeWorker(splitter,stop):
    """Pulls data from splitter.writeQueue() and writes it to csv via 
    splitter.writeDFChunk(). Why is method outside of the dfCountySplitter 
    class? Because it needs to be splotted with a threading.Event() passed from
    main(). When this method is part of the dfCountySplitter class it was 
    unresponsive to the stop event.  
    
    Args:
        splitter (Splitter instance): instance of Splitter class
        
        stop (threading.Event()):  event used to stop the loop
        
    """
    while splitter.writeQueue.qsize()>0:
        df2Write = splitter.writeQueue.get(timeout = 30)
        #logging.debug('writeWorker got df2Write')
        splitter.writeDFChunk(df2Write[0],df2Write[1])
        #logging.debug('sent splitter.writeDFChunk')
        
    # Repeat until stop set
    if not stop.is_set():
        threading.Timer(60, writeWorker, 
                        [splitter, stop]).start()

def reporter(matcher, stopReport):
    """Writes information about runner queues to log.
    
    Args:
        runner (Runner instance): reference to instance of Runner class
        
        stopReport (threading.Event()):  event used to stop the loop
    
    """
    logging.info('Reporter---------------')
    logging.info('workQueue size: {0}'.format(matcher.workQueue.qsize()))
    logging.info('doneQueue size: {0}'.format(matcher.doneQueue.qsize()))
    logging.info('Tracts processed Validate: {0}'.format(matcher.valTractsPrcd.value))
    logging.info('Tracts processed Putative: {0}'.format(matcher.putTractsPrcd.value))
    logging.info('Tracts processed Confirm: {0}'.format(matcher.confTractsPrcd.value))
    logging.info('---------------Reporter')
    
    # Repeat until stop set
    if not stopReport.is_set():
        threading.Timer(120, reporter, 
                        [matcher, stopReport]).start()


if __name__ == '__main__':
    main()
