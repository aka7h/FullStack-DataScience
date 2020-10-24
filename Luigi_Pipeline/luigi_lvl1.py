# -*- coding: utf-8 -*-
"""
Created on Fri Oct 16 16:43:00 2020

@author: akkash.nr
"""
import luigi
from luigi import Task, LocalTarget


#creating a process task
class ProcessOrders(Task):
    
    def output(self):
        return LocalTarget("orders.csv")
    
    def run(self):
        with self.output().open("w") as f:
            print("May,100",file=f)
            print("May,200",file=f)
            print("June,210",file=f)
            print("June,250",file=f)
        
        
class GenerateReport(Task):
    def requires(self):
        return ProcessOrders()
    
    def output(self):
        return LocalTarget("report.csv")
    
    def run(self):
        report = {}
        for line in self.input().open():
            month, amount = line.split(",")
            if month in report:
                report[month]+=float(amount)
            else:
                report[month] = float(amount)
        
        with self.output().open("w") as out:
            for month in report:
                print(month + "," + str(report[month]),file=out)
                
#level 2
class SummarizeReport(Task):
    def requires(self):
        return GenerateReport()
    
    def output(self):
        return LocalTarget("summary.txt")
    
    def run(self):
        total = 0.0
        for line in self.input().open():
            month, amount = line.split(",")
            total += float(amount)
        with self.output().open("w") as f:
            f.write(str(total))
                
                
if __name__=="__main__":
    luigi.build([SummarizeReport()]) #to run using luigi ui
    #luigi.build([SummarizeReport()],local_scheduler=True)
    #always use build() instead of run(), might face Task class ambiguous issue.
        
    
    
        
