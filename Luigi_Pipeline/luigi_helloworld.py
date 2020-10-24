# -*- coding: utf-8 -*-
"""
Created on Fri Oct 16 16:30:55 2020

@author: akkash.nr


Luigi uses 3 task 
requires() - dendending tasks
output() - final target / checkpoint
run() - business logic


"""

import luigi

class SayHello6(luigi.Task):
    def output(self):
        return complete()
    
    def run(self):
        print("Hello world")
        with self.output().open("w") as f:
            f.write("ok")
            
    def complete(self):
        return True
            
            
if __name__=="__main__":
    luigi.build([SayHello6()],local_scheduler=True)
    