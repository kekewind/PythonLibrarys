#!/usr/bin/env python
# -*- coding: utf-8 -*-

import clr

clr.AddReference("E:\Python_Pycharm_Workspace\PythonLibrarys\CrossPlatform\PythonXCSharp\ParallelCompute.dll")
clr.AddReference("D:\Software\SuperMap\IObject.Net9D\Bin_x64\SuperMap.Data.dll")

import SuperMap.Data
import time
import ParallelCompute
import System


class MultiThreadUtils():
    def __init__(self):
        self.id = 0

    def run(self, start, end, target):
        print(time.process_time())
        ac = System.Action[int](target)
        p = ParallelCompute.ParallelCompute(8)
        p.MultiThreadFor(start, end, ac)
        print(time.process_time())

