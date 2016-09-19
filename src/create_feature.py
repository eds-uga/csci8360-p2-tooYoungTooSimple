# -*- coding: utf-8 -*-
"""
Created on Sun Sep 18 21:04:54 2016

@author: Yaotong Cai
"""

# The following python code is for extracting features: 1) proportion of lines or characters in each 'section'; 2) Number of 
# occurences of specific dll's; 3) proportion of certain interpunction characters in
# each 'section'

# NOTE: To run, get trainLabels.csv and Sample.csv ready in the execution directory, 
#       with subdirectories train and test containing asm files.

# Reference to pick the features: 2nd Place solution documentation by Marios
# Michailidis and Gert Jacobusse 

# import dependencies

import os
import csv
import zipfile
from io import BytesIO
from collections import defaultdict
import re
import numpy as np 

# list ids and labels

ids=[]
labels=[]
with open('trainLabels.csv','r') as f:
    r=csv.reader(f)
    r.next() # skip header
    for row in r:
        ids.append(row[0])
        labels.append(float(row[1]))

testids=[]
with open('Sample.csv','r') as f:
    r=csv.reader(f)
    r.next()
    for row in r:
        testids.append(row[0])
        
        
# general functions
        
def readdata(fname,header=True,selectedcols=None):
    with open(fname,'r') as f:
        r=csv.reader(f)
        names = r.next() if header else None
        if selectedcols:
            assert header==True
            data = [[float(e) for i,e in enumerate(row) if names[i] in selectedcols] for row in r]
            names = [name for name in names if name in selectedcols]
        else:
            data = [[float(e) for e in row] for row in r]
    return data,names
    
# write dictionary to .csv file
def writedata(data,fname,header=None):
    with open(fname,'w') as f:
        w=csv.writer(f)
        if header:
            w.writerow(header)
        for row in data:
            w.writerow(row)
            
# extract file properties

"""
function compressedsize
input: path to file
output: compressed size of file
* read file and compress it in memory
"""
def compressedsize(fpath):
    OutputFile = BytesIO()
    zf = zipfile.ZipFile(OutputFile, 'w') # reading ZIP files
    zf.write(fpath, compress_type=zipfile.ZIP_DEFLATED)
    s = float(zf.infolist()[0].compress_size) # obtain compressed size of each file
    zf.close()
    return s

"""
function fileprops
input: ids of train or test, string "train" or "test"
output: writes train_props or test_props
* extract file properties (size, compressed size, ratios) from all files
"""
def fileprops(ids,trainortest):
    with open('%s_fileprops.csv'%trainortest,'w') as f:
        w=csv.writer(f)
        w.writerow(['asmSize','bytesSize',
                    'asmCompressionRate','bytesCompressionRate',
                    'ab_ratio','abc_ratio','ab2abc_ratio']) # save three ratios
                    # in csv file
        for i in ids:
            asmsiz=float(os.path.getsize('%s/'%trainortest+i+'.asm')) # obtain 
            # size of asm file
            bytsiz=float(os.path.getsize('%s/'%trainortest+i+'.bytes'))# obtain
            # size of bytes file
            asmcr=compressedsize('%s/'%trainortest+i+'.asm')/asmsiz
            # asm file compression rate
            bytcr=compressedsize('%s/'%trainortest+i+'.bytes')/bytsiz
            # bytes file compression rate
            ab=asmsiz/bytsiz # asm file size divided by bytes file size
            abc=asmcr/bytcr # asm compression rate divided by bytes compression
            # rate
            w.writerow([asmsiz,bytsiz,asmcr,bytcr,ab,abc,ab/abc])
            # the last is ab_ratio divided by abc_ratio
            f.flush()

# extract asm contents
# sections that occur in at least 5 files from the trainset:
selsections=['.2', '.3', '.CRT', '.Lax503', '.Much', '.Pav', '.RDATA', '.Racy',
             '.Re82', '.Reel', '.Sty', '.Tls', '.adata', '.bas', '.bas0', '.brick',
             '.bss', '.code', '.cud', '.data', '.data1', '.edata', '.gnu_deb', '.hdata',
             '.icode', '.idata', '.laor', '.ndata', '.orpc', '.pdata', '.rata', '.rdat',
             '.rdata', '.reloc', '.rsrc', '.sdbid', '.sforce3', '.text', '.text1', '.tls',
             '.xdata', '.zenc', 'BSS', 'CODE', 'DATA', 'GAP', 'HEADER', 'Hc%37c',
             'JFsX_', 'UPX0', 'UPX1', 'Xd_?_mf', '_0', '_1', '_2', '_3',
             '_4', '_5', 'bss', 'code', 'seg000', 'seg001', 'seg002', 'seg003',
             'seg004']
             
# dlls that occur in at least 30 files from the trainset:
seldlls=['', '*', '2', '32', 'advapi32', 'advpack', 'api', 'apphelp',
         'avicap32', 'clbcatq', 'comctl32', 'comdlg32', 'crypt32', 'dbghelp', 'dpnet', 'dsound',
         'e', 'gdi32', 'gdiplus', 'imm32', 'iphlpapi', 'kernel32', 'libgcj_s', 'libvlccore',
         'mapi32', 'mfc42', 'mlang', 'mpr', 'msasn1', 'mscms', 'mscoree', 'msdart',
         'msi', 'msimg32', 'msvcp60', 'msvcp71', 'msvcp80', 'msvcr71', 'msvcr80', 'msvcr90',
         'msvcrt', 'msvfw32', 'netapi32', 'ntdll', 'ntdsapi', 'ntmarta', 'ntshrui', 'ole32',
         'oleacc', 'oleaut32', 'oledlg', 'opengl32', 'psapi', 'rasapi32', 'riched20', 'riched32',
         'rnel32', 'rpcrt4', 'rsaenh', 'secur32', 'security', 'sensapi', 'setupapi', 'shell32',
         'shfolder', 'shlwapi', 'tapi32', 'unicows', 'urlmon', 'user32', 'usp10', 'uxtheme',
         'version', 'wab32', 'wininet', 'winmm', 'wintrust', 'wldap32', 'ws2_32', 'wsock32',
         'xprt5']
         

"""
function gsectioncounts
input: list of lines in an asm file
output: dictionary with number of lines in each section

* count number of lines in each section
"""
def gsectioncounts(asmlines):
    seccounts=defaultdict(int)
    for l in asmlines:
        seccounts[l.split(':')[0]]+=1 # split by : to add one by one
    return seccounts


"""
function gdlls
input: list of lines in an asm file
output: dictionary with number of times each dll is found
* count number of times each dll occurs
"""
def gdlls(asmlines):
    dlls=defaultdict(int)
    for l in asmlines:
        ls=l.lower().split('.dll')
        if len(ls)>1:
            dlls[ls[0].replace('\'',' ').split(' ')[-1].split('"')[-1].
            split('<')[-1].split('\\')[-1].split('\t')[-1]]+=1
    return dlls

"""
function asmcontents
input: ids of train or test, string "train" or "test"
output: train_asmcontents or test_asmcontents + dlls on sections
* extract features from contents of asm from all files in train or test set

"""

def asmcontents(ids,trainortest):
    with open('%s_asmcontents.csv'%trainortest,'w') as f:
        w=csv.writer(f)
        w.writerow(
            ['sp_%s'%key for key in selsections]
            +['dl_%s'%key for key in seldlls]
            )
        fsec=open('secmetadata%s.txt'%trainortest,'w')
        wsec=csv.writer(fsec)
        fdll=open('dllmetadata%s.txt'%trainortest,'w')
        wdll=csv.writer(fdll)
        for i in ids:
            with open('%s/'%trainortest+i+'.asm','r') as fasm:
                asmlines=[line for line in fasm.readlines()]
            # section counts/ proportions
            sc=gsectioncounts(asmlines)
            wsec.writerow([i]+['%s:%s'%(key,sc[key]) for key in sc if sc[key]>0])
            scsum=sum([sc[key] for key in sc])
            secfeat=[float(sc[key])/scsum for key in selsections]
            # dlls
            dll=gdlls(asmlines)
            wdll.writerow([i]+['%s:%s'%(key,dll[key]) for key in dll if dll[key]>0])
            dllfeat=[float(dll[key]) for key in seldlls]
            w.writerow(secfeat+dllfeat)
            f.flush()
        fsec.close()
        fdll.close()


# reduce asm contents features by restricting the number of files with nonzero value

"""
function reducefeats
input: train matrix, test matrix, feature names, restriction on required number of nonzeros in each column
output: reduced train matrix, test matrix and feature names
* calculate number of nonzeros by column and keep features within restriction only
"""
def reducefeats(xtrain,xtest,names,res=500):
    ntrain=np.sum(np.array([np.array([ei!=0 for ei in e]) for e in xtrain]),axis=0)
    xtrain=np.array([np.array([e[j] for j,n in enumerate(ntrain) if n>res]) for e in xtrain])
    xtest=np.array([np.array([e[j] for j,n in enumerate(ntrain) if n>res]) for e in xtest])
    names=[names[j] for j,n in enumerate(ntrain) if n>res]
    return xtrain,xtest,names

"""
function reducecontents
input: none
output: write reduced asm contents
* read features on asm contents, reduce them by calling reducefeats
"""
def reducecontents():
    train_asmcontents,asmcontentshead=readdata('train_asmcontents.csv')
    test_asmcontents=readdata('test_asmcontents.csv')
    train_asmcontents_red,test_asmcontents_red,asmcontentshead_red=reducefeats(
    train_asmcontents,test_asmcontents,asmcontentshead)
    writedata(train_asmcontents_red,'train_asmcontents_red.csv',asmcontentshead_red)
    writedata(test_asmcontents_red,'test_asmcontents_red.csv',asmcontentshead_red)


# execute
if __name__ == '__main__':
    fileprops(ids,'train')
    fileprops(testids,'test')
    asmcontents(ids,'train')
    asmcontents(testids,'test')
    reducecontents()