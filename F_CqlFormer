# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import pandas as pd
from py2neo import Graph, Node, Relationship, NodeMatcher, RelationshipMatcher, Subgraph
import time


class CqlFormer:

    def __init__(self,
                host="http://XXXX:7474",
                userName="neo4j",
                pwd="neo4j",
                idNameList=["OrgID","RtxName"]):
        '''
        host:"http://XXXX:7474";
        userName:"neo4j"
        pwd:"neo4j"
        '''

        self.graph=Graph(host,username=userName,password=pwd)

        self.cql={
            "match":{
                "sub":{},#{"IDName":"","IDValue":"","nickName":""}
                "rel":{},#{"relName":"","nickName":""}
                "obj":{},#{"IDName":"","IDValue":"","nickName":""}
            },
            "where":"",
            "return":[],
        }

    def getSub(self,**sub):
        '''
        sub:subject ID (eg.RtxName="ahw")
        ================
        return: self
        '''
        idName=list(sub.keys())[0]
        self.cql["match"]["sub"]={"IDName":idName,"IDValue":sub[idName],"nickName":"sub{}".format(abs(hash(sub[idName]+str(time.time()))))}
        return self
    
    def getObj(self,**obj):
        '''
        obj:object ID (eg.RtxName="ahw")
        ================
        return: self
        '''
        idName=list(obj.keys())[0]
        self.cql["match"]["obj"]={"IDName":idName,"IDValue":obj[idName],"nickName":"obj{}".format(abs(hash(obj[idName]+str(time.time()))))}
        return self

    def getRel(self,rel):
        '''
        rel:relation name (eg."memberOf")
        ================
        return: self
        '''
        if type(rel)==str:
            self.cql["match"]["rel"]={"relName":rel,"nickName":"rel{}".format(abs(hash("relName"+str(time.time()))))}
        elif type(rel)==list:
            if len(rel)==1:
                self.cql["match"]["rel"]={"relName":rel[0],"nickName":"rel{}".format(abs(hash("relName"+str(time.time()))))}
            else:
                self.cql["match"]["rel"]=[]
                for relItem in rel:
                    self.cql["match"]["rel"].append({"relName":relItem,"nickName":"rel{}".format(abs(hash(relItem+str(time.time()))))})
        return self

    def getTri(self,triList):
        '''
        triList:[{"subIdName":subid,"subIdValue":subvalue},
                {"relName":relname},
                {"objIdName":objid,"objIdValue":objvalue}]
        ======================
        return: self
        '''
        self.cql["match"]["sub"]={"IDName":triList[0]["subIdName"],"IDValue":triList[0]["subIdValue"],"nickName":"sub{}".format(abs(hash(sub[idName]+str(time.time()))))}
        self.cql["match"]["rel"]={"relName":triList[1]["relName"],"nickName":"rel{}".format(abs(hash("relName"+str(time.time()))))}
        self.cql["match"]["obj"]={"IDName":triList[2]["objIdName"],"IDValue":triList[0]["objIdValue"],"nickName":"obj{}".format(abs(hash(sub[idName]+str(time.time()))))}

        return self

    def getReturn(self,sro=["s","r","o"],agg=None):
        '''
        sro: ["s","r","o"] (default)
        agg: [("count","s")]
        ===========================
        return: self
        '''
        returnList=[]
        if "s" in sro:
            if len(self.cql["match"]["sub"])>0:
                returnList.append(self.cql["match"]["sub"]["nickName"])
            else:
                self.cql["match"]["sub"]["nickName"]="sub{}".format(abs(hash("sub"+str(time.time()))))
                self.cql["match"]["sub"]["IDName"]=""
                self.cql["match"]["sub"]["IDValue"]=""
                returnList.append(self.cql["match"]["sub"]["nickName"])

        if "r" in sro:
            if type(self.cql["match"]["rel"])==dict:
                if len(self.cql["match"]["rel"])>0:
                    returnList.append(self.cql["match"]["rel"]["nickName"])
                else:
                    self.cql["match"]["rel"]["nickName"]="rel{}".format(abs(hash("rel"+str(time.time()))))
                    self.cql["match"]["rel"]["relName"]=""
                    returnList.append(self.cql["match"]["rel"]["nickName"])
            elif type(self.cql["match"]["rel"])==list:
                for relItem in self.cql["match"]["rel"]:
                    returnList.append(relItem["nickName"])

        if "o" in sro:
            if len(self.cql["match"]["obj"])>0:
                returnList.append(self.cql["match"]["obj"]["nickName"])
            else:
                self.cql["match"]["obj"]["nickName"]="obj{}".format(abs(hash("obj"+str(time.time()))))
                self.cql["match"]["obj"]["IDName"]=""
                self.cql["match"]["obj"]["IDValue"]=""
                returnList.append(self.cql["match"]["obj"]["nickName"])

        if agg is not None:
            for pairItem in agg:
                aggName=pairItem[0]
                if "." not in pairItem[1]:
                    objectName=returnList[sro.index(pairItem[1])]
                    aggStr="{aggName}({objectName}) as my{aggName}{objectName}".format(aggName=aggName,objectName=objectName)
                    returnList.append(aggStr)
                else:
                    objectName=returnList[sro.index(pairItem[1].split(".")[0])]
                    attrName=pairItem[1].split(".")[1]
                    aggStr="{aggName}({objectName}.{attrName}) as my{aggName}{objectName}".format(aggName=aggName,
                                                                        objectName=objectName,
                                                                        attrName=attrName)
                    returnList.append(aggStr)
                
        self.cql["return"]=returnList
        return self

    def outputJson(self):
        return self.cql

    def buildCypher(self):
        subNickName=self.cql["match"]["sub"]["nickName"]
        objNickName=self.cql["match"]["obj"]["nickName"]
        self.tmpList=[]
        if type(self.cql["match"]["rel"])==dict:
            relNickName=self.cql["match"]["rel"]["nickName"]
            relName=self.cql["match"]["rel"]["relName"]
            if len(relName)>0:
                relName=":"+relName
            matchStr="MATCH ({subNickName})-[{relNickName}{relName}]->({objNickName})".format(subNickName=subNickName,
                                                                                            relNickName=relNickName,
                                                                                            relName=relName,
                                                                                            objNickName=objNickName)
        else:
            relNickNameList=[relItem["nickName"] for relItem in self.cql["match"]["rel"]]
            relNameList=[relItem["relName"] for relItem in self.cql["match"]["rel"]]
            relList=[]
            for relI in range(len(relNameList)):
                relList.append("[{relNickName}{relName}]".format(relNickName=relNickNameList[relI],
                                                                    relName=":"+relNameList[relI]))
                if relI<len(relNameList)-1:
                    self.tmpList.append("tmp{}".format(relI))
                    relList.append("(tmp{})".format(relI))
            relStr="-".join(relList)
            matchStr="MATCH ({subNickName})-{relStr}->({objNickName})".format(subNickName=subNickName,
                                                                                relStr=relStr,
                                                                                objNickName=objNickName)

        subIdName=self.cql["match"]["sub"]["IDName"]
        subIdValue=self.cql["match"]["sub"]["IDValue"]
        objIdName=self.cql["match"]["obj"]["IDName"]
        objIdValue=self.cql["match"]["obj"]["IDValue"]
        whereList=[]
        if len(subIdName)>0:
            subConStr="{subNickName}.{subIdName}='{subIdValue}'".format(subNickName=subNickName,
                                                                        subIdName=subIdName,
                                                                        subIdValue=subIdValue)
            whereList.append("{subConStr}".format(subConStr=subConStr))
        if len(objIdName)>0:
            objConStr="{objNickName}.{objIdName}='{objIdValue}'".format(objNickName=objNickName,
                                                                        objIdName=objIdName,
                                                                        objIdValue=objIdValue)
            whereList.append("{objConStr}".format(objConStr=objConStr))
        whereStr="WHERE "+" AND ".join(whereList)
        whereStr+=self.cql["where"]

        returnStr="RETURN "+",".join(self.cql["return"])
        if len(self.tmpList)>0:
            returnStr+=","+",".join(self.tmpList)

        self.mwrStr=matchStr+" "+whereStr+" "+returnStr

        return self

    def outputCypher(self):
        self.buildCypher()
        return self.mwrStr
    
    def run(self):
        self.buildCypher()
        return pd.DataFrame(self.graph.run(self.mwrStr).data())

if __name__=="__main__":
    # %%
    myCF=CqlFormer()


    # %%
    myCF.getSub(RtxName="alexhnwang").getRel(["subordinate"]).getReturn()


    # %%
    print(myCF.run())
